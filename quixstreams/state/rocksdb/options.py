import dataclasses
import threading
from datetime import timedelta
from typing import Dict, Mapping, Optional

import rocksdict
from rocksdict import DBCompressionType

from quixstreams.state.serialization import DumpsFunc, LoadsFunc
from quixstreams.utils.json import dumps, loads

from .types import CompressionType, RocksDBOptionsType

__all__ = ("RocksDBOptions",)

# One block cache per configured capacity, shared by every store partition in
# the process.
#
# RocksDB's own guidance is a single block cache shared across databases, and
# without this ``block_cache_size`` is a PER-PARTITION multiplier: a 32-partition
# 3-store application would reserve 96 x the configured size. Keying on capacity
# rather than on the options instance is deliberate — a partition constructed
# without explicit options builds its own ``RocksDBOptions`` (see
# ``RocksDBStorePartition.__init__``), so per-instance memoization would not
# share for the default case, which is most deployments.
#
# A cache is a lazily-filled ceiling, not an allocation, so a small deployment
# never realizes the full capacity.
_BLOCK_CACHES: Dict[int, rocksdict.Cache] = {}
_BLOCK_CACHES_LOCK = threading.Lock()


def _shared_block_cache(capacity: int) -> rocksdict.Cache:
    """
    Get the process-wide block cache for ``capacity``, creating it on first use.

    Locked because partitions are opened concurrently during a rebalance, and two
    threads racing here would otherwise each build a cache of this size.
    """
    with _BLOCK_CACHES_LOCK:
        cache = _BLOCK_CACHES.get(capacity)
        if cache is None:
            cache = rocksdict.Cache(capacity)
            _BLOCK_CACHES[capacity] = cache
        return cache


COMPRESSION_TYPES: Mapping[CompressionType, DBCompressionType] = {
    "none": DBCompressionType.none(),
    "snappy": DBCompressionType.snappy(),
    "zlib": DBCompressionType.zlib(),
    "bz2": DBCompressionType.bz2(),
    "lz4": DBCompressionType.lz4(),
    "lz4hc": DBCompressionType.lz4hc(),
    "zstd": DBCompressionType.zstd(),
}


@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType):
    """
    RocksDB database options.

    :param dumps: function to dump data to JSON
    :param loads: function to load data from JSON
    :param open_max_retries: number of times to retry opening the database
            if it's locked by another process. To disable retrying, pass 0
    :param open_retry_backoff: number of seconds to wait between each retry.
    :param on_corrupted_recreate: when True, the corrupted DB will be destroyed
        if the `use_changelog_topics=True` is also set on the Application.
        If this option is True, but `use_changelog_topics=False`,
        the DB won't be destroyed.
        Note: risk of data loss! Make sure that the changelog topics are up-to-date before disabling it in production.
        Default - `True`.
    :param block_cache_size: size of the RocksDB block cache, in bytes.
        This is an **aggregate ceiling for the whole process**: one cache of this
        size is shared by every store partition, following RocksDB's own guidance
        that a single block cache be shared across databases. It is not a
        per-partition budget, so a 32-partition application does not reserve 32
        times this value.
        A cache is filled lazily, so a small deployment never realizes the full
        capacity; size it against total available memory rather than per store.
        Note that index and bloom-filter blocks are held outside this budget
        (``cache_index_and_filter_blocks`` is not enabled), so real usage is this
        value plus roughly ``bloom_filter_bits_per_key / 8`` bytes per key.
        Default - ``1 GiB``.
    :param max_evictions_per_flush: cap on TTL-driven evictions performed
        during a single ``flush()`` for stores with TTL enabled. Larger values
        increase per-flush latency but let the sweep keep up with higher
        steady-state expiration rates. Only meaningful for TTL-enabled
        stores; ignored otherwise.

        This is the sweep's throughput dial, and it is the one to reach for:
        the drain rate is ``max_evictions_per_flush / commit_interval``, but a
        checkpoint's cost is mostly *fixed* (a producer flush barrier plus an
        offset commit, milliseconds against a remote broker), so shrinking
        ``commit_interval`` buys no drain speed and starves message processing.
        Raise this instead.

        **Interaction with the producer queue.** Each eviction produces one
        changelog tombstone (see ``ttl_changelog_tombstones``), so a sweep can
        enqueue far more records than librdkafka's
        ``queue.buffering.max.messages`` (default ``100_000``) would appear to
        allow. In practice it does not, because ``Producer.produce()`` polls
        after every produce, so against a live broker the queue is a rolling
        window rather than an accumulator: enqueueing ``150_000`` tombstones was
        measured to peak at a queue depth of **~6,800 (6.8% of the default)**,
        with the checkpoint's producer flush completing in ~4ms. The drain rate
        governs, not the queue depth.

        Two situations still bound it: the peak depth scales with
        partitions-per-process (roughly ~15 partitions sweeping concurrently at
        that depth would approach the default cap, so raise
        ``queue.buffering.max.messages`` for high partition counts), and a broker
        that is not draining at all will raise ``BufferError`` once the queue
        genuinely fills — though by then the application has larger problems.
        ``queue.buffering.max.kbytes`` (~1 GB default) is a second cap that binds
        first for multi-KB records.
        Default - ``150_000``. Measured on one partition against a live broker: a
        full ``150_000``-eviction sweep completed in **1.12s** (index scan 0.31s,
        produces 0.53s, flush 0.004s, commit 0.27s) — a ~268x margin under a 300s
        ``max.poll.interval.ms``. The original ``10_000`` sustained only ~300
        evictions/s once checkpoints grew, which loses the race against expiry
        and lets the store grow without bound.
    :param legacy_records_ttl: expiry for pre-existing records when enabling
        TTL on a **populated** legacy store that already holds un-stamped
        records. When ``None`` (the default), the migration still completes:
        the pre-existing records are backfilled using the ttl the service
        itself uses (the max ``ttl=`` in the triggering flush) and a WARNING
        names the implicit value. When set to a strictly positive
        ``timedelta``, that value is used instead: the partition **backfills**
        its pre-existing un-stamped records with a uniform expiry of
        ``high_water + legacy_records_ttl`` (event-time high-water at the
        enable moment) and flips into TTL mode in place — no state deletion.
        New records keep getting their true event-time expiry. The backfill
        runs exactly once; a redeploy / restart never re-runs it. Ignored for
        windowed / timestamped stores (they opt out of the TTL stamp
        machinery at the class level). Must be strictly positive if set;
        ``<= 0`` raises ``ValueError`` at construction.
        Default - ``None``.
    :param legacy_backfill_chunk_size: number of pre-existing records re-stamped
        per write-batch during the one-time legacy backfill (see
        ``legacy_records_ttl``). The backfill iterates the populated default CF
        in chunks of this size; each chunk is re-stamped, produced to the
        changelog, flushed, and committed before the next chunk is read, so peak
        transient memory is bounded to one chunk regardless of total store size.
        Lower it on memory-constrained deployments. Only meaningful on the single
        backfilling flush; ignored otherwise and on windowed / timestamped
        stores. Must be strictly positive; ``<= 0`` raises ``ValueError`` at
        construction.
        Default - ``150_000``. Raising it mainly reduces the number of confirming
        flushes (one broker round-trip each), whose fixed cost otherwise dominates
        a large backfill; the producer-queue note under
        ``max_evictions_per_flush`` applies here too.
    :param ttl_changelog_tombstones: when ``True`` (the default), TTL-driven
        evictions are also produced to the changelog as tombstones
        (``value=None``) so log compaction physically reclaims expired keys in
        step with the local store — ``cleanup.policy=compact`` alone then shrinks
        the changelog as keys expire (no ``delete`` policy / retention tuning
        needed to reclaim). When ``False``, evictions are local-only (the
        pre-change behavior): the changelog keeps each expired key's last record
        until compacted by other means, and rebuilds rely on the read-time
        expiry filter. Read-time consistency is identical either way. Only
        meaningful for TTL-enabled stores; ignored for windowed / timestamped
        stores and for no-``ttl=`` workloads.
        Default - ``True``.
    :param ttl_rollback: operational lever that reverts a store which was
        PROVISIONALLY cold-adopted as a v3.24.0 TTL store back to legacy mode
        (see ``RocksDBStorePartition._rollback_provisional_adopt``): on a warm
        restart the pre-adoption originals are restored byte-identical; on a
        fresh volume the provisional adopt is suppressed. It never touches the
        sound warm-deterministic path nor a corroborated store.
        This option is the IN-CODE surface of the
        ``QUIXSTREAMS_STATE_TTL_ROLLBACK=1`` environment variable, which keeps
        working: the lever is ON when EITHER is set (option wins when ``True``,
        else the env var is consulted), and the partition logs which source
        turned it on. Prefer the option in Quix Cloud, where a deployment
        environment variable that is not declared in the app's ``app.yaml`` is
        silently dropped on redeploy — a lever that can vanish between runs is
        not a lever you can reason about afterwards.
        Mutually exclusive with ``ttl_force_flip``; both on raises.
        Default - ``False``.
    :param ttl_force_flip: operational REPAIR lever, the inverse of
        ``ttl_rollback``. It forces a store whose ``__ttl_enabled__`` flag is
        ABSENT into TTL mode and persists the flip, then lets the recovery pass
        finish any leftover migration. Use it when a store holds TTL-stamped
        values but has no bookkeeping left for the automatic open-time repair to
        identify it by (a rebuilt state directory, or a previous rollback that
        removed the flag while its untouched values stayed stamped) — the
        symptom is a crash loop where every read of a stamped value fails in the
        value deserializer. No-op on a store that is already flipped, so it is
        safe to leave set for one restart and then clear.
        Same dual surface as ``ttl_rollback``: this option, or
        ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1``.
        Mutually exclusive with ``ttl_rollback``; both on raises.
        Default - ``False``.

    Please see `rocksdict.Options` for a complete description of other options.
    """

    write_buffer_size: int = 64 * 1024 * 1024
    target_file_size_base: int = 64 * 1024 * 1024
    max_write_buffer_number: int = 3
    block_cache_size: int = 1024 * 1024 * 1024
    bloom_filter_bits_per_key: int = 10
    enable_pipelined_write: bool = False
    compression_type: CompressionType = "lz4"
    wal_dir: Optional[str] = None
    max_total_wal_size: int = 128 * 1024 * 1024
    db_log_dir: Optional[str] = None
    dumps: DumpsFunc = dumps
    loads: LoadsFunc = loads
    open_max_retries: int = 10
    open_retry_backoff: float = 3.0
    use_fsync: bool = True
    on_corrupted_recreate: bool = True
    max_evictions_per_flush: int = 150_000
    legacy_records_ttl: Optional[timedelta] = None
    legacy_backfill_chunk_size: int = 150_000
    ttl_changelog_tombstones: bool = True
    ttl_rollback: bool = False
    ttl_force_flip: bool = False

    def __post_init__(self) -> None:
        if self.legacy_records_ttl is not None and self.legacy_records_ttl <= timedelta(
            0
        ):
            raise ValueError(
                "legacy_records_ttl must be a strictly positive timedelta or "
                f"None, got {self.legacy_records_ttl!r}"
            )
        if self.legacy_records_ttl is not None:
            # Symmetric upper bound: the backfill expiry is
            # ``enable_time + legacy_records_ttl`` and ``enable_time`` is unknown
            # at config time, so bound the ttl magnitude itself. A ttl of
            # ~31,600 years would derive a backfill stamp above
            # ``_MAX_PLAUSIBLE_STAMP_MS`` that the read validator refuses on every
            # read (permanently unreadable). Reject the ``timedelta.max`` /
            # unit-mistake config here, before it can produce such a stamp. Local
            # imports mirror the codebase's circular-import avoidance (options is
            # imported early by the rocksdb package).
            from .transaction import _ttl_to_ms
            from .ttl_codec import _MAX_PLAUSIBLE_STAMP_MS

            if _ttl_to_ms(self.legacy_records_ttl) >= _MAX_PLAUSIBLE_STAMP_MS:
                raise ValueError(
                    "legacy_records_ttl is implausibly large "
                    f"({self.legacy_records_ttl!r}): its millisecond magnitude "
                    f"({_ttl_to_ms(self.legacy_records_ttl)}) meets or exceeds "
                    f"the maximum representable TTL stamp ({_MAX_PLAUSIBLE_STAMP_MS}"
                    "), so the derived backfill expiry would be unreadable. Use a "
                    "ttl below ~31,600 years (check for a unit mistake)."
                )
        if self.legacy_backfill_chunk_size <= 0:
            raise ValueError(
                "legacy_backfill_chunk_size must be a strictly positive int, "
                f"got {self.legacy_backfill_chunk_size!r}"
            )
        if self.max_evictions_per_flush <= 0:
            # A 0/negative cap silently disables the per-flush
            # TTL sweep AND the tombstone reclamation that rides on it, so expired
            # records accumulate unbounded with no error. Reject at construction,
            # mirroring the other bound checks above.
            raise ValueError(
                "max_evictions_per_flush must be a strictly positive int, "
                f"got {self.max_evictions_per_flush!r}"
            )
        if self.ttl_rollback and self.ttl_force_flip:
            # Contradictory levers: one reverts a store to legacy, the other
            # forces it into TTL mode. Rejected here so a pure-config mistake
            # fails at construction with a stack pointing at the caller; the
            # partition repeats the check on the RESOLVED values at open, which
            # is where an option/env-var combination can also collide.
            raise ValueError(
                "ttl_rollback and ttl_force_flip are mutually exclusive "
                "(one reverts a store to legacy mode, the other forces it into "
                "TTL mode); set at most one of them."
            )

    def to_options(self) -> rocksdict.Options:
        """
        Convert parameters to `rocksdict.Options`
        :return: instance of `rocksdict.Options`
        """
        opts = rocksdict.Options(raw_mode=True)
        opts.create_if_missing(True)
        opts.set_write_buffer_size(self.write_buffer_size)
        opts.set_target_file_size_base(self.target_file_size_base)
        opts.set_max_write_buffer_number(self.max_write_buffer_number)
        opts.set_enable_pipelined_write(self.enable_pipelined_write)
        opts.set_use_fsync(self.use_fsync)
        if self.wal_dir is not None:
            opts.set_wal_dir(self.wal_dir)
        if self.db_log_dir is not None:
            opts.set_db_log_dir(self.db_log_dir)

        table_factory_options = rocksdict.BlockBasedOptions()
        table_factory_options.set_block_cache(
            _shared_block_cache(self.block_cache_size)
        )
        table_factory_options.set_bloom_filter(
            self.bloom_filter_bits_per_key, block_based=True
        )
        opts.set_block_based_table_factory(table_factory_options)
        compression_type = COMPRESSION_TYPES[self.compression_type]
        opts.set_compression_type(compression_type)
        opts.set_max_total_wal_size(size=self.max_total_wal_size)
        return opts
