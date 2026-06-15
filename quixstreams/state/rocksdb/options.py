import dataclasses
from datetime import timedelta
from typing import Mapping, Optional

import rocksdict
from rocksdict import DBCompressionType

from quixstreams.state.serialization import DumpsFunc, LoadsFunc
from quixstreams.utils.json import dumps, loads

from .types import CompressionType, RocksDBOptionsType

__all__ = ("RocksDBOptions",)

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
    :param max_evictions_per_flush: cap on TTL-driven evictions performed
        during a single ``flush()`` for stores with TTL enabled. Larger values
        increase per-flush latency but let the sweep keep up with higher
        steady-state expiration rates. Only meaningful for TTL-enabled
        stores; ignored otherwise.
        Default - ``10_000``.
    :param legacy_records_ttl: opt-in for enabling TTL on a **populated**
        legacy store that already holds un-stamped records. When ``None``
        (the default), the first ``state.set(..., ttl=...)`` write on a
        populated legacy store raises ``IncompatibleStateStoreError`` —
        byte-for-byte the v3.24 behavior. When set to a strictly positive
        ``timedelta``, the partition instead **backfills** its pre-existing
        un-stamped records with a uniform expiry of
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
        Default - ``10_000``.

    Please see `rocksdict.Options` for a complete description of other options.
    """

    write_buffer_size: int = 64 * 1024 * 1024
    target_file_size_base: int = 64 * 1024 * 1024
    max_write_buffer_number: int = 3
    block_cache_size: int = 128 * 1024 * 1024
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
    max_evictions_per_flush: int = 10_000
    legacy_records_ttl: Optional[timedelta] = None
    legacy_backfill_chunk_size: int = 10_000

    def __post_init__(self) -> None:
        if (
            self.legacy_records_ttl is not None
            and self.legacy_records_ttl <= timedelta(0)
        ):
            raise ValueError(
                "legacy_records_ttl must be a strictly positive timedelta or "
                f"None, got {self.legacy_records_ttl!r}"
            )
        if self.legacy_backfill_chunk_size <= 0:
            raise ValueError(
                "legacy_backfill_chunk_size must be a strictly positive int, "
                f"got {self.legacy_backfill_chunk_size!r}"
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
        table_factory_options.set_block_cache(rocksdict.Cache(self.block_cache_size))
        table_factory_options.set_bloom_filter(
            self.bloom_filter_bits_per_key, block_based=True
        )
        opts.set_block_based_table_factory(table_factory_options)
        compression_type = COMPRESSION_TYPES[self.compression_type]
        opts.set_compression_type(compression_type)
        opts.set_max_total_wal_size(size=self.max_total_wal_size)
        return opts
