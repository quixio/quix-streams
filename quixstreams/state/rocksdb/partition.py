import logging
import time
from datetime import timedelta
from itertools import islice
from typing import (
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
    cast,
)

from rocksdict import AccessType, ColumnFamily, Rdict, ReadOptions, WriteBatch

from quixstreams.state.base import (
    PartitionTransactionCache,
    StorePartition,
)
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    LOCAL_ONLY_CFS,
    METADATA_CF_NAME,
    Marker,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.transaction import RocksDBPartitionTransaction
from quixstreams.state.serialization import int_from_bytes, int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

from .exceptions import IncompatibleStateStoreError, RocksDBCorruptedError
from .metadata import (
    CHANGELOG_OFFSET_KEY,
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
)
from .options import RocksDBOptions
from .ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)
from .types import RocksDBOptionsType

__all__ = ("RocksDBStorePartition",)


logger = logging.getLogger(__name__)


class RocksDBStorePartition(StorePartition):
    """
    A base class to access state in RocksDB.
    It represents a single RocksDB database.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB
     4. Maintaining the per-write TTL machinery (8-byte expiry stamp on every
        value in the user-facing default CF, secondary expiry index in
        ``__ttl_index__``, partition high-water mark, bounded sweep on
        flush, recovery filter / index rebuild on changelog replay).

    It opens the RocksDB on `__init__`. If the db is locked by another process,
    it will retry according to `open_max_retries` and `open_retry_backoff` options.

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    """

    # Class-level switch that subclasses with their own retention model
    # (windowed, timestamped) flip to ``False`` to permanently opt out of the
    # TTL stamp machinery. For general-purpose partitions this stays ``True``
    # at the class level; the **per-instance** ``uses_ttl_stamps`` (set in
    # ``__init__``) decides whether the machinery is *active* — it starts
    # ``False`` on a fresh / legacy store and flips to ``True`` only when a
    # ``state.set(..., ttl=...)`` write is detected at flush time on an empty
    # default CF (see :meth:`_flip_into_ttl_mode`). This keeps no-TTL
    # workloads byte-identical to v3.23.6 on disk and on the changelog.
    uses_ttl_stamps: bool = True

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        if not options:
            options = RocksDBOptions()

        super().__init__(options.dumps, options.loads, changelog_producer)
        self._path = path
        self._options = options
        self._rocksdb_options = self._options.to_options()
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._max_evictions_per_flush = self._options.max_evictions_per_flush
        # Opt-in for backfilling a populated legacy store on TTL enable.
        # ``None`` preserves the current reject-on-populated-store behavior;
        # a strictly positive ``timedelta`` enables the in-place backfill
        # (see :meth:`backfill_legacy_records`). Read by the transaction layer
        # through this partition in ``_maybe_flip_or_reject``.
        self._legacy_records_ttl: Optional[timedelta] = (
            self._options.legacy_records_ttl
        )
        # Number of pre-existing records re-stamped per write-batch during the
        # one-time legacy backfill. Bounds peak transient memory to one chunk
        # (see :meth:`backfill_legacy_records`). Only consulted on the single
        # backfilling flush.
        self._legacy_backfill_chunk_size: int = (
            self._options.legacy_backfill_chunk_size
        )
        self._db = self._init_rocksdb()
        self._cf_cache: Dict[str, Rdict] = {}
        self._cf_handle_cache: Dict[str, ColumnFamily] = {}
        self._high_water_ms: Optional[int] = None
        # Wallclock reference captured once per changelog-recovery session
        # (lazily, on the first stamped default-CF replay). Used to judge
        # whether a replayed TTL entry is already expired and to seed the
        # post-recovery high-water (see :meth:`recover_from_changelog_message`,
        # spec ``spec-recovery-wallclock.md`` §3.2/§3.3). ``None`` means no
        # stamped message has been replayed yet in this partition's lifetime;
        # a fresh partition instance per assignment is a fresh recovery session.
        self._recovery_now_ms: Optional[int] = None

        # Resolve the **runtime** TTL flag. Subclasses that nail
        # ``uses_ttl_stamps = False`` at the class level (windowed,
        # timestamped) stay opted-out forever. Otherwise we read the
        # persisted ``__ttl_enabled__`` flag from the metadata CF: absent
        # means "legacy mode, behave like v3.23.6"; present-and-truthy means
        # "this partition flipped into TTL mode in a previous flush".
        class_uses_ttl_stamps = type(self).uses_ttl_stamps
        if class_uses_ttl_stamps:
            self.uses_ttl_stamps = self._load_ttl_enabled_flag()
        else:
            self.uses_ttl_stamps = False

        if self.uses_ttl_stamps:
            # Already-flipped store: validate the on-disk format and warm up
            # the TTL bookkeeping (high-water, index CF). For legacy stores
            # (the 99% case) we skip every line of this block — no extra CF
            # is created, no extra metadata read happens beyond the single
            # ``__ttl_enabled__`` probe above.
            self._enforce_format_version()
            self._load_high_water()
            # Pre-create the index CF so writes never race a CF creation.
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)

    @property
    def high_water_ms(self) -> Optional[int]:
        """
        Highest record event-time observed by any transaction on this
        partition since the process started, or ``None`` for cold start.
        """
        return self._high_water_ms

    @property
    def max_evictions_per_flush(self) -> int:
        """Cap on per-flush sweep evictions."""
        return self._max_evictions_per_flush

    @property
    def legacy_records_ttl(self) -> Optional[timedelta]:
        """
        Opt-in TTL applied to pre-existing un-stamped records when TTL is
        enabled on a populated legacy store. ``None`` = reject on populated
        store (default); a positive ``timedelta`` = backfill in place.
        """
        return self._legacy_records_ttl

    @property
    def legacy_backfill_chunk_size(self) -> int:
        """Number of records re-stamped per write-batch during the backfill."""
        return self._legacy_backfill_chunk_size

    def advance_high_water(self, timestamp: Optional[int]) -> None:
        """
        Advance the partition's high-water mark monotonically. Called by the
        transaction layer on every TTL-aware ``set`` / ``get`` that carries a
        timestamp. Late-arriving timestamps never roll the high-water back.
        """
        if timestamp is None:
            return
        if self._high_water_ms is None or timestamp > self._high_water_ms:
            self._high_water_ms = timestamp

    def _now_ms(self) -> int:
        """
        Current wallclock time in epoch milliseconds. Isolated behind a method
        purely as a test seam so changelog-recovery determinism cases can
        inject a fixed ``now`` without sleeping (spec §8, case 6).
        """
        return int(time.time() * 1000)

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        cf_handle = self.get_column_family_handle(cf_name)
        batch = WriteBatch(raw_mode=True)

        # Recovery flag-discovery (spec §6.6).
        #
        # The ``__ttl_enabled__`` key lives in the metadata CF, which is in
        # ``LOCAL_ONLY_CFS`` and therefore never produced to the changelog
        # topic — so a cold-restore recovery cannot read the flag from a
        # changelog message. We instead infer the partition's TTL mode from
        # the **first user-data message replayed**: if its value is
        # stamped (8-byte BE prefix decodable as the sentinel or a
        # plausible epoch-ms expiry), the source partition was flipped, so
        # we flip this recovery partition too and stay in TTL mode for the
        # rest of replay. This is the "peek at the first replayed value"
        # design called out as the fallback in spec §8.
        if (
            type(self).uses_ttl_stamps
            and not self.uses_ttl_stamps
            and cf_name == "default"
            and value
            and self._looks_like_stamped_value(value)
        ):
            logger.info(
                "Recovery: detected stamped default-CF replay; flipping "
                "partition path=%s into TTL mode for the rest of recovery.",
                self._path,
            )
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            # Stamp the on-disk format-version + flag now so a subsequent
            # process restart picks up TTL mode at open time.
            self._stamp_flip_metadata()

        if not self.uses_ttl_stamps:
            # Legacy / non-TTL partitions: replay the raw payload verbatim.
            # Identical to v3.23.6 behavior — no stamp wrapping, no index
            # rebuild, no recovery filter.
            if value is None:
                batch.delete(key, cf_handle)
            else:
                batch.put(key, value, cf_handle)
            self._update_changelog_offset(batch=batch, offset=offset)
            self._write(batch)
            return

        if cf_name in LOCAL_ONLY_CFS:
            # Local-only CFs should never appear on the changelog topic, but
            # if a bogus message arrives, ignore the payload and just roll
            # the offset forward.
            self._update_changelog_offset(batch=batch, offset=offset)
            self._write(batch)
            return

        # Only the user-facing "default" CF carries stamped values. Any other
        # CF (metadata, global counter, etc.) is replayed verbatim.
        is_main_cf = cf_name == "default"

        if value is None:
            batch.delete(key, cf_handle)
        elif is_main_cf:
            stamped, stamp = self._normalize_replay_value(value)
            # Judge expiry against the current wallclock captured once per
            # recovery session, NOT against a stamp-ratcheted pseudo-clock
            # (the old ``recovery_now = self._high_water_ms`` ratchet collapsed
            # uniform-expiry backfilled stores; see spec-recovery-wallclock.md
            # §2/§3). Capture lazily on the first stamped default-CF replay and
            # seed the post-recovery high-water to the same reference so the
            # recovery→live boundary is continuous (§3.3). Sentinel-stamped
            # entries are never compared and always survive (§7).
            if stamp != SENTINEL_NEVER and self._recovery_now_ms is None:
                self._recovery_now_ms = self._now_ms()
                self._high_water_ms = self._recovery_now_ms
            if stamp != SENTINEL_NEVER and stamp <= self._recovery_now_ms:
                # Already-expired against wallclock-at-recovery; skip both the
                # main and the index write. Roll the changelog offset forward
                # so recovery progresses. The __ttl_index__ stays consistent
                # with survivors — a dropped entry writes neither (§7).
                pass
            else:
                batch.put(key, stamped, cf_handle)
                if stamp != SENTINEL_NEVER:
                    index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
                    batch.put(encode_index_key(stamp, key), b"", index_handle)
        else:
            batch.put(key, value, cf_handle)

        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
        batch: Optional[WriteBatch] = None,
    ):
        """
        Write data to RocksDB.

        For TTL-enabled partitions this also persists the high-water mark and
        runs the bounded sweep over the secondary expiry index, all within
        the same WriteBatch so the on-disk commit is atomic.

        For legacy / unflipped partitions (the 99% no-TTL workload) the path
        is byte-identical to v3.23.6: no stamp prefix, no high-water write,
        no sweep, no index CF use. The hot-path branch is a single Python
        attribute check.

        :param cache: The modified data
        :param changelog_offset: The changelog message offset of the data.
        :param batch: prefilled `rocksdict.WriteBatch`, optional.
        """
        if batch is None:
            batch = WriteBatch(raw_mode=True)

        column_families = cache.get_column_families()

        # Iterate over the transaction update cache and stage writes verbatim.
        # For unflipped partitions this commits the cache as-is — exactly the
        # v3.23.6 behavior. For flipped partitions the transaction layer has
        # already stamped values and emitted index-CF writes into the cache.
        for cf_name in column_families:
            cf_handle = self.get_column_family_handle(cf_name)

            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    batch.put(key, value, cf_handle)

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                batch.delete(key, cf_handle)

        if self.uses_ttl_stamps and self._high_water_ms is not None:
            batch.put(
                TTL_HIGH_WATER_KEY,
                int_to_bytes(self._high_water_ms),
                self.get_column_family_handle(METADATA_CF_NAME),
            )

        if self.uses_ttl_stamps:
            self._run_sweep(batch=batch)

        # Save the latest changelog topic offset to know where to recover from
        # It may be None if changelog topics are disabled
        if changelog_offset is not None:
            self._update_changelog_offset(batch=batch, offset=changelog_offset)
        logger.debug(
            f"Flushing state changes to the disk "
            f'path="{self.path}" '
            f"changelog_offset={changelog_offset} "
            f"bytes_total={batch.size_in_bytes()}"
        )

        self._write(batch)

    # ------------------------------------------------------------------
    # TTL flip / probe helpers (used by the transaction at flush time).
    # ------------------------------------------------------------------

    def main_cf_has_user_data(self) -> bool:
        """
        Return True if the default column family already contains at least one
        entry. Used by the transaction layer at flush time to decide between
        the empty-store flip path and the populated-store rejection path.

        ``seek_to_first`` on the default CF runs once per partition lifetime
        (only on the flush that flips), so its cost is irrelevant.
        """
        return self._main_cf_has_user_data()

    def estimated_main_cf_key_count(self, cap: int = 10_000) -> int:
        """
        Best-effort count of the keys in the default CF, used in the
        rejection error message to give the operator a rough scale of the
        state they are about to wipe.

        rocksdict does not expose RocksDB's ``GetEstimatedNumKeys``, so we
        iterate up to ``cap`` keys; "saturated" means ">= cap". This runs
        once per partition lifetime (only on the rejection path), so the
        cost is irrelevant. Returns 0 only if the iteration fails for a
        reason other than emptiness — the operator-visible contract
        documents 0 as "unknown but non-zero".
        """
        try:
            cf = self.get_or_create_column_family("default")
            count = 0
            for _ in cf.items():
                count += 1
                if count >= cap:
                    break
            return count
        except Exception:
            return 0

    def flip_into_ttl_mode(self, batch: WriteBatch) -> None:
        """
        Atomically flip this partition into TTL mode.

        Called by the transaction layer from ``flush()`` when a TTL write is
        detected on a partition whose default CF is empty (the empty-store
        fast path). Writes ``__ttl_enabled__`` and ``__ttl_format_version__``
        to the metadata CF in the **same** ``batch`` as the first stamped
        user writes, so the change is atomic on disk and replayable through
        the changelog.

        After this call:

        - ``self.uses_ttl_stamps`` is True; the next transaction starts in
          TTL mode and stamps inline.
        - The ``__ttl_index__`` CF exists; subsequent writes can index
          non-sentinel entries.
        - The ``__ttl_format_version__`` marker is on disk; future opens
          take the TTL-aware branch in ``__init__``.
        """
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        batch.put(TTL_ENABLED_KEY, b"\x01", metadata_handle)
        batch.put(
            STATE_FORMAT_VERSION_KEY,
            int_to_bytes(STATE_FORMAT_VERSION),
            metadata_handle,
        )
        # Lazily create the index CF on first need.
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        self.uses_ttl_stamps = True

    def backfill_legacy_records(
        self,
        expires_at_ms: int,
        changelog_producer: Optional[ChangelogProducer],
        processed_offsets: Optional[dict[str, int]],
        staged_default_keys: set[bytes],
        chunk_size: int,
    ) -> int:
        """
        Re-stamp every pre-existing record in the default CF with a uniform
        ``expires_at_ms`` expiry in **bounded chunks**, persisting and producing
        each chunk before reading the next so peak transient memory is one chunk
        regardless of total store size (spec ``spec-chunked-backfill.md`` §3).

        Called by the transaction layer from ``prepare()`` when a TTL write is
        detected on a partition whose default CF is **populated** *and*
        ``legacy_records_ttl`` is set (the backfill branch of
        ``_maybe_flip_or_reject``). The companion empty-store flip path
        (:meth:`flip_into_ttl_mode`) handles the empty-CF case.

        Per chunk (up to ``chunk_size`` (key, value) pairs read from a single
        forward iterator over the default CF):

        1. Build a ``WriteBatch`` of default-CF puts (the re-stamped values) and
           ``__ttl_index__`` puts (``encode_index_key(expires_at_ms, key)``).
        2. Produce the re-stamped default-CF records to the changelog (the index
           CF is local-only and is rebuilt on recovery), then ``flush()`` the
           producer so its in-flight queue stays bounded to one chunk.
        3. Commit the batch with the raw writer ``self._write(batch)`` — NOT
           ``self.write(...)`` — so no sweep runs (the partition is still legacy
           during the backfill) and the per-chunk default+index puts commit
           atomically together.
        4. Drop the chunk's structures before reading the next.

        This deliberately departs from the "transaction writes once at flush"
        model — the chunk loop issues ``_db.write()`` per chunk *before* the
        transaction's own ``flush()``. It is sound because the writes target
        pre-existing keys the transaction is not otherwise touching (those are
        in ``staged_default_keys`` and skipped) and are forward-only convergent.

        Idempotency / re-run safety (spec §4): a crash before the caller writes
        ``__ttl_enabled__`` leaves the partition legacy, so a re-run re-reads the
        already-stamped chunks. For each record this:

        - skips it if it already decodes to exactly ``expires_at_ms`` (no-op);
        - re-stamps from the **stripped payload** (``decode_ttl_value`` →
          payload), never from the stamped blob, if it decodes to a *different*
          stamp (prior partial run with a different expiry) — no double-wrap,
          converging to a uniform expiry; the stale index key is deleted;
        - wraps the whole value (first run / genuine legacy value) otherwise.

        "Already stamped" reuses :meth:`_looks_like_stamped_value` exactly, so
        the heuristic matches the recovery path (spec §4.2). The only residual
        hazard is a genuine legacy value whose leading 8 bytes coincidentally
        decode to exactly ``expires_at_ms`` (skipped → left un-stamped). That is
        a specific 64-bit collision; accepted and documented (spec §4.1, OP-CB-1)
        — the same heuristic class already used by ``_looks_like_stamped_value``.

        The caller writes ``__ttl_enabled__`` / the format version **last**,
        after this method returns, so the flip is durable only once every chunk
        has landed (spec §3.3 flag-last ordering).

        :param expires_at_ms: uniform absolute event-time expiry to stamp on
            every pre-existing record (``high_water + legacy_records_ttl``).
        :param changelog_producer: the partition's changelog producer, or
            ``None`` when changelog topics are disabled (chunks still persist
            locally; production is skipped).
        :param processed_offsets: ``<topic: offset>`` of the latest processed
            message, encoded into the changelog headers exactly as the base
            ``_prepare`` path does.
        :param staged_default_keys: serialized default-CF keys present in the
            current transaction's update cache (genuine in-batch user writes).
            They are skipped here and re-stamped with their own true pending
            stamp by ``_restamp_default_cf_cache_for_flip``.
        :return: count of pre-existing records re-stamped (skipped no-ops are
            not counted).
        """
        # Pre-create the index CF so the per-chunk batch never races a CF
        # creation.
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        default_handle = self.get_column_family_handle("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)

        headers = None
        if changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(
                    processed_offsets
                ),
            }

        # Single forward iterator over the default CF for the whole backfill.
        # RocksDB iterators read consistent snapshot data; the per-chunk writes
        # put re-stamped values at the same (already-passed) keys, so they are
        # not revisited within a single run. A crash + re-run uses a fresh
        # iterator and relies on the already-stamped detection above.
        iterator: Iterator[tuple[bytes, bytes]] = cast(
            Iterator[tuple[bytes, bytes]], default_cf.items()
        )

        restamped = 0
        while True:
            # READ one chunk into a bounded local list.
            chunk = list(islice(iterator, chunk_size))
            if not chunk:
                break

            # RE-STAMP + build this chunk's WriteBatch (default + index puts),
            # and collect the produce payloads so the producer queue is bounded
            # to one chunk.
            batch = WriteBatch(raw_mode=True)
            produce: list[tuple[bytes, bytes]] = []
            for raw_key, raw_value in chunk:
                key = cast(bytes, raw_key)
                value = cast(bytes, raw_value)
                if key in staged_default_keys:
                    # Genuine in-batch user write; re-stamped by the caller.
                    continue

                stamped = self._restamp_one_for_backfill(
                    key=key,
                    value=value,
                    expires_at_ms=expires_at_ms,
                    batch=batch,
                    default_handle=default_handle,
                    index_handle=index_handle,
                )
                if stamped is None:
                    # Already at the target expiry — skip (no-op put avoided).
                    continue
                produce.append((key, stamped))
                restamped += 1

            # PRODUCE this chunk's re-stamped default-CF records, then flush so
            # the producer's in-flight queue does not grow across chunks.
            if changelog_producer is not None and produce:
                for key, stamped in produce:
                    changelog_producer.produce(
                        key=key, value=stamped, headers=headers
                    )
                changelog_producer.flush()

            # COMMIT this chunk atomically (default + index puts together).
            self._write(batch)

            # RELEASE: drop the chunk's structures before the next iteration.
            del chunk, batch, produce

        return restamped

    def _restamp_one_for_backfill(
        self,
        key: bytes,
        value: bytes,
        expires_at_ms: int,
        batch: WriteBatch,
        default_handle: ColumnFamily,
        index_handle: ColumnFamily,
    ) -> Optional[bytes]:
        """
        Stage the (default + index) puts re-stamping one default-CF record to
        ``expires_at_ms`` into ``batch`` (spec §4). Returns the new stamped
        default-CF blob, or ``None`` if the record is already at the target
        expiry and was skipped (no put staged).

        Three cases:

        - value looks stamped (reuse :meth:`_looks_like_stamped_value`) and its
          decoded stamp already equals ``expires_at_ms`` → skip (return None);
        - value looks stamped with a *different* stamp (prior partial run) →
          re-stamp from the **stripped payload** (no double-wrap) and delete the
          stale ``encode_index_key(stamp_old, key)`` (spec §4.3);
        - otherwise (first run / genuine legacy value) → wrap the whole value.
        """
        if self._looks_like_stamped_value(value):
            stamp_old, payload = decode_ttl_value(value)
            if stamp_old == expires_at_ms:
                # Already converged to this run's uniform expiry.
                return None
            # Prior-run stamp with a different expiry: re-stamp from the
            # stripped payload (never the stamped blob) and fix the index.
            stamped = encode_ttl_value(expires_at_ms, payload)
            if stamp_old != SENTINEL_NEVER:
                batch.delete(encode_index_key(stamp_old, key), index_handle)
        else:
            # First run / genuine legacy value: wrap the whole value.
            stamped = encode_ttl_value(expires_at_ms, value)

        batch.put(key, stamped, default_handle)
        batch.put(encode_index_key(expires_at_ms, key), b"", index_handle)
        return stamped

    def reject_ttl_on_populated_store(self) -> "IncompatibleStateStoreError":
        """
        Build (and log) the structured ERROR raised when a TTL write lands on
        a partition that has existing un-stamped data **and** the operator did
        not opt in to backfill. The caller is expected to ``raise`` the
        returned exception; emitting the log line here keeps the message format
        in one place.

        Silent TTL drop is the worst possible failure mode for the dedup
        workload this feature exists for, so we halt loudly. The message points
        at the operator-callable fix (set ``legacy_records_ttl``), not at
        deleting the state directory — Quix Cloud has no customer-callable
        state reset.
        """
        approx_keys = self.estimated_main_cf_key_count()
        msg = (
            f"IncompatibleStateStoreError: state store at {self._path!r} "
            f"has {approx_keys} un-stamped existing entries; cannot enable "
            "TTL on a populated store without backfilling them. To enable "
            "TTL in place, set a uniform expiry for the existing records via "
            "RocksDBOptions(legacy_records_ttl=timedelta(...)) and redeploy: "
            "the partition will re-stamp every existing record with an expiry "
            "of high-water + legacy_records_ttl (event-time at enable) and "
            "flip into TTL mode without deleting any state. New records keep "
            "their true event-time expiry."
        )
        logger.error(
            msg,
            extra={
                "state_store_path": self._path,
                "approx_key_count": approx_keys,
                "operator_action": "set_legacy_records_ttl",
            },
        )
        return IncompatibleStateStoreError(msg)

    def _write(self, batch: WriteBatch):
        """
        Write `WriteBatch` to RocksDB
        :param batch: an instance of `rocksdict.WriteBatch`
        """
        self._db.write(batch)

    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from RocksDB.

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the DB. Otherwise, `default`
        """
        result = self.get_or_create_column_family(cf_name).get(
            key, default=Marker.UNDEFINED
        )

        # RDict accept Any type as value but we only write bytes so we should only get bytes back.
        return cast(Union[bytes, Literal[Marker.UNDEFINED]], result)

    def iter_items(
        self,
        lower_bound: bytes,  # inclusive
        upper_bound: bytes,  # exclusive
        backwards: bool = False,
        cf_name: str = "default",
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Iterate over key-value pairs within a specified range in a column family.

        :param lower_bound: The lower bound key (inclusive) for the iteration range.
        :param upper_bound: The upper bound key (exclusive) for the iteration range.
        :param backwards: If `True`, iterate in reverse order (descending).
            Default is `False` (ascending).
        :param cf_name: The name of the column family to iterate over.
            Default is "default".
        :return: An iterator yielding (key, value) tuples.
        """
        cf = self.get_or_create_column_family(cf_name=cf_name)

        # Set iterator bounds to reduce IO by limiting the range of keys fetched
        read_opt = ReadOptions()
        read_opt.set_iterate_lower_bound(lower_bound)
        read_opt.set_iterate_upper_bound(upper_bound)

        from_key = upper_bound if backwards else lower_bound

        # RDict accepts Any type as value but we only write bytes so we should only get bytes back.
        items = cast(
            Iterator[tuple[bytes, bytes]],
            cf.items(from_key=from_key, read_opt=read_opt, backwards=backwards),
        )

        if not backwards:
            # NOTE: Forward iteration respects bounds correctly.
            # Also, we need to use yield from notation to replace RdictItems
            # with Python-native generator or else garbage collection
            # will make the result unpredictable.
            yield from items
        else:
            # NOTE: When iterating backwards, the `read_opt` lower bound
            # is not respected by Rdict for some reason. We need to manually
            # filter it here.
            for key, value in items:
                if key < lower_bound:
                    # Exit early if the key falls below the lower bound
                    break
                yield key, value

    def begin(self) -> RocksDBPartitionTransaction:
        return RocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the DB.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        cf_dict = self.get_or_create_column_family(cf_name)
        return key in cf_dict

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)
        if offset_bytes is None:
            return None

        return int_from_bytes(offset_bytes)

    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """
        batch = WriteBatch(raw_mode=True)
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def close(self):
        """
        Close the underlying RocksDB
        """
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        # Clean the column family caches to drop references
        # Otherwise the Rocksdb won't close properly
        self._cf_handle_cache = {}
        self._cf_cache = {}
        self._db.close()
        logger.debug(f'Closed rocksdb partition on "{self._path}"')

    @property
    def path(self) -> str:
        """
        Absolute path to RocksDB database folder
        :return: file path
        """
        return self._path

    @classmethod
    def destroy(cls, path: str):
        """
        Delete underlying RocksDB database

        The database must be closed first.

        :param path: an absolute path to the RocksDB folder
        """
        Rdict.destroy(path=path)

    def get_column_family_handle(self, cf_name: str) -> ColumnFamily:
        """
        Get a column family handle to pass to it WriteBatch.
        This method will cache the CF handle instance to avoid creating them
        repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.ColumnFamily`
        """
        if (cf_handle := self._cf_handle_cache.get(cf_name)) is None:
            self.get_or_create_column_family(cf_name)
            cf_handle = self._db.get_column_family_handle(cf_name)
            self._cf_handle_cache[cf_name] = cf_handle
        return cf_handle

    def get_or_create_column_family(self, cf_name: str) -> Rdict:
        """
        Get a column family instance.
        This method will cache the CF instance to avoid creating them repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.Rdict` for the given column family
        """
        if (cf := self._cf_cache.get(cf_name)) is None:
            try:
                cf = self._db.get_column_family(cf_name)
            except Exception as exc:
                if "does not exist" not in str(exc):
                    raise
                cf = self._db.create_column_family(
                    cf_name, options=self._rocksdb_options
                )
            self._cf_cache[cf_name] = cf
        return cf

    def list_column_families(self) -> List[str]:
        return self._db.list_cf(self._path)

    # ------------------------------------------------------------------
    # TTL machinery (only used when ``uses_ttl_stamps`` is True).
    # ------------------------------------------------------------------

    def _run_sweep(self, batch: WriteBatch) -> None:
        """
        Bounded-budget sweep over the secondary expiry index.

        Called from :meth:`write` so any deletes go into the same batch as
        the user-driven writes for atomicity.
        """
        if self._high_water_ms is None:
            # Cold start: no event-time established yet — skip the sweep.
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index_cf = self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        main_cf = self.get_or_create_column_family("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        main_handle = self.get_column_family_handle("default")

        # Bound the iterator at the cutoff stamp to skip future expiries
        # without paying for the iterator step. Build the cutoff prefix as
        # 8 BE bytes equal to ``now_ms + 1`` so any entry whose first
        # 8 bytes equal exactly ``now_ms`` is still iterated.
        upper_bound = int_to_bytes(now_ms + 1) if now_ms < (2**64 - 1) else None
        read_opt = ReadOptions()
        if upper_bound is not None:
            read_opt.set_iterate_upper_bound(upper_bound)

        evicted = 0
        iterator: Iterator[tuple[bytes, bytes]] = cast(
            Iterator[tuple[bytes, bytes]],
            index_cf.items(from_key=b"", read_opt=read_opt),
        )
        for index_key, _ in iterator:
            if evicted >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                batch.delete(index_key, index_handle)
                continue

            if idx_expires_at > now_ms:
                # Sorted by expiry — the rest is in the future.
                break

            main_value = main_cf.get(user_key, default=None)
            if main_value is None:
                # Ghost: user deleted the main entry but the index still
                # points at it. GC the orphaned index entry.
                batch.delete(index_key, index_handle)
                continue

            try:
                main_expires_at, _ = decode_ttl_value(cast(bytes, main_value))
            except ValueError:
                logger.warning(
                    "Malformed TTL value at %s for key %r; dropping orphan "
                    "index entry but leaving main untouched.",
                    self._path,
                    user_key,
                )
                batch.delete(index_key, index_handle)
                continue

            if main_expires_at == SENTINEL_NEVER:
                # Ghost: key was overwritten by a plain ``state.set`` and
                # is now permanent. Drop the stale index pointer only.
                batch.delete(index_key, index_handle)
                continue

            if main_expires_at == idx_expires_at:
                batch.delete(user_key, main_handle)
                batch.delete(index_key, index_handle)
                evicted += 1
            else:
                # Ghost: key was overwritten with a fresh expiry stamp.
                batch.delete(index_key, index_handle)

        if evicted:
            logger.debug(
                "TTL sweep evicted %d expired entries on partition path=%s "
                "now_ms=%d budget=%d",
                evicted,
                self._path,
                now_ms,
                budget,
            )

    def _enforce_format_version(self) -> None:
        """
        Validate the on-disk format-version marker on a partition that is
        already flipped into TTL mode (``__ttl_enabled__`` was True at open
        time). A flipped store must carry a marker whose value is at least
        :data:`STATE_FORMAT_VERSION`; anything older is forward-incompatible
        and the operator must reset the state directory.

        Stores that never enabled TTL never enter this method — they have no
        marker by design (and remain byte-identical to v3.23.6 on disk).
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(STATE_FORMAT_VERSION_KEY, default=None)
        if raw is None:
            # Defensive: a flipped store must have the marker. Treat its
            # absence as "needs reset" rather than silently re-stamp.
            raise IncompatibleStateStoreError(
                f'State store at "{self._path}" is marked TTL-enabled but '
                "is missing its format-version marker. Delete the state "
                "directory and let recovery rebuild from the changelog."
            )
        try:
            version = int_from_bytes(cast(bytes, raw))
        except Exception:
            version = -1
        if version < STATE_FORMAT_VERSION:
            raise IncompatibleStateStoreError(
                f'State store at "{self._path}" was written with an older '
                f"on-disk layout (version={version}); the running version "
                f"requires {STATE_FORMAT_VERSION}. Delete the state "
                "directory and let recovery rebuild from the changelog."
            )

    def _load_ttl_enabled_flag(self) -> bool:
        """
        Read the persistent ``__ttl_enabled__`` flag from the metadata CF.
        Absent / falsy = legacy mode (the 99% case, byte-identical to
        v3.23.6). Present-and-truthy = the partition flipped into TTL mode
        on a previous flush; resume the v2 stamped path on the next write.
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(TTL_ENABLED_KEY, default=None)
        if raw is None:
            return False
        # Any non-empty bytes value counts as "True" — we currently write
        # ``b"\x01"`` but stay liberal in what we accept.
        return bool(raw)

    def _stamp_flip_metadata(self) -> None:
        """
        Persist the TTL-enabled flag and the format-version marker to the
        metadata CF on disk **outside of a user write batch**. Used by the
        recovery flag-discovery path: when recovery detects a stamped
        replayed value, it flips the partition immediately so the rest of
        replay decodes correctly, and a subsequent process restart picks
        up TTL mode at open time.
        """
        batch = WriteBatch(raw_mode=True)
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        batch.put(TTL_ENABLED_KEY, b"\x01", metadata_handle)
        batch.put(
            STATE_FORMAT_VERSION_KEY,
            int_to_bytes(STATE_FORMAT_VERSION),
            metadata_handle,
        )
        self._write(batch)

    def _looks_like_stamped_value(self, value: bytes) -> bool:
        """
        Heuristic used by the recovery flag-discovery path to decide whether
        a replayed default-CF value originated from a flipped store.

        Conservative recognizer: the value must be at least 8 bytes long,
        and the leading 8 BE bytes must be either:

        - ``SENTINEL_NEVER`` (always-true marker for "never expires"), or
        - a plausible epoch-millisecond expiry — strictly positive and
          smaller than 10^15 ms (≈ year 33658) which comfortably bounds
          any realistic event-time TTL while excluding sentinel collisions
          and most "this is actually serialized user data" false positives.

        False negatives (a flipped store that produced an unrecognizable
        first value) would leave the recovery partition in legacy mode —
        which then writes un-stamped values back to disk and effectively
        clears TTL on that key. The 10^15 ms cap is generous enough that
        this is unlikely in practice; the documented operator action is
        still "delete state directory and let recovery rebuild" if the
        heuristic misfires.
        """
        if len(value) < 8:
            return False
        try:
            stamp, _ = decode_ttl_value(value)
        except ValueError:
            return False
        if stamp == SENTINEL_NEVER:
            return True
        # ~year 33658; far beyond any realistic event-time clock.
        return 0 < stamp < 10**15

    def _main_cf_has_user_data(self) -> bool:
        """
        Return True if the default column family contains at least one
        non-metadata user entry.
        """
        default_cf = self.get_or_create_column_family("default")
        for _ in default_cf.items():
            return True
        return False

    def _load_high_water(self) -> None:
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(TTL_HIGH_WATER_KEY, default=None)
        if raw is None:
            return
        try:
            self._high_water_ms = int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning(
                "Failed to decode persisted TTL high-water at %s; "
                "treating it as undefined.",
                self._path,
            )

    def _normalize_replay_value(self, value: bytes) -> tuple[bytes, int]:
        """
        Decode a replayed main-CF value into ``(stamped_blob, stamp)``.

        - Already-stamped values (8 bytes prefix decodable) round-trip with
          their original stamp.
        - Unstamped legacy values (e.g. from a pre-v2 changelog topic) are
          wrapped with the sentinel so downstream reads round-trip
          natively.

        Heuristic: a value strictly shorter than 8 bytes cannot carry a
        stamp; one of length >= 8 is always treated as stamped, since the
        format-version guard prevents a populated unstamped on-disk store
        from ever reaching this method. Pre-v2 *changelog* values may still
        be unstamped, but those are replayed into a fresh (empty) on-disk
        store after the guard has stamped a fresh format-version marker —
        the guard's check ran on an empty default CF and passed. To
        distinguish them, we wrap any value whose first 8 bytes do not
        resemble a plausible stamp range. To stay safe we wrap *only* if
        the value is shorter than 8 bytes; anything else is assumed
        already-stamped. The risk: a pre-v2 changelog value of >=8 bytes
        will be misinterpreted. The mitigation, called out in the spec
        §6.8 / §8, is that operators must also rebuild the changelog topic
        when crossing the v3.23.6 → v2 boundary if they want clean replay.
        """
        if len(value) < 8:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        try:
            stamp, _ = decode_ttl_value(value)
        except ValueError:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        return value, stamp

    # ------------------------------------------------------------------
    # RocksDB lifecycle.
    # ------------------------------------------------------------------

    def _open_rocksdict(self) -> Rdict:
        options = self._rocksdb_options
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        create_rdict = lambda: Rdict(
            path=self._path,
            options=options,
            access_type=AccessType.read_write(),
        )
        # TODO: Add docs

        try:
            rdict = create_rdict()
        except Exception as exc:
            if not str(exc).startswith("Corruption"):
                raise
            elif not self._changelog_producer:
                raise RocksDBCorruptedError(
                    f'State store at "{self._path}" is corrupted '
                    f"and cannot be recovered from the changelog topic: "
                    "`use_changelog_topics` is set to False."
                ) from exc
            elif not self._options.on_corrupted_recreate:
                raise RocksDBCorruptedError(
                    f'State store at "{self._path}" is corrupted '
                    f"but may be recovered from the changelog topic. "
                    "`on_corrupted_recreate` is set to False; "
                    "remove the override (or pass "
                    "`rocksdb_options=RocksDBOptions(..., on_corrupted_recreate=True)`) "
                    "to destroy the corrupted state "
                    "and recover it from the changelog."
                ) from exc

            logger.warning(f"Destroying corrupted RocksDB path={self._path}")
            Rdict.destroy(self._path)
            logger.warning(f"Recreating corrupted RocksDB path={self._path}")
            rdict = create_rdict()

        # Ensure metadata column family is created without defining it upfront
        try:
            rdict.get_column_family(METADATA_CF_NAME)
        except Exception as exc:
            if "does not exist" in str(exc):
                rdict.create_column_family(METADATA_CF_NAME, options=options)
            else:
                raise

        return rdict

    def _init_rocksdb(self) -> Rdict:
        attempt = 1
        while True:
            logger.debug(
                f'Opening rocksdb partition on "{self._path}" attempt={attempt}',
            )
            try:
                db = self._open_rocksdict()
                logger.debug(
                    f'Successfully opened rocksdb partition on "{self._path}"',
                )
                return db
            except Exception as exc:
                is_locked = str(exc).lower().startswith("io error")
                if not is_locked:
                    raise

                if self._open_max_retries <= 0 or attempt >= self._open_max_retries:
                    raise

                logger.warning(
                    f"Failed to open rocksdb partition , cannot acquire a lock. "
                    f"Retrying in {self._open_retry_backoff}sec."
                )

                attempt += 1
                time.sleep(self._open_retry_backoff)

    def _update_changelog_offset(self, batch: WriteBatch, offset: int):
        batch.put(
            CHANGELOG_OFFSET_KEY,
            int_to_bytes(offset),
            self.get_column_family_handle(METADATA_CF_NAME),
        )
