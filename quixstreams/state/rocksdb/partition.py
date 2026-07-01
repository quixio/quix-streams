import logging
import time
from datetime import timedelta
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
    CHANGELOG_TTL_STAMPED_HEADER,
    LOCAL_ONLY_CFS,
    METADATA_CF_NAME,
    Marker,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.transaction import (
    RocksDBPartitionTransaction,
    _ttl_to_ms,
)
from quixstreams.state.serialization import int_from_bytes, int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

from .exceptions import IncompatibleStateStoreError, RocksDBCorruptedError
from .metadata import (
    CHANGELOG_OFFSET_KEY,
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_PROGRESS_KEY,
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

# Census-size threshold above which the in-memory key list is logged as a
# future spill-to-disk concern (OP-BC-2, spec-backfill-completeness.md §5).
# At ~80 B per held key, this is ~800 MB — large enough to threaten a small
# container. The backfill still proceeds in memory; the warning only flags
# that a multi-million-key store should grow a disk-spill census in future.
_CENSUS_SPILL_WARN_THRESHOLD = 3_000_000


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
        # Incomplete-migration detection (spec §8.8). Set True on the first
        # header-true default-CF replay (the same condition that flips the
        # partition into TTL mode). Combined with a non-empty
        # ``__ttl_backfill_pending__`` CF at end of recovery, it marks a MIXED
        # (incomplete-migration) changelog whose leftover legacy records must be
        # completed by :meth:`complete_recovery`. False (the all-legacy first-
        # enablement case) never triggers completion.
        self._recovery_saw_stamped: bool = False

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
        self,
        key: bytes,
        value: Optional[bytes],
        cf_name: str,
        offset: int,
        ttl_stamped: bool = False,
    ):
        cf_handle = self.get_column_family_handle(cf_name)
        batch = WriteBatch(raw_mode=True)

        # Recovery flip-discovery (spec §8.7).
        #
        # The ``__ttl_enabled__`` key lives in the metadata CF, which is in
        # ``LOCAL_ONLY_CFS`` and therefore never produced to the changelog
        # topic — so a cold-restore recovery cannot read the flag from a
        # changelog message. Instead, every stamped ``default``-CF record
        # produced while the source partition was in TTL mode carries the
        # out-of-band ``__ttl_stamped__`` header (set in the base ``_prepare``),
        # surfaced here as ``ttl_stamped``. On the first header-true default-CF
        # record we flip this recovery partition into TTL mode and latch for the
        # rest of the session. This REPLACES the old value-content heuristic
        # (``_looks_like_stamped_value``), which false-positived on legacy 8-byte
        # epoch-ms values and dropped them (OP-3 / §8.6.3). Header absent → the
        # record is legacy / un-stamped and replays verbatim below, so a purely
        # legacy changelog never latches (the §8.6 Option-1 requirement).
        if (
            type(self).uses_ttl_stamps
            and not self.uses_ttl_stamps
            and cf_name == "default"
            and ttl_stamped
        ):
            logger.info(
                "Recovery: __ttl_stamped__ header on default-CF replay; flipping "
                "partition path=%s into TTL mode for the rest of recovery.",
                self._path,
            )
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            # Stamp the on-disk format-version + flag now so a subsequent
            # process restart picks up TTL mode at open time.
            self._stamp_flip_metadata()

        # Incomplete-migration detection / census (spec §8.8). The
        # stamped-vs-legacy decision is per-record on the ``ttl_stamped`` header,
        # NOT on the latched ``uses_ttl_stamps`` flag: a MIXED changelog replays
        # header-absent legacy records AFTER the partition has flipped, and those
        # must still land verbatim (never re-wrapped) and be censused into
        # ``__ttl_backfill_pending__`` for the completion backfill. The pending
        # bookkeeping rides the SAME WriteBatch as the default-CF write so it is
        # atomic with the replay (§4.1).
        if type(self).uses_ttl_stamps and cf_name == "default":
            pending_handle = self.get_column_family_handle(
                TTL_BACKFILL_PENDING_CF_NAME
            )
            if ttl_stamped:
                # A header-true default-CF record. Mark that the partition
                # replayed at least one stamped record (the MIXED-detection
                # half) and drop any earlier legacy census entry for this key —
                # a later stamped write supersedes it (compaction ordering, §4).
                self._recovery_saw_stamped = True
                batch.delete(key, pending_handle)
            elif value is not None:
                # A header-absent (legacy) default-CF record. Census the key as a
                # leftover-legacy candidate; it lands verbatim below. A tombstone
                # (value is None) is not a leftover record and is not censused.
                batch.put(key, b"", pending_handle)

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
        elif is_main_cf and not ttl_stamped:
            # Header-absent default-CF record replayed on a (now-)flipped
            # partition — a leftover legacy record of an interrupted migration
            # (MIXED changelog, §8.8). It MUST land verbatim (no stamp wrap, no
            # index entry, no Rule 4 filter): its key was just censused into the
            # pending CF and the completion backfill (:meth:`complete_recovery`)
            # will stamp it at end of recovery. Routing here on the per-record
            # header — not on the latched flag — is the §6.2 fix.
            batch.put(key, value, cf_handle)
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

    def complete_recovery(self) -> None:
        """
        Recovery-finalize hook (spec §8.8). Called once by the recovery manager
        after this partition has replayed its changelog up to the high-watermark
        and before it is handed to live processing.

        Completes an **interrupted legacy-TTL migration**. During replay a MIXED
        changelog (some ``__ttl_stamped__``-header records + some header-absent
        legacy records) flips the partition into TTL mode on the first stamped
        record (so ``_recovery_saw_stamped`` is True) and lands the leftover
        legacy records verbatim while censusing their keys into
        ``__ttl_backfill_pending__``. Those leftovers are otherwise stranded as
        never-expiring forever (the live ``ttl=`` write sees an already-flipped
        partition and the backfill gate short-circuits — OP-4).

        Trigger (only the MIXED shape — §7):

        - if NOT ``_recovery_saw_stamped`` → all-legacy first-enablement (§8.6);
          the live first-``ttl=``-write backfill owns it. No-op.
        - if the pending CF is empty → all-stamped / fully-migrated (§8.7);
          nothing to complete. No-op.
        - else (stamped seen AND pending non-empty) → incomplete migration:
            - if ``legacy_records_ttl is None`` → **reject loudly** with the
              operator-callable :class:`IncompatibleStateStoreError` (§8): the
              field is required to finish the migration; restore it and redeploy
              (do NOT delete state).
            - else → chunk-backfill exactly the pending keys, stamping each with
              ``self._recovery_now_ms + _ttl_to_ms(legacy_records_ttl)`` (wallclock-
              at-rebuild, §5), writing the ``__ttl_index__`` entry, producing a
              header-bearing stamped record to the changelog, and deleting the key
              from the pending CF as the chunk commits (the delete IS the durable
              progress cursor — §6).

        Un-gated by the live flip flag (the partition is already flipped). Idempotent
        and convergent across interrupts: an interrupted run leaves the still-pending
        keys in the CF; the next cold restore rebuilds pending from the (now-more-
        stamped) changelog and resumes over exactly the remainder.
        """
        if not (type(self).uses_ttl_stamps and self.uses_ttl_stamps):
            # Never flipped (pure-legacy partition) or a subclass opted out:
            # there is no migration to complete.
            return
        if not self._recovery_saw_stamped:
            # All-legacy first-enablement (§8.6): even if the pending CF holds
            # every key, completion does NOT run — the live first-``ttl=``-write
            # backfill owns this case.
            return

        pending_count = self._count_backfill_pending()
        if pending_count == 0:
            # All-stamped / fully-migrated changelog (§8.7): nothing leftover.
            return

        legacy_records_ttl = self._legacy_records_ttl
        if legacy_records_ttl is None:
            # Config-absent (wrinkle #1, §8): the replay finished cleanly (no data
            # lost / corrupted), but completion needs a duration it does not have.
            # Reject loudly with an operator-callable message — silently landing
            # the leftovers as never-expire is exactly the OP-4 bug being fixed.
            raise self._reject_incomplete_migration_no_ttl(pending_count)

        # Wallclock-at-rebuild expiry (wrinkle #2, §5). ``_recovery_now_ms`` was
        # captured on the first stamped default-CF replay (which is exactly when
        # ``_recovery_saw_stamped`` was set), so it is normally populated here;
        # capture defensively if a stamped record was seen but no non-sentinel
        # stamp ever set it.
        if self._recovery_now_ms is None:
            self._recovery_now_ms = self._now_ms()
            self._high_water_ms = self._recovery_now_ms
        expires_at_ms = self._recovery_now_ms + _ttl_to_ms(legacy_records_ttl)

        logger.info(
            "Recovery: completing interrupted legacy-TTL migration at path=%s; "
            "%d leftover legacy record(s) will be stamped with expiry=%d "
            "(wallclock-at-rebuild + legacy_records_ttl).",
            self._path,
            pending_count,
            expires_at_ms,
        )
        completed = self._complete_pending_backfill(
            expires_at_ms=expires_at_ms,
            chunk_size=self._legacy_backfill_chunk_size,
        )
        logger.info(
            "Recovery: completed legacy-TTL migration at path=%s; stamped %d "
            "leftover record(s); __ttl_backfill_pending__ is now empty.",
            self._path,
            completed,
        )

    def _count_backfill_pending(self) -> int:
        """Count keys currently in the ``__ttl_backfill_pending__`` census CF."""
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        count = 0
        for _ in pending_cf.keys():
            count += 1
        return count

    def _complete_pending_backfill(
        self,
        expires_at_ms: int,
        chunk_size: int,
    ) -> int:
        """
        Chunk-backfill the leftover legacy keys censused in
        ``__ttl_backfill_pending__`` (spec §8.8 §6). Mirrors
        :meth:`backfill_legacy_records` but drives its census from the pending CF
        instead of the full default CF, and uses the pending-CF delete as its
        durable progress cursor (no integer cursor needed — a key leaves pending
        only once it has been stamped + indexed + produced atomically).

        Per chunk (up to ``chunk_size`` pending keys, byte-sorted):

        1. Point-get the key's current default-CF value; wrap it whole with
           ``encode_ttl_value(expires_at_ms, value)``, write the ``__ttl_index__``
           entry, and delete the key from the pending CF — all in one WriteBatch.
        2. Produce the chunk's stamped + header-bearing default-CF records to the
           changelog, then flush so the producer queue stays bounded.
        3. Commit the WriteBatch with the raw writer (no sweep — the high-water is
           seeded to wallclock-at-rebuild and the leftovers expire in the future).

        :return: count of leftover records stamped on this run.
        """
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        default_handle = self.get_column_family_handle("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        pending_handle = self.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)

        headers = None
        if self._changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                # Completion runs at recovery with no triggering live message, so
                # there are no processed offsets to encode (cf. the live backfill).
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                # Completion records are stamped, so they carry the §8.7 bit; a
                # subsequent restore then sees an all-stamped changelog and never
                # re-enters completion (§6).
                CHANGELOG_TTL_STAMPED_HEADER: b"\x01",
            }

        # Total leftover count for the DEBUG progress denominator (logging only).
        # The pending CF shrinks as the loop deletes stamped keys, so capture it
        # once up front — but only when DEBUG is on, so the census scan never
        # runs at INFO. ``stamped_count`` is the cumulative numerator.
        total_pending = (
            self._count_backfill_pending() if logger.isEnabledFor(logging.DEBUG) else 0
        )

        stamped_count = 0
        while True:
            # CENSUS one chunk from the pending CF in byte-sorted order. The
            # pending-CF delete (below) is the cursor, so each pass re-reads the
            # current head of the CF — a key reappears only if its chunk did not
            # commit (crash), in which case re-stamping it is harmless (whole-
            # value-once on the raw legacy value, never on an already-stamped one).
            chunk_keys: list[bytes] = []
            for raw_key in pending_cf.keys():
                chunk_keys.append(cast(bytes, raw_key))
                if len(chunk_keys) >= chunk_size:
                    break
            if not chunk_keys:
                break

            batch = WriteBatch(raw_mode=True)
            produce: list[tuple[bytes, bytes]] = []
            for key in chunk_keys:
                raw_value = default_cf.get(key, default=None)
                if raw_value is None:
                    # Censused key whose default-CF entry vanished (tombstoned
                    # since census). Nothing to stamp; just drop the stale
                    # pending entry so the cursor advances.
                    batch.delete(key, pending_handle)
                    continue
                stamped = encode_ttl_value(expires_at_ms, cast(bytes, raw_value))
                batch.put(key, stamped, default_handle)
                batch.put(encode_index_key(expires_at_ms, key), b"", index_handle)
                batch.delete(key, pending_handle)
                produce.append((key, stamped))
                stamped_count += 1

            if self._changelog_producer is not None and produce:
                for key, stamped in produce:
                    self._changelog_producer.produce(
                        key=key, value=stamped, headers=headers
                    )
                self._changelog_producer.flush()

            # COMMIT atomically: default puts + index puts + pending deletes. The
            # pending deletes are the durable cursor — a crash before this commit
            # leaves the chunk's keys in pending and the next pass redoes them.
            self._write(batch)

            # PROGRESS: one DEBUG line per chunk on the OP-4 recovery-completion
            # path, distinct from the live backfill message so the source is
            # clear in logs. Denominator is the initial leftover census; the
            # caller still emits the final "completed … migration" log at INFO.
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Recovery: legacy-TTL migration completion progress: "
                    "%d / %d leftover record(s) stamped path=%s",
                    stamped_count,
                    total_pending,
                    self._path,
                )

            del batch, produce

        return stamped_count

    def _reject_incomplete_migration_no_ttl(
        self, pending_count: int
    ) -> "IncompatibleStateStoreError":
        """
        Build (and log) the operator-callable ERROR raised when a MIXED
        (incomplete-migration) changelog is cold-restored but
        ``legacy_records_ttl`` is absent from config (spec §8). The caller
        ``raise``s the returned exception.

        Distinct from :meth:`reject_ttl_on_populated_store` (enable-on-populated):
        here the migration is already in progress and merely needs the duration to
        finish. The message names ``legacy_records_ttl``, states the leftover
        count, and points at restoring the field + redeploying — NOT at deleting
        state (Quix Cloud has no customer-callable state reset).
        """
        msg = (
            f"IncompatibleStateStoreError: state store at {self._path!r} has an "
            f"incomplete legacy-TTL migration: {pending_count} leftover legacy "
            "record(s) replayed from a MIXED changelog were never stamped "
            "(a previous backfill was interrupted). Completing the migration "
            "requires a uniform expiry for these records, but "
            "legacy_records_ttl is not set. Restore "
            "RocksDBOptions(legacy_records_ttl=timedelta(...)) in config and "
            "redeploy: the partition will stamp the leftover records "
            "(expiry = wallclock-at-rebuild + legacy_records_ttl) and finish the "
            "migration. Do NOT delete the state directory — the leftover records "
            "are intact and only need the expiry to be stamped."
        )
        logger.error(
            msg,
            extra={
                "state_store_path": self._path,
                "pending_leftover_count": pending_count,
                "operator_action": "restore_legacy_records_ttl",
            },
        )
        return IncompatibleStateStoreError(msg)

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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                'Flushing state changes to the disk path="%s" '
                "changelog_offset=%s bytes_total=%d",
                self.path,
                changelog_offset,
                batch.size_in_bytes(),
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
        Provably-complete backfill (Fix A, spec ``spec-backfill-completeness.md``
        §3): census the full default-CF key list FIRST, then chunk over that
        frozen list, point-getting each value fresh and re-stamping it with a
        uniform ``expires_at_ms`` expiry. Persisting and producing each chunk
        before reading the next bounds peak transient memory to one chunk (plus
        the key-list census) regardless of total store size.

        Called by the transaction layer from ``prepare()`` when a TTL write is
        detected on a partition whose default CF is **populated** *and*
        ``legacy_records_ttl`` is set (the backfill branch of
        ``_maybe_flip_or_reject``). The companion empty-store flip path
        (:meth:`flip_into_ttl_mode`) handles the empty-CF case.

        **Why census-then-chunk (no iterate-while-write).** The earlier design
        held a single live forward iterator over the default CF *while* writing
        re-stamped values back into that same CF. At real scale (200k+ keys,
        SST flushes/compactions triggered mid-iteration) that read-while-write
        pattern can skip or duplicate keys — and a single skipped key flips a
        populated store into TTL mode with an un-stamped value, which the read
        path then mis-strips → corruption (the live incident). Instead we freeze
        the set of keys to stamp up front via a single ``keys()`` scan and drive
        the write loop from that frozen Python list, point-getting each value
        with ``default_cf.get(key)``. The read driver is independent of the CF's
        live structure, so every census key is visited **exactly once**.

        **Deterministic census order + persisted cursor (re-run safety, §3.3).**
        ``default_cf.keys()`` yields keys in RocksDB byte-sorted order; we
        ``sorted(...)`` the materialized list to make the order explicit and
        reproducible across runs. Progress is tracked by an integer cursor ``N``
        persisted under ``__ttl_backfill_progress__`` in the metadata CF, advanced
        in the **same WriteBatch** as each chunk's puts. A crash mid-backfill
        leaves the partition legacy (the flag is written LAST by the caller), so
        a re-run re-censuses — producing the identical sorted list — and resumes
        at key index ``N``: keys ``[0:N)`` are known done (skipped via the cursor,
        **not** byte-sniffed) and keys ``[N:]`` are re-stamped. There is no
        inference and no double-wrap: a resumed key is read fresh and wrapped
        whole exactly once.

        **No format inference.** On the first run the partition is legacy by
        precondition (flag absent), so every value is genuine legacy and is
        wrapped whole with ``encode_ttl_value(expires_at_ms, value)``.
        ``_looks_like_stamped_value`` is **not** used anywhere in this path; it
        survives only for the recovery flag-discovery path.

        Per chunk (up to ``chunk_size`` keys from the frozen census list):

        1. Point-get each key's value fresh; wrap whole into a ``WriteBatch`` of
           default-CF puts + ``__ttl_index__`` puts
           (``encode_index_key(expires_at_ms, key)``). Skip ``staged_default_keys``
           (re-stamped by the caller) and keys deleted since the census
           (``get`` → ``None``).
        2. Produce the chunk's re-stamped default-CF records to the changelog
           (the index CF is local-only and is rebuilt on recovery), then
           ``flush()`` the producer so its in-flight queue stays bounded.
        3. Stage the advanced cursor into the same batch and commit with the raw
           writer ``self._write(batch)`` — NOT ``self.write(...)`` — so no sweep
           runs (the partition is still legacy) and the per-chunk default+index
           puts + cursor commit atomically together.
        4. Drop the chunk's structures before reading the next.

        After the cursor reaches ``len(key_list)`` the caller writes
        ``__ttl_enabled__`` / the format version **last**, so the flip is durable
        only once every census key has landed (§3.4 flag-last ordering).

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
        :return: count of pre-existing records re-stamped on this run (cursor-
            skipped, staged, and deleted-since-census keys are not counted).
        """
        # Pre-create the index CF so the per-chunk batch never races a CF
        # creation.
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        default_handle = self.get_column_family_handle("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)

        headers = None
        if changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(
                    processed_offsets
                ),
                # Backfill records are always stamped (re-stamped legacy values),
                # so they unconditionally carry the §8.7 stamped bit. The base
                # ``_prepare`` cannot set it for these because they are produced
                # directly here, before ``uses_ttl_stamps`` is flipped to True.
                CHANGELOG_TTL_STAMPED_HEADER: b"\x01",
            }

        # Step 0 — CENSUS: materialize the complete, deterministically-ordered
        # key list ONCE, with no concurrent writes to the default CF (backfill
        # is sequential inside prepare(); processing is paused). Keys only —
        # values are point-got fresh per chunk. ``sorted`` makes the resume
        # order explicit and identical across runs so the integer cursor is
        # exact (§3.3). ``staged_default_keys`` are excluded here so they are
        # never censused, never counted in the cursor, and re-stamped only by
        # the caller's ``_restamp_default_cf_cache_for_flip``.
        key_list: list[bytes] = sorted(
            cast(bytes, key)
            for key in default_cf.keys()
            if cast(bytes, key) not in staged_default_keys
        )
        total = len(key_list)
        if total > _CENSUS_SPILL_WARN_THRESHOLD:
            logger.warning(
                "TTL legacy backfill censused %d keys at path=%s; the key list "
                "is held in memory (~80 B/key). For multi-million-key stores a "
                "spill-to-disk census will be needed (OP-BC-2); proceeding "
                "in memory for now.",
                total,
                self._path,
            )

        # STARTED bracket (lifecycle log). One INFO line once the census count
        # is known, before the chunk loop, so an operator sees the migration
        # bracket open even when the app's periodic status logger is off in
        # production. The matching FINISHED line is emitted after the last
        # chunk commits; the per-chunk progress line sits between them and the
        # caller's "Backfilled N … flipped" log confirms the flip.
        logger.info(
            "TTL legacy backfill STARTED: %d records to re-stamp path=%s",
            total,
            self._path,
        )

        # Resume from the persisted cursor (0 on a first run / absent key).
        cursor = self._load_backfill_progress()
        if cursor > total:
            # Defensive: census shrank since the cursor was written (keys
            # deleted between runs). Clamp so the loop is a no-op tail.
            cursor = total

        restamped = 0
        while cursor < total:
            chunk_keys = key_list[cursor : cursor + chunk_size]

            # RE-STAMP this chunk: point-get each value fresh and wrap whole.
            batch = WriteBatch(raw_mode=True)
            produce: list[tuple[bytes, bytes]] = []
            for key in chunk_keys:
                raw_value = default_cf.get(key, default=None)
                if raw_value is None:
                    # Deleted since the census — nothing to stamp, no index
                    # entry to create. Skip cleanly (§5 / §8 edge cases).
                    continue
                stamped = encode_ttl_value(expires_at_ms, cast(bytes, raw_value))
                batch.put(key, stamped, default_handle)
                batch.put(
                    encode_index_key(expires_at_ms, key), b"", index_handle
                )
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

            # ADVANCE the cursor IN THE SAME batch as the chunk's puts so the
            # progress marker and the stamped data commit atomically. A crash
            # after this commit but before the flag leaves the partition legacy
            # with the cursor at this value; the re-run resumes here exactly.
            cursor += len(chunk_keys)
            batch.put(
                TTL_BACKFILL_PROGRESS_KEY,
                int_to_bytes(cursor),
                metadata_handle,
            )

            # COMMIT this chunk atomically (default + index puts + cursor).
            self._write(batch)

            # PROGRESS: one DEBUG line per chunk (~chunk_size records, default
            # 10k) so a large/long backfill is observable instead of looking
            # like a hang. Mirrors recovery's ``Recovery progress … X / Total``
            # cadence. ``cursor`` is the cumulative census index reached; the
            # final completion log is still emitted by the caller.
            logger.debug(
                "TTL legacy backfill progress: %d / %d records re-stamped path=%s",
                cursor,
                total,
                self._path,
            )

            # RELEASE: drop the chunk's structures before the next iteration.
            del batch, produce

        # FINISHED bracket (lifecycle log). Closes the STARTED line above after
        # the last chunk has committed and before returning to the caller. The
        # caller's "Backfilled N … flipped (legacy_records_ttl)" log is the
        # separate flip confirmation; this line brackets the backfill loop.
        logger.info(
            "TTL legacy backfill FINISHED: %d records re-stamped path=%s",
            restamped,
            self._path,
        )

        return restamped

    def _load_backfill_progress(self) -> int:
        """
        Read the persisted backfill cursor ``__ttl_backfill_progress__`` from
        the metadata CF. Absent / undecodable = ``0`` (first run / no progress
        yet). The cursor is the number of census keys already stamped (Fix A,
        spec §3.3).
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(TTL_BACKFILL_PROGRESS_KEY, default=None)
        if raw is None:
            return 0
        try:
            return int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning(
                "Failed to decode TTL backfill progress cursor at %s; "
                "restarting the backfill from the beginning.",
                self._path,
            )
            return 0

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
        Value-content recognizer for a stamped default-CF value.

        NO LONGER ON THE RECOVERY PATH (spec §8.7). Recovery flip-discovery now
        routes purely on the out-of-band ``__ttl_stamped__`` changelog header
        (see ``recover_from_changelog_message``), because this content heuristic
        false-positives on legacy 8-byte epoch-ms values (OP-3). Retained because
        ``test_backfill_completeness`` spies on it to assert the backfill never
        byte-sniffs; it has no remaining live production caller.

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
