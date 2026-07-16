import functools
import logging
import os
import time
from datetime import timedelta
from typing import Any, Dict, Literal, Optional, Union, cast

from quixstreams.models import HeadersMapping
from quixstreams.state import PartitionTransaction
from quixstreams.state.base import PartitionTransactionCache, StorePartition
from quixstreams.state.base.transaction import (
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.exceptions import ChangelogFlushError, StateSerializationError
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    LOCAL_ONLY_CFS,
    METADATA_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_INDEX_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
    Marker,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.exceptions import IncompatibleStateStoreError
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ADOPT_PENDING_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_ROLLBACK_ENV_VAR,
)
from quixstreams.state.rocksdb.transaction import _safe_decode_stamp, _ttl_to_ms
from quixstreams.state.rocksdb.ttl_codec import (
    _MAX_PLAUSIBLE_STAMP_MS,
    SENTINEL_NEVER,
    TTL_STAMP_BYTES,
    clamp_additive_expiry,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)
from quixstreams.state.serialization import int_from_bytes, int_to_bytes
from quixstreams.utils.json import dumps as json_dumps
from quixstreams.utils.json import loads as json_loads

logger = logging.getLogger(__name__)

__all__ = ("MemoryStorePartition",)

_DEFAULT_MAX_EVICTIONS_PER_FLUSH = 10_000

# Bounded flush timeout (seconds) for the done-marker changelog site, matching
# ``RocksDBStorePartition._BACKFILL_CHANGELOG_FLUSH_TIMEOUT_S`` (below the 30 s
# producer poll interval): confirm the marker on the changelog before recording
# it locally, or raise instead of hanging indefinitely.
_MIGRATION_MARKER_FLUSH_TIMEOUT_S: float = 25.0


def _validate_partition_state():
    """
    Check that the store is not closed
    """

    def wrapper(func):
        @functools.wraps(func)
        def _wrapper(partition: "MemoryStorePartition", *args, **kwargs):
            if partition.closed:
                raise RuntimeError("partition is closed")

            return func(partition, *args, **kwargs)

        return _wrapper

    return wrapper


class MemoryStorePartition(StorePartition):
    """
    Class to access in-memory state.

    Responsibilities:
     1. Recovering from changelog messages
     2. Creating transaction to interact with data
     3. Track partition state in-memory
     4. Maintaining the same per-write TTL machinery as
        :class:`quixstreams.state.rocksdb.RocksDBStorePartition` so dev/test
        workflows that switch between MemoryStore and RocksDBStore see
        identical semantics.
    """

    # Class-level switch (subclasses can hard-disable). The **runtime**
    # flag is initialized to False in ``__init__`` to mirror the
    # v3.23.6-compatible default; the partition flips itself into TTL mode on
    # first detection of a ``state.set(..., ttl=...)`` write at flush time.
    uses_ttl_stamps: bool = True

    def __init__(
        self,
        changelog_producer: Optional[ChangelogProducer],
        max_evictions_per_flush: int = _DEFAULT_MAX_EVICTIONS_PER_FLUSH,
        legacy_records_ttl: Optional[timedelta] = None,
        ttl_changelog_tombstones: bool = True,
    ) -> None:
        super().__init__(
            dumps=json_dumps,
            loads=json_loads,
            changelog_producer=changelog_producer,
        )
        self._changelog_offset: Optional[int] = None
        # Parity with ``RocksDBOptions.ttl_changelog_tombstones`` (default ON):
        # when True, TTL evictions are staged into the transaction cache at
        # prepare-time so they are produced to the changelog as tombstones and the
        # changelog shrinks under compaction; when False, the eviction stays
        # local-only (the pre-change ``_run_sweep``-in-``write()`` path). Memory
        # does not consume ``RocksDBOptions``, so it is a direct constructor arg.
        self._ttl_changelog_tombstones: bool = ttl_changelog_tombstones
        # §5.6 operational rollback lever, read ONCE at open from the environment
        # (mirrors ``RocksDBStorePartition._ttl_rollback``). Memory is always the
        # COLD regime (§5.7); when set it SUPPRESSES the cold provisional adopt on
        # the next full changelog replay (a fresh replay reconstructs the un-adopted
        # originals, so no explicit restore is needed).
        self._ttl_rollback: bool = os.environ.get(TTL_ROLLBACK_ENV_VAR) == "1"
        # Runtime mirror of the COLD-heuristic provisional-adoption state (§5.2,
        # parity with ``RocksDBStorePartition._adopt_provisional``). True == this
        # partition was provisionally cold-adopted and is not yet corroborated:
        # the TTL sweep is suppressed (§5.3) until a live ``ttl=`` write
        # corroborates (§5.4). Always a runtime-only flag on the non-persistent
        # memory backend. No in-RAM value backup is kept — memory rollback (§5.7)
        # is replay-suppression (the next full changelog replay reconstructs the
        # un-adopted originals), not an in-lifetime restore, so a backup copy would
        # be dead state.
        self._adopt_provisional: bool = False
        self._state: Dict[str, Dict[bytes, Any]] = {
            "default": {},
            METADATA_CF_NAME: {},
        }
        self._closed = False
        self._max_evictions_per_flush = max_evictions_per_flush
        # Optional uniform expiry for leftover legacy records completed during a
        # MIXED-changelog recovery (parity with
        # ``RocksDBOptions.legacy_records_ttl``). ``None`` = auto-finish at the
        # survivor-derived default; a positive ``timedelta`` = complete at
        # ``wallclock_now + legacy_records_ttl``. MemoryStore has no persistent
        # options surface, so this is set only via direct construction.
        self._legacy_records_ttl: Optional[timedelta] = legacy_records_ttl
        self._high_water_ms: Optional[int] = None
        # Wallclock reference captured once per changelog-recovery session
        # (lazily, on the first stamped default-CF replay). Mirrors
        # ``RocksDBStorePartition._recovery_now_ms``: used to judge whether a
        # replayed TTL entry is already expired and to seed the post-recovery
        # high-water (see :meth:`recover_from_changelog_message`). ``None`` means
        # no stamped message has been replayed yet in this partition's lifetime.
        self._recovery_now_ms: Optional[int] = None
        # Count of wallclock-expired stamped records dropped during this
        # recovery's changelog replay by the latest-record-wins recovery-drop
        # filter. Surfaced as
        # one aggregate INFO at :meth:`complete_recovery` so an operator sees the
        # deletions instead of records silently vanishing. Mirrors
        # ``RocksDBStorePartition._recovery_expired_drops``.
        self._recovery_expired_drops: int = 0
        # Incomplete-migration detection, mirroring
        # ``RocksDBStorePartition``. Set True on the first header-true default-CF
        # replay; combined with a non-empty ``__ttl_backfill_pending__`` census at
        # end of recovery it marks a MIXED changelog whose leftover legacy records
        # :meth:`complete_recovery` must stamp. False (all-legacy replay) never
        # triggers completion.
        self._recovery_saw_stamped: bool = False
        # Survivor-derived completion default: max absolute expiry among
        # replayed default-CF records that are stamped (header-true), non-SENTINEL,
        # and NOT dropped by the latest-record-wins recovery-drop filter. Used to
        # complete an
        # incomplete migration when ``legacy_records_ttl`` is absent; ``None`` (no
        # surviving future stamp) falls back to SENTINEL_NEVER + a WARN.
        self._recovery_max_survivor_expiry_ms: Optional[int] = None
        # Durable done-flag latch, mirroring
        # ``RocksDBStorePartition``. Set True when the replicated
        # ``__ttl_system__`` marker is replayed; makes :meth:`complete_recovery`
        # flip + discard pending + never re-run completion.
        self._recovery_saw_migration_done: bool = False
        # Warn-once guard for the fail-safe degraded TTL read,
        # scoped to the partition so it fires once per partition lifetime rather
        # than once per checkpoint transaction.
        self._unstamped_read_warned: bool = False

        # #5 (review batch 3): per-partition delivery accounting for the memory
        # migration produce sites (live populated-legacy backfill, recovery-
        # completion re-stamps, done-marker). Mirrors ``RocksDBStorePartition``:
        # the done-marker flush confirmation compares THIS partition's
        # ``produced - acked`` rather than the shared producer's GLOBAL flush
        # count, so a sibling partition's wedged records cannot falsely raise on
        # this partition's marker.
        self._backfill_produced: int = 0
        self._backfill_acked: int = 0

        # #9 (review batch 3): parity with ``RocksDBStorePartition``. A memory
        # partition is non-persistent (fresh ``_state`` every open), so this is
        # always False at construction — the ``_recovery_saw_migration_done``
        # replay latch is what stops the census once the marker replays (the marker
        # is produced last, so it arrives after every record it certifies).
        # Kept for structural symmetry and to read uniformly in the census gate.
        self._migration_done_at_open: bool = bool(
            self._state.get(TTL_SYSTEM_CF_NAME, {}).get(TTL_MIGRATION_DONE_KEY)
        )

        class_uses_ttl_stamps = type(self).uses_ttl_stamps
        if class_uses_ttl_stamps:
            # Memory store has no persistent on-disk metadata (each
            # partition starts fresh), but recovery can flip it later.
            self.uses_ttl_stamps = False
        else:
            self.uses_ttl_stamps = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def high_water_ms(self) -> Optional[int]:
        return self._high_water_ms

    @property
    def max_evictions_per_flush(self) -> int:
        return self._max_evictions_per_flush

    @property
    def legacy_records_ttl(self) -> Optional[timedelta]:
        """
        Optional uniform expiry for leftover legacy records completed during a
        MIXED-changelog recovery. Mirrors
        ``RocksDBStorePartition.legacy_records_ttl``. ``None`` = auto-finish at
        the survivor-derived default.
        """
        return self._legacy_records_ttl

    @property
    def ttl_changelog_tombstones(self) -> bool:
        """
        Whether TTL evictions are produced to the changelog as tombstones
        (default ``True``) or kept local-only (``False``). Parity with
        ``RocksDBStorePartition.ttl_changelog_tombstones``.
        """
        return self._ttl_changelog_tombstones

    def advance_high_water(self, timestamp: Optional[int]) -> None:
        if timestamp is None:
            return
        if timestamp < 0:
            # #12 (review batch 3): a negative timestamp (Kafka NO_TIMESTAMP = -1
            # or pre-epoch event-time) never represents a real position, so it must
            # not advance / establish the high-water — a negative high-water then
            # fails the unsigned int_to_bytes persist with a raw struct.error.
            # Mirrors ``RocksDBStorePartition.advance_high_water``.
            return
        if self._high_water_ms is None or timestamp > self._high_water_ms:
            self._high_water_ms = timestamp

    def _now_ms(self) -> int:
        """
        Current wallclock time in epoch milliseconds. Isolated behind a method
        purely as a test seam so changelog-recovery determinism cases can
        inject a fixed ``now`` without sleeping. Mirrors
        ``RocksDBStorePartition._now_ms``.
        """
        return int(time.time() * 1000)

    def _on_backfill_delivery(
        self, err: Optional[object], msg: Optional[object] = None
    ) -> None:
        """Chained delivery callback for the memory migration produce sites (#5).
        Counts a successful delivery so the done-marker flush confirmation can
        measure THIS partition's own outstanding records instead of the shared
        producer's global queue depth. Mirrors
        ``RocksDBStorePartition._on_backfill_delivery``."""
        if err is None:
            self._backfill_acked += 1

    def close(self) -> None:
        self._closed = True

    def begin(self) -> PartitionTransaction:
        return MemoryPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    @_validate_partition_state()
    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
    ) -> None:
        """
        Write data to the state

        :param cache: The partition update cache
        :param changelog_offset: The changelog message offset of the data.
        """
        if changelog_offset is not None:
            self._changelog_offset = changelog_offset

        for cf_name in cache.get_column_families():
            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    self._state.setdefault(cf_name, {})[key] = value

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                self._state.setdefault(cf_name, {}).pop(key, None)

        if self.uses_ttl_stamps:
            if self._high_water_ms is not None:
                self._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = int_to_bytes(
                    self._high_water_ms
                )
            self._state.setdefault(TTL_INDEX_CF_NAME, {})
            if not self._ttl_changelog_tombstones:
                # OFF path only: local-only sweep. On the ON path (default),
                # eviction + index GC were staged into the cache at prepare-time
                # (sweep_expired_into_cache) and already applied by the cache walk
                # above — main-CF evictions also produced as changelog tombstones.
                self._run_sweep()

    # ------------------------------------------------------------------
    # TTL flip / probe helpers (mirror RocksDBStorePartition).
    # ------------------------------------------------------------------

    def main_cf_has_user_data(self) -> bool:
        return bool(self._state.get("default"))

    def has_incomplete_ttl_migration(self) -> bool:
        """
        Memory mirror of ``RocksDBStorePartition.has_incomplete_ttl_migration``.
        Always ``False``: the memory backend has no persistent on-disk
        flip flag or durable pending census — every open re-recovers from the
        changelog via a FULL replay (its stored offset is always ``None`` →
        ``OFFSET_BEGINNING``), so the offset-caught-up crash window that this
        detects on RocksDB cannot arise. A MIXED changelog is therefore always
        completed by the normal replay-driven :meth:`complete_recovery` path.
        """
        return False

    def recover_from_changelog_message(
        self,
        key: bytes,
        value: Optional[bytes],
        cf_name: str,
        offset: int,
        ttl_stamped: bool = False,
    ) -> None:
        # Done-flag consumption, mirroring
        # ``RocksDBStorePartition``. The replicated ``__ttl_system__`` marker
        # means the source store completed its migration; latch it so
        # :meth:`complete_recovery` flips, discards any pending census, and never
        # re-runs completion. The record still lands in the local ``__ttl_system__``
        # dict below. Marker absent leaves the latch False (existing logic).
        if (
            type(self).uses_ttl_stamps
            and cf_name == TTL_SYSTEM_CF_NAME
            and key == TTL_MIGRATION_DONE_KEY
        ):
            self._recovery_saw_migration_done = True

        # Recovery flip-discovery (mirrors
        # ``RocksDBStorePartition.recover_from_changelog_message``).
        #
        # Every stamped ``default``-CF record produced while the source partition
        # was in TTL mode carries the out-of-band ``__ttl_stamped__`` changelog
        # header (set in the base ``_prepare``), surfaced here as ``ttl_stamped``.
        # On the first header-true default-CF record we flip this recovery
        # partition into TTL mode and latch for the rest of the session. This
        # REPLACES the old value-content heuristic (``_looks_like_stamped_value``),
        # which false-positived on legacy 8-byte epoch-ms values and dropped them.
        # Header absent → the record is legacy / un-stamped and
        # replays verbatim below, so a purely legacy changelog never latches. The
        # flip is session-local: memory
        # partitions are non-persistent and re-recover from the changelog on every
        # open, so there is no ``_stamp_flip_metadata`` mirror.
        if (
            type(self).uses_ttl_stamps
            and not self.uses_ttl_stamps
            and cf_name == "default"
            and ttl_stamped
        ):
            logger.info(
                "Recovery: __ttl_stamped__ header on default-CF replay; flipping "
                "in-memory partition into TTL mode for the rest of recovery."
            )
            self.uses_ttl_stamps = True
            self._state.setdefault(TTL_INDEX_CF_NAME, {})

        # Incomplete-migration detection / census, mirroring
        # ``RocksDBStorePartition``. Gated on the CLASS-level flag (not the
        # instance flag): a MIXED changelog replays header-absent legacy records
        # BEFORE the first stamped record flips the partition, so they must be
        # censused into ``__ttl_backfill_pending__`` now (and land verbatim below).
        # A header-true replay sets the MIXED-detection latch and supersedes any
        # earlier legacy census entry for the same key (compaction ordering).
        if (
            type(self).uses_ttl_stamps
            and cf_name == "default"
            and not (self._migration_done_at_open or self._recovery_saw_migration_done)
        ):
            # #9 (parity with RocksDB): stop censusing once the migration
            # done-marker is known (latched when the marker replays — it is produced
            # last, so it arrives after every record it certifies).
            # ``complete_recovery`` discards / no-ops the census in that case, so
            # skipping the writes is safe; the no-marker else-branch is unchanged.
            pending = self._state.setdefault(TTL_BACKFILL_PENDING_CF_NAME, {})
            if ttl_stamped:
                self._recovery_saw_stamped = True
                pending.pop(key, None)
            elif value is not None:
                pending[key] = b""

        if not self.uses_ttl_stamps:
            # Legacy / non-TTL partitions: replay the raw payload verbatim. Key
            # off ``is None`` (parity with ``RocksDBStorePartition``): only a
            # genuine tombstone deletes; a legitimate empty-bytes value (``b""``)
            # is preserved instead of being dropped as a false tombstone.
            if value is None:
                self._state.setdefault(cf_name, {}).pop(key, None)
            else:
                self._state.setdefault(cf_name, {})[key] = value
            self._changelog_offset = offset
            return

        if cf_name in LOCAL_ONLY_CFS:
            self._changelog_offset = offset
            return

        is_main_cf = cf_name == "default"

        if value is None:
            self._state.setdefault(cf_name, {}).pop(key, None)
        elif is_main_cf and not ttl_stamped:
            # A header-absent (legacy) default-CF record replayed on
            # a (now-)flipped partition — a leftover of an interrupted migration
            # (MIXED changelog). It MUST land VERBATIM (raw bytes), NOT through
            # ``_normalize_replay_value`` (which would SENTINEL-wrap it, corrupting
            # the payload the first 8 bytes are later stripped as a fake stamp).
            # Its key was just censused into the pending map and
            # :meth:`complete_recovery` will stamp it once at end of recovery.
            # Mirrors the RocksDB ``is_main_cf and not ttl_stamped`` verbatim branch.
            self._state.setdefault(cf_name, {})[key] = value
        elif is_main_cf:
            stamped, stamp = self._normalize_replay_value(value)
            # Judge expiry against the current wallclock captured once per
            # recovery session, NOT against a stamp-ratcheted pseudo-clock (the
            # old ``recovery_now = self._high_water_ms`` ratchet, advanced by
            # each entry's stamp, collapsed uniform-expiry backfilled stores).
            # Capture lazily on the first
            # stamped default-CF replay. The wallclock is the
            # replay drop clock ONLY; it is NO LONGER seeded into the live
            # ``_high_water_ms`` clock (removed for parity with RocksDB, so an
            # event-time-lagging workload never over-expires its own writes).
            # Sentinel-stamped entries are never compared and always survive.
            expired = False
            if stamp != SENTINEL_NEVER:
                if self._recovery_now_ms is None:
                    self._recovery_now_ms = self._now_ms()
                recovery_now_ms = self._recovery_now_ms
                expired = stamp <= recovery_now_ms
            if expired:
                # Already-expired against wallclock-at-recovery
                # (latest-record-wins, parity with RocksDB). An OLDER copy of
                # ``key`` replayed earlier this session (verbatim legacy, or an
                # older unexpired stamped copy) may already sit in the main dict;
                # skipping (the old bare ``pass``) let it survive as a
                # never-expiring leftover the completion pass can no longer repair
                # (its pending census entry was just popped above). Explicitly
                # DELETE so this newest (expired) copy supersedes any older
                # survivor; the index write is still skipped and any stale index
                # pointer is GC'd by the sweep's ghost handling (main is gone).
                self._state.setdefault(cf_name, {}).pop(key, None)
                self._recovery_expired_drops += 1
            else:
                self._state.setdefault(cf_name, {})[key] = stamped
                if stamp != SENTINEL_NEVER:
                    self._state.setdefault(TTL_INDEX_CF_NAME, {})[
                        encode_index_key(stamp, key)
                    ] = b""
                    # Surviving future non-sentinel stamp on a header-true
                    # record — track the max as the config-absent completion
                    # source (mirrors ``RocksDBStorePartition``).
                    if (
                        self._recovery_max_survivor_expiry_ms is None
                        or stamp > self._recovery_max_survivor_expiry_ms
                    ):
                        self._recovery_max_survivor_expiry_ms = stamp
        else:
            self._state.setdefault(cf_name, {})[key] = value

        self._changelog_offset = offset

    def complete_recovery(self) -> None:
        """
        Recovery-finalize hook, mirroring
        :meth:`RocksDBStorePartition.complete_recovery` at the semantics level
        (in-RAM dicts instead of CFs). There is **no reject branch** — the memory
        backend never had one, and the migration is auto-finished.

        Completes an interrupted legacy-TTL migration replayed from a MIXED
        changelog: leftover legacy records landed VERBATIM during replay and were
        censused into ``__ttl_backfill_pending__``; here they are stamped with a
        uniform expiry, indexed, produced as header-true stamped records (so a
        subsequent restore sees an all-stamped changelog and never re-enters
        completion), and drained from the census.

        - NOT ``_recovery_saw_stamped`` → all-legacy replay: discard the orphan
          pending census and no-op (hygiene parity).
        - pending empty → all-stamped / fully migrated: no-op.
        - else (MIXED) → stamp exactly the pending leftovers with:
            - ``legacy_records_ttl`` set → ``wallclock_now + legacy_records_ttl``
              (parity; explicit config wins);
            - absent → the survivor-derived expiry, or ``SENTINEL_NEVER``
              (never-expire) + a WARN in the all-expired fallback.
        """
        if self._recovery_expired_drops > 0:
            # Make the wallclock-expired replay drops
            # observable — ONE aggregate INFO per partition per recovery (no
            # per-record logging). Emitted at the once-per-recovery finalize seam
            # before any early return; a non-zero count implies the partition
            # flipped, so no early-return branch can skip a non-zero log.
            logger.info(
                "Recovery dropped %d already-expired stamped record(s) from the "
                "in-memory changelog replay (expired against recovery "
                "wallclock=%d ms; latest-record-wins).",
                self._recovery_expired_drops,
                self._recovery_now_ms or 0,
            )
        if not type(self).uses_ttl_stamps:
            return
        if self._recovery_saw_migration_done:
            # Durable done-flag present (parity with RocksDB): the
            # migration is complete and is never redone. Ensure flipped, discard
            # any pending census, run no completion.
            if not self.uses_ttl_stamps:
                self.uses_ttl_stamps = True
                self._state.setdefault(TTL_INDEX_CF_NAME, {})
            # §5.4 done-marker index rebuild (parity with RocksDB): a cold restore
            # of a CORROBORATED cold-adopted memory store replays the header-absent
            # adopted records into the census with NO index entry; rebuild the
            # in-RAM __ttl_index__ from the still-censused all-stamped records
            # before discarding, else the adopted records are unindexed and never
            # expire. Self-distinguishing: a completed legacy->TTL migration drains
            # its census to empty on replay, so the gate is False there (and a
            # partial / non-stamp census is likewise skipped), leaving that path
            # byte-identical.
            if self._all_pending_values_are_stamped():
                self._rebuild_index_from_stamped_census()
            self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)
            return
        # Fix 4 (review re-review #4, parity with RocksDB): discriminate an
        # interrupted THIS-BRANCH completion from opt-in v3.24.0 adoption. Memory is
        # always fresh (no persisted flip flag, no ledger), so
        # ``_recovery_saw_stamped`` alone is the this-branch discriminator.
        if not self._recovery_saw_stamped:
            # BRANCH B — no this-branch evidence: unflipped, pure-legacy OR a stock
            # v3.24.0 cold restore. Auto-adoption is now AUTOMATIC + REVERSIBLE
            # (spec §5.2 / §5.7), so a 100%-stamped not-all-past census is
            # provisionally adopted instead of logging a CRITICAL and staying legacy.
            if self._all_pending_values_are_stamped():
                if self._ttl_rollback:
                    # §5.6 Case B: the rollback lever suppresses the cold provisional
                    # adopt — stay legacy, quarantine the census (byte-identical).
                    logger.warning(
                        "Recovery: cold v3.24.0 auto-adopt SUPPRESSED by "
                        "QUIXSTREAMS_STATE_TTL_ROLLBACK=1; the in-memory store stays "
                        "legacy, every value reads back byte-identical, and the %d "
                        "censused key(s) are quarantined (unset the env var and "
                        "restart to re-enable auto-adopt).",
                        len(self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})),
                    )
                    return
                if self._pending_all_stamps_in_past():
                    # QUARANTINE (downgraded from CRITICAL to WARN): every censused
                    # stamp is already in the past — the legacy set_bytes() dedup
                    # shape. Adopting would rebuild the index with past stamps and
                    # the next sweep would DELETE everything, so refuse; stay legacy,
                    # preserve the census, read back byte-identical.
                    logger.warning(
                        "Refused auto-adopt: all %d censused in-memory stamp(s) are "
                        "already in the past (legacy dedup shape); the store stays "
                        "legacy, byte-identical, and the census is preserved "
                        "(quarantined).",
                        len(self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})),
                    )
                    return
                # Not-all-past, 100%-stamped: provisional (reversible) auto-adopt.
                self._adopt_v3240_stamps()
                return
            # Sub-100% "looks-like": genuine legacy — discard the orphan census so a
            # pure-legacy store carries no lasting overhead.
            self._warn_if_looks_like_v3240_upgrade()
            self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)
            return

        # BRANCH A — this-branch evidence present (saw_stamped): completion.
        if not self.uses_ttl_stamps:
            return

        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        if not pending:
            # Fully-migrated MIXED changelog drained to empty: record the done-
            # marker best-effort (Fix 6 part 2 parity). A failed flush must NOT fail
            # recovery — nothing was stamped this session.
            try:
                self._produce_migration_done_marker()
            except ChangelogFlushError:
                logger.warning(
                    "Recovery: in-memory state fully migrated (empty pending "
                    "census) but the done-marker changelog flush failed; leaving "
                    "the store unmarked. Recovery continues; the marker will be "
                    "retried on the next restart."
                )
            return

        if (
            self._all_pending_values_are_stamped()
            and not self._pending_all_stamps_in_past()
        ):
            # Future-stamped ambiguous census on a flipped store: genuine legacy to
            # wrap once OR already-stamped v3.24.0 that wrapping would DOUBLE-WRAP.
            if self._legacy_records_ttl is None:
                # §5.2 Branch-A reconciliation: keep the values VERBATIM via the
                # reversible provisional adopt instead of HALTing. ``legacy_records_ttl``
                # remains the explicit wrap-once override (fall-through below).
                if self._ttl_rollback:
                    logger.warning(
                        "Recovery: ambiguous flipped all-stamped in-memory census "
                        "auto-adopt SUPPRESSED by QUIXSTREAMS_STATE_TTL_ROLLBACK=1; "
                        "the %d leftover value(s) are kept verbatim and the census "
                        "is quarantined. Set legacy_records_ttl to complete as a "
                        "legacy migration, or unset the env var to keep the v3.24.0 "
                        "stamps.",
                        len(pending),
                    )
                    return
                self._adopt_v3240_stamps()
                return
            # else legacy_records_ttl set -> fall through to completion below.

        legacy_records_ttl = self._legacy_records_ttl
        if legacy_records_ttl is not None:
            # Explicit config wins (parity): wallclock-at-rebuild + ttl.
            # The wallclock derives this completion expiry only; it is NOT
            # seeded into the live ``_high_water_ms`` clock (removed for parity).
            if self._recovery_now_ms is None:
                self._recovery_now_ms = self._now_ms()
            expires_at_ms = self._recovery_now_ms + _ttl_to_ms(legacy_records_ttl)
            logger.info(
                "Recovery: completing interrupted legacy-TTL migration in "
                "memory; %d leftover legacy record(s) stamped with expiry=%d "
                "(wallclock-at-rebuild + legacy_records_ttl).",
                len(pending),
                expires_at_ms,
            )
        else:
            # Config absent: derive from the surviving stamped cohort, CLAMPED
            # against the recovery clock (Fix 5, parity with RocksDB). The
            # ``_max_index_stamp_ms()`` fallback is structurally unreachable on the
            # always-full-replay memory backend, but the clamp is implemented for
            # parity and defence: over-clamping to SENTINEL_NEVER only over-keeps
            # (safe), never deletes, and matches the replay drop clock so warm and
            # cold restores of one changelog converge.
            now = (
                self._recovery_now_ms
                if self._recovery_now_ms is not None
                else self._now_ms()
            )
            survivor_expiry = (
                self._recovery_max_survivor_expiry_ms
                if self._recovery_max_survivor_expiry_ms is not None
                else self._max_index_stamp_ms()
            )
            if survivor_expiry is not None and survivor_expiry > now:
                expires_at_ms = survivor_expiry
                logger.warning(
                    "Recovery: completing interrupted legacy-TTL migration in "
                    "memory WITHOUT legacy_records_ttl; %d leftover legacy "
                    "record(s) stamped with expiry=%d, derived from the max "
                    "surviving future stamp of their backfill cohort.",
                    len(pending),
                    expires_at_ms,
                )
            else:
                expires_at_ms = SENTINEL_NEVER
                logger.warning(
                    "Recovery: completing interrupted legacy-TTL migration in "
                    "memory WITHOUT legacy_records_ttl; the only available expiry "
                    "to derive from is already in the past relative to the "
                    "recovery clock (%d), so stamping the %d leftover legacy "
                    "record(s) with it would delete them on the next sweep. "
                    "Retaining them as never-expiring (SENTINEL_NEVER) to honor "
                    "the never-mass-delete guarantee.",
                    now,
                    len(pending),
                )

        default = self._state.setdefault("default", {})
        index = self._state.setdefault(TTL_INDEX_CF_NAME, {})
        headers: Optional[HeadersMapping] = None
        if self._changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                CHANGELOG_TTL_STAMPED_HEADER: b"\x01",
            }

        produced = False
        for key in list(pending.keys()):
            raw_value = default.get(key)
            if raw_value is None:
                # Censused key whose default entry vanished since census — drop
                # the stale pending entry, nothing to stamp.
                pending.pop(key, None)
                continue
            stamped = encode_ttl_value(expires_at_ms, cast(bytes, raw_value))
            default[key] = stamped
            # Sentinel-stamped (never-expire) records skip the expiry index.
            if expires_at_ms != SENTINEL_NEVER:
                index[encode_index_key(expires_at_ms, key)] = b""
            if self._changelog_producer is not None:
                # Recovery-completion runs with no open checkpoint
                # transaction, so use the non-transactional migration route under
                # exactly-once (parity with RocksDB).
                self._changelog_producer.produce(
                    key=key,
                    value=stamped,
                    headers=headers,
                    migration=True,
                    # #5: per-partition delivery accounting.
                    on_delivery=self._on_backfill_delivery,
                )
                self._backfill_produced += 1
                produced = True
            pending.pop(key, None)

        if produced and self._changelog_producer is not None:
            self._changelog_producer.flush(migration=True)

        # Drain the (now-empty) census.
        self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)

        # Flag-last: the migration is complete, so record the
        # durable done-flag marker AFTER the last stamped record.
        self._produce_migration_done_marker()

    def _confirm_migration_delivery_or_raise(
        self,
        changelog_producer: Optional[ChangelogProducer],
        context: str,
    ) -> None:
        """
        Single bounded migration flush + per-partition confirm-or-raise, shared by
        the live in-RAM backfill and the done-marker. Memory has no chunking, so a
        single trailing flush suffices; RocksDB uses a progress-sliced
        ``_flush_backfill_changelog`` loop for its chunked on-disk path. Both
        backends confirm delivery BEFORE the local commit so the local store never
        gets ahead of the changelog.

        Fail loudly only on a concrete POSITIVE backlog; a non-int return (an
        unconfigured test double) is treated as indeterminate and does NOT block. A
        0 backlog means the shared producer's queue is drained, so no raise. #5
        (review batch 3): on a positive backlog, base the decision on THIS
        partition's own outstanding (``produced - acked``, driven by the flush
        above) rather than the shared producer's GLOBAL count, so a sibling
        partition's wedged records never falsely raise here; fall back to the int
        return only when this partition produced nothing through the
        counter-tracked route. Raises :class:`ChangelogFlushError` on a positive
        per-partition outstanding (the caller then retries).
        """
        if changelog_producer is None:
            return
        unproduced = changelog_producer.flush(
            timeout=_MIGRATION_MARKER_FLUSH_TIMEOUT_S, migration=True
        )
        if isinstance(unproduced, int) and unproduced > 0:
            if self._backfill_produced > 0:
                outstanding = self._backfill_produced - self._backfill_acked
            else:
                outstanding = unproduced
            if outstanding > 0:
                raise ChangelogFlushError(
                    f"{outstanding} {context} still undelivered after "
                    f"{_MIGRATION_MARKER_FLUSH_TIMEOUT_S}s; not finalizing the "
                    "migration locally so the local store never gets ahead of the "
                    "changelog (the next attempt retries)."
                )

    def _produce_migration_done_marker(self) -> None:
        """
        Record + produce the durable "migration done" marker,
        mirroring ``RocksDBStorePartition._produce_migration_done_marker`` with
        the same **changelog-first ordering**: produce the marker and confirm its
        delivery with a bounded flush BEFORE recording it in the in-RAM
        ``__ttl_system__`` dict, so the local state never marks "done" ahead of
        the changelog. A failed / timed-out flush raises
        :class:`ChangelogFlushError` (the marker is not recorded, so the next
        completion retries); the changelog-carried marker is what a cold rebuild
        learns "done, never redo" from (the in-RAM dict does not survive a
        restart).
        """
        marker_value = int_to_bytes(STATE_FORMAT_VERSION)
        if self._changelog_producer is not None:
            self._changelog_producer.produce(
                key=TTL_MIGRATION_DONE_KEY,
                value=marker_value,
                headers={
                    CHANGELOG_CF_MESSAGE_HEADER: TTL_SYSTEM_CF_NAME,
                    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                },
                # Non-transactional migration route (parity with RocksDB).
                migration=True,
                # #5: count this record against THIS partition's outstanding.
                on_delivery=self._on_backfill_delivery,
            )
            self._backfill_produced += 1
            self._confirm_migration_delivery_or_raise(
                self._changelog_producer,
                "__ttl_migration_done__ marker record(s)",
            )
        self._state.setdefault(TTL_SYSTEM_CF_NAME, {})[TTL_MIGRATION_DONE_KEY] = (
            marker_value
        )

    def _warn_if_looks_like_v3240_upgrade(self, sample_size: int = 256) -> None:
        """
        Memory mirror of
        ``RocksDBStorePartition._warn_if_looks_like_v3240_upgrade``.
        Sample the censused values; if most decode as plausible 8-byte stamps,
        WARN that this LOOKS like a stock-v3.24.0 cold restore. DETECTION ONLY:
        the sampled bytes gate the log line, never data.
        """
        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        default = self._state.get("default", {})
        sampled = 0
        plausible = 0
        for key in pending:
            value = default.get(key)
            if value is not None:
                sampled += 1
                if _safe_decode_stamp(cast(bytes, value)) is not None:
                    plausible += 1
            if sampled >= sample_size:
                break
        if sampled and plausible / sampled >= 0.5:
            logger.warning(
                "Recovery replayed a header-absent (legacy) in-memory changelog "
                "whose values mostly decode as 8-byte TTL stamps (%d/%d sampled); "
                "this LOOKS like a stock v3.24.0 cold restore whose values may "
                "read back with a spurious 8-byte prefix. HEURISTIC detection "
                "only: NO automatic repair is applied and no value is modified.",
                plausible,
                sampled,
            )

    def _all_pending_values_are_stamped(self) -> bool:
        """
        Total-quorum strict stamp validation over the in-RAM pending census,
        mirroring
        ``RocksDBStorePartition._all_pending_values_are_stamped``. Require EVERY
        censused value to pass ``_safe_decode_stamp``; short-circuit on the first
        failure. Returns ``False`` on an empty census.
        """
        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        default = self._state.get("default", {})
        saw_any = False
        for key in pending:
            saw_any = True
            value = default.get(key)
            if value is None or _safe_decode_stamp(cast(bytes, value)) is None:
                return False
        return saw_any

    def _pending_all_stamps_in_past(self) -> bool:
        """
        Precondition: :meth:`_all_pending_values_are_stamped` is True. Return True
        iff the MAX plausible stamp across the in-RAM census is ``<= the recovery
        clock`` (every censused stamp is in the past). A ``SENTINEL_NEVER`` stamp
        counts as future. Memory mirror of
        ``RocksDBStorePartition._pending_all_stamps_in_past`` (Fix 4).
        """
        now = (
            self._recovery_now_ms
            if self._recovery_now_ms is not None
            else self._now_ms()
        )
        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        default = self._state.get("default", {})
        max_stamp: Optional[int] = None
        for key in pending:
            value = default.get(key)
            if value is None:
                continue
            decoded = _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                continue
            stamp, _ = decoded
            if stamp == SENTINEL_NEVER:
                return False
            if max_stamp is None or stamp > max_stamp:
                max_stamp = stamp
        if max_stamp is None:
            return False
        return max_stamp <= now

    def _rebuild_index_from_stamped_census(self) -> None:
        """
        §5.4 done-marker index rebuild (memory parity of
        ``RocksDBStorePartition._rebuild_index_from_stamped_census``). On a cold
        restore of a CORROBORATED cold-adopted memory store the header-absent
        adopted records replay verbatim into the census but carry NO index entry;
        rebuild the in-RAM ``__ttl_index__`` from the still-censused all-stamped
        records (stamps kept verbatim, non-sentinel only) before the census is
        discarded, else the adopted records are unindexed and the sweep can never
        expire them. Precondition: :meth:`_all_pending_values_are_stamped` is True.
        """
        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        default = self._state.get("default", {})
        index = self._state.setdefault(TTL_INDEX_CF_NAME, {})
        rebuilt = 0
        for key in pending:
            value = default.get(key)
            if value is None:
                continue
            decoded = _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                continue
            stamp, _ = decoded
            if stamp != SENTINEL_NEVER:
                index[encode_index_key(stamp, key)] = b""
                rebuilt += 1
        if rebuilt:
            logger.info(
                "Recovery: rebuilt %d in-memory __ttl_index__ entry(ies) from the "
                "corroborated cold-adopted census before discard (done-marker "
                "path).",
                rebuilt,
            )

    def _adopt_v3240_stamps(self) -> None:
        """
        PROVISIONAL, REVERSIBLE adoption of the pending census (spec §5.2 / §5.7
        memory parity of ``RocksDBStorePartition._adopt_v3240_stamps``). Flip into
        TTL mode, keep each default value verbatim (no re-wrap — preserves the
        v3.24.0 stamp), rebuild the ``__ttl_index__`` entry from each non-sentinel
        stamp, discard the census, then set the provisional marker + runtime guard
        (``_adopt_provisional``) so the sweep is suppressed (§5.3) until a live
        ``ttl=`` write corroborates (§5.4). Changelog-silent.

        No in-RAM value backup is taken: memory rollback (§5.7) is
        replay-suppression, not an in-lifetime restore — a fresh full changelog
        replay reconstructs the un-adopted originals — so a backup copy would be
        dead state (finding #5).
        """
        if not self.uses_ttl_stamps:
            self.uses_ttl_stamps = True
            self._state.setdefault(TTL_INDEX_CF_NAME, {})

        default = self._state.setdefault("default", {})
        index = self._state.setdefault(TTL_INDEX_CF_NAME, {})
        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})

        adopted = 0
        for key in list(pending.keys()):
            value = default.get(key)
            if value is None:
                continue
            decoded = _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                continue
            stamp, _ = decoded
            if stamp != SENTINEL_NEVER:
                index[encode_index_key(stamp, key)] = b""
            adopted += 1
        self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)

        # Provisional marker + runtime guard (parity with RocksDB).
        now_ms = (
            self._recovery_now_ms
            if self._recovery_now_ms is not None
            else self._now_ms()
        )
        self._state.setdefault(METADATA_CF_NAME, {})[TTL_ADOPT_PENDING_KEY] = (
            int_to_bytes(now_ms)
        )
        self._adopt_provisional = True

        logger.warning(
            "Auto-adopted %d v3.24.0-shaped record(s) from the in-memory changelog "
            "(REVERSIBLE): values kept verbatim, __ttl_index__ rebuilt, "
            "sweep-deletion suppressed until corroborated by a live ttl= write. If "
            "this is actually a pre-TTL legacy store, set "
            "QUIXSTREAMS_STATE_TTL_ROLLBACK=1 and restart to suppress the adopt "
            "(the next full changelog replay reconstructs the un-adopted "
            "originals).",
            adopted,
        )

    def corroborate_adoption(self) -> None:
        """
        §5.4 corroboration (memory parity of
        ``RocksDBStorePartition.corroborate_adoption``): a live ``ttl=`` write
        confirms a provisional cold-adoption. Produce the durable migration-done
        marker (changelog-first, confirm-or-raise), drop the in-RAM backup +
        provisional marker, and lift the sweep guard. One-time per partition.
        """
        self._produce_migration_done_marker()
        self._state.get(METADATA_CF_NAME, {}).pop(TTL_ADOPT_PENDING_KEY, None)
        self._adopt_provisional = False
        logger.info(
            "Corroborated v3.24.0 adoption in memory on a live ttl= write; produced "
            "the durable migration-done marker, lifted sweep suppression."
        )

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        return self._changelog_offset

    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """
        self._changelog_offset = offset

    @_validate_partition_state()
    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from the store

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the store. Otherwise, `default`
        """
        return self._state.get(cf_name, {}).get(key, Marker.UNDEFINED)

    @_validate_partition_state()
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the store.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        return key in self._state.get(cf_name, {})

    # ------------------------------------------------------------------
    # TTL helpers (mirror of RocksDBStorePartition).
    # ------------------------------------------------------------------

    def _load_high_water(self) -> None:
        raw = self._state.get(METADATA_CF_NAME, {}).get(TTL_HIGH_WATER_KEY)
        if raw is None:
            return
        try:
            self._high_water_ms = int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning("Failed to decode persisted TTL high-water; ignoring.")

    def _normalize_replay_value(self, value: bytes) -> tuple[bytes, int]:
        # Route through the same strict validator as the live read path;
        # see RocksDBStorePartition._normalize_replay_value. A value that
        # does not validate as a stamp (too short, zero, or out-of-range) is
        # treated as never-expires and sentinel-wrapped so it round-trips on read
        # and is never dropped by the latest-record-wins recovery-drop filter.
        decoded = _safe_decode_stamp(value)
        if decoded is None:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        stamp, _ = decoded
        return value, stamp

    def _looks_like_stamped_value(self, value: bytes) -> bool:
        """See ``RocksDBStorePartition._looks_like_stamped_value``.

        NOTE: as of the header-routing change this is **no longer on the
        recovery path** — ``recover_from_changelog_message`` routes purely on the
        ``__ttl_stamped__`` header. Retained (not deleted) to stay symmetric with
        the RocksDB backend, which keeps its copy because a test patches it; this
        method has no live caller in the memory backend and must not be
        reintroduced into recovery (it false-positives on legacy 8-byte epoch-ms
        values).
        """
        if len(value) < 8:
            return False
        try:
            stamp, _ = decode_ttl_value(value)
        except ValueError:
            return False
        if stamp == SENTINEL_NEVER:
            return True
        return 0 < stamp < 10**15

    def _max_index_stamp_ms(self) -> Optional[int]:
        """Max expiry stamp among in-RAM ``__ttl_index__`` entries, or ``None``
        if empty (parity with RocksDB; see
        ``RocksDBStorePartition._max_index_stamp_ms``)."""
        index = self._state.get(TTL_INDEX_CF_NAME, {})
        last_stamp: Optional[int] = None
        for index_key in index:
            try:
                stamp, _ = decode_index_key(cast(bytes, index_key))
            except ValueError:
                continue
            if last_stamp is None or stamp > last_stamp:
                last_stamp = stamp
        return last_stamp

    def _run_sweep(self) -> None:
        if self._high_water_ms is None:
            return
        if self._adopt_provisional:
            # §5.3 sweep guard: no eviction while a COLD-heuristic adoption is
            # provisional (uncorroborated). Lifted on corroboration.
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index = self._state.setdefault(TTL_INDEX_CF_NAME, {})
        main = self._state.setdefault("default", {})

        evicted = 0
        visited = 0
        # Iterate in sorted order — index keys are big-endian-stamped so
        # ascending byte order equals ascending expiry order. The budget
        # counts every index-entry VISIT (ghost or genuine), not just evictions,
        # so a store dense with refresh-minted ghost entries cannot pay more than
        # ``budget`` main-CF point-gets per sweep. Convergent — ghosts shrink each
        # sweep until none remain and cease consuming budget.
        for index_key in sorted(index.keys()):
            if visited >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                index.pop(index_key, None)
                visited += 1
                continue

            if idx_expires_at > now_ms:
                # Sorted by expiry — the rest is future (no point-get, so it is
                # not counted against the visit budget).
                break
            visited += 1

            main_value = main.get(user_key)
            if main_value is None:
                index.pop(index_key, None)
                continue

            # Decode through the same strict validator as the live read path:
            # a value that does not validate as a stamp (too short,
            # zero, or out-of-range) reads as never-expires, so the sweep must not
            # evict it — drop the orphan index pointer only.
            decoded_main = _safe_decode_stamp(cast(bytes, main_value))
            if decoded_main is None:
                index.pop(index_key, None)
                continue
            main_expires_at, _ = decoded_main

            if main_expires_at == SENTINEL_NEVER:
                index.pop(index_key, None)
                continue

            if main_expires_at == idx_expires_at:
                main.pop(user_key, None)
                index.pop(index_key, None)
                evicted += 1
            else:
                index.pop(index_key, None)

    def sweep_expired_into_cache(
        self,
        cache: PartitionTransactionCache,
        staged_default_keys: set[bytes],
        staged_ttl_index_keys: set[bytes],
    ) -> None:
        """
        Prepare-time sweep on the ON path (parity with
        ``RocksDBStorePartition.sweep_expired_into_cache``).

        Reads committed in-RAM state (``self._state``) and stages its deletes into
        the transaction ``cache``: a main-CF eviction →
        ``cache.delete(user_key, cf_name="default")`` (base ``_prepare`` produces a
        changelog tombstone AND ``write()`` applies the local delete); index-CF GC
        → ``cache.delete(index_key, cf_name=TTL_INDEX_CF_NAME)`` (local-only).

        Unlike the OFF-path :meth:`_run_sweep` — which runs in ``write()`` AFTER the
        cache is applied to ``self._state`` and so sees fresh writes — this scan
        runs at prepare-time against COMMITTED state, so it takes the ``staged_*``
        guard sets from the caller to preserve the #1129 same-flush protections
        (never evict / GC a key this transaction just wrote). ``prefix=b""`` is
        safe for every delete because staged keys are never handed to
        ``cache.delete``.
        """
        if self._high_water_ms is None:
            return
        if self._adopt_provisional:
            # §5.3 sweep guard (ON path): no eviction while a COLD-heuristic
            # adoption is provisional (uncorroborated). Lifted on corroboration.
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index = self._state.get(TTL_INDEX_CF_NAME, {})
        main = self._state.get("default", {})

        def delete_index_if_not_staged(index_key: bytes) -> None:
            if index_key not in staged_ttl_index_keys:
                cache.delete(index_key, prefix=b"", cf_name=TTL_INDEX_CF_NAME)

        evicted = 0
        visited = 0
        # Budget counts every index-entry visit (ghost or genuine), bounding
        # main-CF point-gets to <= budget per sweep (parity with _run_sweep).
        for index_key in sorted(index.keys()):
            if visited >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                delete_index_if_not_staged(index_key)
                visited += 1
                continue

            if idx_expires_at > now_ms:
                # Sorted by expiry — the rest is future (no point-get, not counted).
                break
            visited += 1

            main_value = main.get(user_key)
            if main_value is None:
                delete_index_if_not_staged(index_key)
                continue

            decoded_main = _safe_decode_stamp(cast(bytes, main_value))
            if decoded_main is None:
                delete_index_if_not_staged(index_key)
                continue
            main_expires_at, _ = decoded_main

            if main_expires_at == SENTINEL_NEVER:
                delete_index_if_not_staged(index_key)
                continue

            if (
                main_expires_at == idx_expires_at
                and user_key not in staged_default_keys
            ):
                cache.delete(user_key, prefix=b"", cf_name="default")
                delete_index_if_not_staged(index_key)
                evicted += 1
            else:
                delete_index_if_not_staged(index_key)


class MemoryPartitionTransaction(PartitionTransaction[bytes, Any]):
    """
    TTL-aware transaction for the in-memory store.

    Mirrors :class:`quixstreams.state.rocksdb.RocksDBPartitionTransaction`
    exactly except for the absence of a RocksDB write batch — writes go
    straight to the parent's transaction cache.
    """

    _partition: MemoryStorePartition  # type: ignore[assignment]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._partition = cast(MemoryStorePartition, self._partition)
        self._batch_has_ttl_writes: bool = False
        self._pending_stamps: dict[tuple[bytes, bytes], int] = {}
        # Max ``ttl=`` duration (ms) among default-CF TTL writes staged on a
        # not-yet-flipped partition (#1, parity with
        # ``RocksDBPartitionTransaction._max_batch_ttl_ms``). Feeds the implicit
        # legacy expiry (``high_water + max(ttl=)``) when a POPULATED legacy memory
        # store flips WITHOUT ``legacy_records_ttl`` configured. ``None`` until the
        # first ttl= write; only maintained on the unflipped write path.
        self._max_batch_ttl_ms: Optional[int] = None

    def _stamps_enabled(self, cf_name: str) -> bool:
        return cf_name == "default" and self._partition.uses_ttl_stamps

    def _track_batch_ttl_ms(self, ttl: timedelta) -> None:
        """Record the max ``ttl=`` duration (ms) among unflipped default-CF TTL
        writes (#1, parity with
        ``RocksDBPartitionTransaction._track_batch_ttl_ms``). Called only after
        ``_compute_stamp`` has validated ``ttl > 0``."""
        ttl_ms = _ttl_to_ms(ttl)
        if self._max_batch_ttl_ms is None or ttl_ms > self._max_batch_ttl_ms:
            self._max_batch_ttl_ms = ttl_ms

    def _compute_stamp(self, ttl: Optional[timedelta], timestamp: Optional[int]) -> int:
        if ttl is None:
            return SENTINEL_NEVER
        if ttl <= timedelta(0):
            raise ValueError(f"ttl must be a positive timedelta or None, got {ttl!r}")
        if timestamp is None:
            raise ValueError(
                "ttl=... on state.set() requires the current record's "
                "event-time timestamp; the framework injects it via the "
                "stateful StreamingDataFrame wrapper. If you are calling "
                "state.set() outside an apply/update/filter callback, use "
                "as_state(prefix, timestamp=...)."
            )
        expiry = timestamp + _ttl_to_ms(ttl)
        if expiry < 0:
            # #12: reject a negative expiry loudly (parity with
            # ``RocksDBPartitionTransaction._compute_stamp``); caught by the #14
            # validate-before-stage + FAILED handling so nothing is committed.
            raise ValueError(
                f"ttl=... produced a negative expiry ({expiry} ms) from "
                f"timestamp={timestamp} and ttl={ttl!r}; a pre-epoch or negative "
                "event-time cannot be TTL-stamped."
            )
        if expiry >= _MAX_PLAUSIBLE_STAMP_MS:
            # Symmetric upper bound (review re-review #3), parity with
            # ``RocksDBPartitionTransaction._compute_stamp``: a stamp this large
            # encodes fine but the strict read validator refuses it on every
            # read, so reject at write time with the same ValueError class.
            raise ValueError(
                f"ttl=... produced an implausible expiry ({expiry} ms >= "
                f"{_MAX_PLAUSIBLE_STAMP_MS}) from timestamp={timestamp} and "
                f"ttl={ttl!r}; this exceeds the maximum representable TTL stamp "
                "and would be unreadable. Check for a nanosecond/second vs "
                "millisecond timestamp mistake or an unbounded ttl."
            )
        return expiry

    def set(
        self,
        key: Any,
        value: Any,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
        ttl: Optional[timedelta] = None,
    ) -> None:
        if cf_name != "default" or not type(self._partition).uses_ttl_stamps:
            super().set(
                key=key,
                value=value,
                prefix=prefix,
                cf_name=cf_name,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        if self._partition.uses_ttl_stamps:
            self._set_default_cf_stamped(key, value, prefix, timestamp, ttl)
            return

        # #14: validate the stamp BEFORE staging the value, fail on a stamp
        # ValueError (parity with RocksDB) so a caught error stages nothing. The
        # TTL bookkeeping is grouped with the validated stamp.
        if ttl is not None:
            try:
                stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            except ValueError:
                self._status = PartitionTransactionStatus.FAILED
                raise
            self._batch_has_ttl_writes = True
            self._track_batch_ttl_ms(ttl)
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)
        elif self._pending_stamps:
            # Fix 2 (review re-review #2), parity with
            # ``RocksDBPartitionTransaction.set``: last-write-wins — a plain
            # (no-ttl) write of a key that had an earlier ttl= write staged in
            # this unflipped batch must clear that key's pending stamp, else the
            # flip stamps the never-expires value with the stale finite expiry.
            # Gated on a non-empty map so the pure-legacy hot path pays nothing.
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps.pop((prefix, key_serialized), None)

        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

        super().set_bytes(
            key=key, value=value_serialized, prefix=prefix, cf_name="default"
        )

    def set_bytes(
        self,
        key: Any,
        value: bytes,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
        ttl: Optional[timedelta] = None,
    ) -> None:
        if cf_name != "default" or not type(self._partition).uses_ttl_stamps:
            super().set_bytes(
                key=key,
                value=value,
                prefix=prefix,
                cf_name=cf_name,
                timestamp=timestamp,
                ttl=ttl,
            )
            return

        if not isinstance(value, bytes):
            self._status = PartitionTransactionStatus.FAILED
            raise StateSerializationError("Value must be bytes")

        if self._partition.uses_ttl_stamps:
            self._set_bytes_default_cf_stamped(key, value, prefix, timestamp, ttl)
            return

        # #14: validate the stamp BEFORE staging (see :meth:`set`).
        if ttl is not None:
            try:
                stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            except ValueError:
                self._status = PartitionTransactionStatus.FAILED
                raise
            self._batch_has_ttl_writes = True
            self._track_batch_ttl_ms(ttl)
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)
        elif self._pending_stamps:
            # Fix 2 (review re-review #2): last-write-wins — clear a stale pending
            # stamp for this key (see :meth:`set`). Gated on a non-empty map.
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps.pop((prefix, key_serialized), None)

        super().set_bytes(key=key, value=value, prefix=prefix, cf_name="default")

    def delete(self, key: Any, prefix: bytes, cf_name: str = "default") -> None:
        # Fix 2 (review re-review #2), parity with
        # ``RocksDBPartitionTransaction.delete``: pop any pending stamp for a
        # deleted key so a delete()→set(no-ttl) sequence cannot resurrect a stale
        # stamp at flip time. Gated on a non-empty map (a no-op on the flipped
        # inline path). ``super().delete`` does the cache mutation + status check.
        if self._pending_stamps:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps.pop((prefix, key_serialized), None)
        super().delete(key=key, prefix=prefix, cf_name=cf_name)

    def _delete_stale_index_entry(
        self, key_serialized: bytes, prefix: bytes, new_stamp: int
    ) -> None:
        """#8 (review batch 3): inline-delete a key's OLD ``__ttl_index__`` entry
        on refresh so a refresh-heavy store does not accumulate ghost index
        entries. Mirror of
        ``RocksDBPartitionTransaction._delete_stale_index_entry``: update-cache
        fast-path first (within-batch repeat, no store read), else read the
        committed in-RAM value; no-op for a new key, a sentinel old stamp, or an
        unchanged stamp. MUST run BEFORE the new value is staged. The
        ``__ttl_index__`` CF is LOCAL_ONLY, so the delete never reaches the
        changelog."""
        cached = self._update_cache.get(
            key=key_serialized, prefix=prefix, cf_name="default"
        )
        if cached is Marker.DELETED:
            return
        if cached is Marker.UNDEFINED:
            committed = self._partition.get(key_serialized, cf_name="default")
            if committed is Marker.UNDEFINED:
                return
            old_raw = cast(bytes, committed)
        else:
            old_raw = cast(bytes, cached)
        decoded = _safe_decode_stamp(old_raw)
        if decoded is None:
            return
        old_stamp, _ = decoded
        if old_stamp == SENTINEL_NEVER or old_stamp == new_stamp:
            return
        self._update_cache.delete(
            key=encode_index_key(old_stamp, key_serialized),
            prefix=b"",
            cf_name=TTL_INDEX_CF_NAME,
        )

    def _set_default_cf_stamped(
        self,
        key: Any,
        value: Any,
        prefix: bytes,
        timestamp: Optional[int],
        ttl: Optional[timedelta],
    ) -> None:
        if timestamp is not None:
            self._partition.advance_high_water(timestamp)
        try:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        except ValueError:
            self._status = PartitionTransactionStatus.FAILED
            raise
        if ttl is not None:
            # Live ttl= write on an already-flipped partition (non-sentinel stamp);
            # record it so §5.4 corroboration can fire (no-op unless provisional).
            self._batch_has_ttl_writes = True
        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        key_serialized = self._serialize_key(key, prefix=prefix)
        self._delete_stale_index_entry(key_serialized, prefix, stamp)
        stamped = encode_ttl_value(stamp, value_serialized)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
            self._update_cache.set(
                key=encode_index_key(stamp, key_serialized),
                value=b"",
                prefix=b"",
                cf_name=TTL_INDEX_CF_NAME,
            )

    def _set_bytes_default_cf_stamped(
        self,
        key: Any,
        value: bytes,
        prefix: bytes,
        timestamp: Optional[int],
        ttl: Optional[timedelta],
    ) -> None:
        if timestamp is not None:
            self._partition.advance_high_water(timestamp)
        try:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        except ValueError:
            self._status = PartitionTransactionStatus.FAILED
            raise
        if ttl is not None:
            # See :meth:`_set_default_cf_stamped`: mark a live ttl= write so §5.4
            # corroboration can fire.
            self._batch_has_ttl_writes = True
        key_serialized = self._serialize_key(key, prefix=prefix)
        self._delete_stale_index_entry(key_serialized, prefix, stamp)
        stamped = encode_ttl_value(stamp, value)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
            self._update_cache.set(
                key=encode_index_key(stamp, key_serialized),
                value=b"",
                prefix=b"",
                cf_name=TTL_INDEX_CF_NAME,
            )

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offsets: Optional[dict[str, int]] = None) -> None:
        """
        Run flush-time TTL detection / flip-or-reject before delegating to
        the parent's changelog production. See
        ``RocksDBPartitionTransaction.prepare`` for the design notes.
        """
        # Mirror the base-class contract: the flip hook runs before
        # ``super().prepare()``, so an exception here must set status=FAILED rather
        # than leave the transaction STARTED / reusable.
        try:
            self._maybe_flip_or_reject()
            self._maybe_corroborate_adoption()
            self._sweep_expired_into_cache_if_enabled()
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        super().prepare(processed_offsets=processed_offsets)

    def _maybe_corroborate_adoption(self) -> None:
        """§5.4 corroboration hook (memory parity of
        ``RocksDBPartitionTransaction._maybe_corroborate_adoption``). A live
        ``ttl=`` write on a provisionally cold-adopted partition confirms the
        adoption; a plain ``set()`` (sentinel) never sets ``_batch_has_ttl_writes``
        and so never corroborates."""
        partition = self._partition
        if (
            self._batch_has_ttl_writes
            and partition.uses_ttl_stamps
            and partition._adopt_provisional  # noqa: SLF001
        ):
            partition.corroborate_adoption()

    def _sweep_expired_into_cache_if_enabled(self) -> None:
        """
        Prepare-time TTL sweep on the ON path (parity with
        ``RocksDBPartitionTransaction._sweep_expired_into_cache_if_enabled``).
        Skipped on the OFF escape hatch, on an unflipped partition, or on an empty
        cache (a read-only tx never swept). The ``staged_*`` guard sets are the
        cache's own pending writes, read one phase before ``write()`` reads them,
        so the same-flush protections are byte-identical.
        """
        partition = self._partition
        if not partition.ttl_changelog_tombstones:
            return
        if not partition.uses_ttl_stamps:
            return
        if self._update_cache.is_empty():
            return

        staged_default = {
            key
            for prefix_map in self._update_cache.get_updates("default").values()
            for key in prefix_map
        }
        staged_ttl_index = {
            key
            for prefix_map in self._update_cache.get_updates(TTL_INDEX_CF_NAME).values()
            for key in prefix_map
        }
        partition.sweep_expired_into_cache(
            self._update_cache, staged_default, staged_ttl_index
        )

    def _resolve_legacy_expiry(self) -> int:
        """
        Resolve the uniform expiry for the #1 live in-RAM backfill via the frozen
        expiry chain (parity with ``RocksDBPartitionTransaction._legacy_expiry_from_ttl_ms``):
        ``legacy_records_ttl`` (if configured) → ``high_water + max(batch ttl=)``
        (the implicit window derived from the triggering batch) → ``SENTINEL_NEVER``
        (defensive; unreachable on the live path where a ttl= write always set
        ``_max_batch_ttl_ms``). ``enable_time`` is the event-time high-water; a
        ``None`` high-water is the same should-be-unreachable framework bug RocksDB
        hard-errors on (the triggering ttl= write carried no timestamp, which
        ``_compute_stamp`` already rejects).
        """
        enable_time_ms = self._partition.high_water_ms
        if enable_time_ms is None:
            raise IncompatibleStateStoreError(
                "Cannot backfill legacy in-memory records: no event-time "
                "high-water is available at flip (a ttl= write carried no record "
                "timestamp). This should have been rejected at the "
                "state.set(..., ttl=...) call site."
            )
        legacy_records_ttl = self._partition.legacy_records_ttl
        if legacy_records_ttl is not None:
            return self._clamp_legacy_expiry(
                enable_time_ms + _ttl_to_ms(legacy_records_ttl)
            )
        if self._max_batch_ttl_ms is not None:
            return self._clamp_legacy_expiry(enable_time_ms + self._max_batch_ttl_ms)
        return SENTINEL_NEVER

    def _clamp_legacy_expiry(self, expiry_ms: int) -> int:
        """
        Clamp an additive legacy-backfill expiry to :data:`SENTINEL_NEVER` when it
        would exceed the maximum readable stamp (parity with the RocksDB
        ``_legacy_expiry_from_ttl_ms`` / completion clamp), WARNing when the clamp
        fires. Keeps the backfilled records readable instead of writing an
        unreadable over-range stamp (review re-review #4).
        """
        clamped = clamp_additive_expiry(expiry_ms)
        if clamped != expiry_ms:
            logger.warning(
                "Legacy in-RAM backfill expiry %d exceeds the maximum readable TTL "
                "stamp (%d); clamping to never-expire (SENTINEL) so the backfilled "
                "records stay readable. Configure a smaller legacy_records_ttl for "
                "a finite window.",
                expiry_ms,
                _MAX_PLAUSIBLE_STAMP_MS,
            )
        return clamped

    def _backfill_populated_legacy_in_ram(self, expires_at_ms: int) -> int:
        """
        Re-stamp every pre-existing default-CF record in RAM with ``expires_at_ms``
        (#1). Excludes the in-batch staged keys (they carry their own pending stamp
        and are re-stamped by the shared cache loop). Each re-stamp is produced to
        the changelog via the non-transactional migration route with the
        ``__ttl_stamped__`` header and always-apply (``processed_offsets=None``)
        semantics — exactly as memory recovery-completion does.

        **Changelog-first ordering** (parity with
        ``RocksDBStorePartition.backfill_legacy_records`` /
        ``_flush_backfill_changelog``): the re-stamps are produced and their
        delivery CONFIRMED with a bounded flush BEFORE the local in-RAM store is
        mutated. A stalled flush raises :class:`ChangelogFlushError` with the
        RAM store untouched, so the flip is aborted and the next ``ttl=`` write
        retries cleanly from the original legacy values (no double-wrap) — the
        two backends resolve a stalled populated-store flip identically. No
        chunking: the store is RAM-bounded. Returns the number of pre-existing
        records re-stamped.
        """
        partition = self._partition
        default = partition._state.setdefault("default", {})  # noqa: SLF001
        index = partition._state.setdefault(TTL_INDEX_CF_NAME, {})  # noqa: SLF001
        staged_keys = {
            key
            for prefix_map in self._update_cache.get_updates("default").values()
            for key in prefix_map
        }
        headers: Optional[HeadersMapping] = None
        if self._changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                CHANGELOG_TTL_STAMPED_HEADER: b"\x01",
            }
        # Phase 1: compute the stamped values + produce them to the changelog.
        # NOTHING is written to the local RAM store yet — the changelog must be
        # confirmed first (below) so the local store never gets ahead of it.
        pending_restamps: list[tuple[bytes, bytes]] = []
        produced = False
        for key in list(default.keys()):
            if key in staged_keys:
                # An in-batch write overwrote this pre-existing key: it is stamped
                # with its own pending stamp by the shared cache loop, not the
                # uniform legacy expiry.
                continue
            stamped = encode_ttl_value(expires_at_ms, cast(bytes, default[key]))
            pending_restamps.append((key, stamped))
            if self._changelog_producer is not None:
                self._changelog_producer.produce(
                    key=key,
                    value=stamped,
                    headers=headers,
                    migration=True,
                    on_delivery=partition._on_backfill_delivery,  # noqa: SLF001
                )
                partition._backfill_produced += 1  # noqa: SLF001
                produced = True

        # Phase 2: confirm delivery (bounded) BEFORE the local commit. Raises
        # ChangelogFlushError on a stalled flush, leaving the RAM store untouched.
        if produced and self._changelog_producer is not None:
            partition._confirm_migration_delivery_or_raise(  # noqa: SLF001
                self._changelog_producer,
                "legacy re-stamp changelog record(s) during the populated-legacy "
                "live backfill",
            )

        # Phase 3: local commit — mutate the RAM store + secondary index only now
        # that the changelog is durable.
        for key, stamped in pending_restamps:
            default[key] = stamped
            if expires_at_ms != SENTINEL_NEVER:
                index[encode_index_key(expires_at_ms, key)] = b""
        return len(pending_restamps)

    def _maybe_flip_or_reject(self) -> None:
        if not self._batch_has_ttl_writes:
            return
        if self._partition.uses_ttl_stamps:
            return

        populated = self._partition.main_cf_has_user_data()
        restamped = 0
        expires_at_ms: Optional[int] = None
        if populated:
            # #1 (review batch 3, HIGH): live in-RAM migration of a POPULATED
            # legacy memory store on the first ttl= write — mirrors the RocksDB
            # populated path but on the in-RAM dict (data already in RAM → no
            # chunking / OOM concern). This REPLACES the old warn-and-defer no-op,
            # which deadlocked: the deferred changelog rebuild classified the store
            # as pure-legacy and no-op'd forever, so TTL never engaged.
            expires_at_ms = self._resolve_legacy_expiry()
            restamped = self._backfill_populated_legacy_in_ram(expires_at_ms)

        # Shared in-batch cache re-encode (empty-store fast path AND the tail of
        # the populated path): stamp the genuine in-batch user writes with their
        # own pending stamps. The pre-existing records were already re-stamped
        # directly in RAM above and are NOT in the cache.
        cache = self._update_cache
        updates = cache.get_updates(cf_name="default")
        for prefix in list(updates.keys()):
            for serialized_key, raw_value in list(updates[prefix].items()):
                stamp = self._pending_stamps.get(
                    (prefix, serialized_key), SENTINEL_NEVER
                )
                stamped = encode_ttl_value(stamp, raw_value)
                cache.set(
                    key=serialized_key,
                    value=stamped,
                    prefix=prefix,
                    cf_name="default",
                )
                if stamp != SENTINEL_NEVER:
                    cache.set(
                        key=encode_index_key(stamp, serialized_key),
                        value=b"",
                        prefix=b"",
                        cf_name=TTL_INDEX_CF_NAME,
                    )

        # Persist the flip flag + format version into the metadata CF cache;
        # MemoryStorePartition.write applies it together with user writes.
        cache.set(
            key=TTL_ENABLED_KEY,
            value=b"\x01",
            prefix=b"",
            cf_name=METADATA_CF_NAME,
        )
        cache.set(
            key=STATE_FORMAT_VERSION_KEY,
            value=int_to_bytes(STATE_FORMAT_VERSION),
            prefix=b"",
            cf_name=METADATA_CF_NAME,
        )
        # Durable done-flag: staged into the ``__ttl_system__`` CF. The base
        # ``_prepare`` produces that system CF LAST by explicit ordering (NOT by
        # staging order — the staged CF set is unordered; see
        # ``base/transaction.py``), identical to the empty-store fast path on BOTH
        # enable paths. The replicated ``__ttl_system__`` CF is not local-only, so
        # the base ``_prepare`` produces it to the changelog and ``write()`` lands
        # it in the in-RAM dict — a cold rebuild then learns "done, never redo" and
        # does not re-run the migration.
        cache.set(
            key=TTL_MIGRATION_DONE_KEY,
            value=int_to_bytes(STATE_FORMAT_VERSION),
            prefix=b"",
            cf_name=TTL_SYSTEM_CF_NAME,
        )
        self._partition.uses_ttl_stamps = True
        self._partition._state.setdefault(TTL_INDEX_CF_NAME, {})  # noqa: SLF001

        if populated:
            if self._partition.legacy_records_ttl is None:
                # §15.1 implicit window: derived from the triggering write, not
                # config. Reuse the RocksDB wording.
                logger.warning(
                    "Enabled TTL on a populated legacy in-memory store WITHOUT "
                    "legacy_records_ttl configured: auto-backfilled %d pre-existing "
                    "record(s) with an implicit expiry of high_water + %s ms "
                    "(= %s), derived from the triggering state.set(..., ttl=...) "
                    "write. To choose a different uniform window for legacy "
                    "records, construct the store with "
                    "legacy_records_ttl=timedelta(...).",
                    restamped,
                    self._max_batch_ttl_ms,
                    expires_at_ms,
                )
            logger.info(
                "Backfilled %d legacy records and flipped in-memory state "
                "partition into TTL mode (populated-store live backfill).",
                restamped,
            )
        else:
            logger.info(
                "Flipping memory state partition into TTL mode (empty-store fast "
                "path)."
            )

    def _get_bytes(
        self,
        key: Any,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
    ) -> Union[bytes, Literal[Marker.UNDEFINED, Marker.DELETED]]:
        if not self._stamps_enabled(cf_name):
            return super()._get_bytes(
                key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
            )

        if timestamp is not None:
            self._partition.advance_high_water(timestamp)

        raw = super()._get_bytes(
            key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
        )
        if raw is Marker.UNDEFINED or raw is Marker.DELETED:
            return raw

        raw_bytes = cast(bytes, raw)
        if len(raw_bytes) < TTL_STAMP_BYTES:
            return Marker.UNDEFINED

        # Fail-safe decode through the strict validator, mirroring
        # ``RocksDBPartitionTransaction._get_bytes``: only strip the
        # 8-byte stamp when it robustly looks like one. A value that does not
        # validate (raw legacy value, zero/out-of-range prefix) degrades to raw
        # (never-expires) rather than being mis-stripped or mis-expired, so reads,
        # the sweep, and recovery all agree.
        decoded = _safe_decode_stamp(raw_bytes)
        if decoded is None:
            # Warn once per PARTITION, not per checkpoint tx.
            if not self._partition._unstamped_read_warned:  # noqa: SLF001
                self._partition._unstamped_read_warned = True  # noqa: SLF001
                logger.warning(
                    "Fail-safe TTL read: a flipped partition holds a value that "
                    "does not decode to a valid stamp (key prefix=%r) at "
                    "path=%s; returning it raw and treating it as never-expires "
                    "rather than stripping 8 bytes. This should be impossible on "
                    "a fully backfilled store and signals a store written before "
                    "the latest-record-wins recovery fix, or "
                    "an externally-mutated value.",
                    prefix[:16],
                    getattr(self._partition, "path", "<memory>"),
                )
            return raw_bytes

        stamp, payload = decoded
        if stamp == SENTINEL_NEVER:
            return payload

        now = self._partition.high_water_ms
        if now is not None and stamp <= now:
            return Marker.UNDEFINED

        return payload

    def exists(
        self,
        key: Any,
        prefix: bytes,
        cf_name: str = "default",
        timestamp: Optional[int] = None,
    ) -> bool:
        if not self._stamps_enabled(cf_name):
            return super().exists(
                key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
            )
        result = self._get_bytes(
            key=key, prefix=prefix, cf_name=cf_name, timestamp=timestamp
        )
        return result is not Marker.UNDEFINED and result is not Marker.DELETED
