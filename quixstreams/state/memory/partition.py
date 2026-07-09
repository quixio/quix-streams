import functools
import logging
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
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
)
from quixstreams.state.rocksdb.transaction import _safe_decode_stamp, _ttl_to_ms
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    TTL_STAMP_BYTES,
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
    # flag is initialized to False in ``__init__`` to mirror the v3 v3.23.6-
    # compatible default; the partition flips itself into TTL mode on first
    # detection of a ``state.set(..., ttl=...)`` write at flush time.
    uses_ttl_stamps: bool = True

    def __init__(
        self,
        changelog_producer: Optional[ChangelogProducer],
        max_evictions_per_flush: int = _DEFAULT_MAX_EVICTIONS_PER_FLUSH,
        legacy_records_ttl: Optional[timedelta] = None,
        ttl_changelog_tombstones: bool = True,
        adopt_v3240_stamps: bool = False,
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
        # Opt-in gate for adopting a store created on the v3.24.0 TTL preview
        # (M1), mirroring ``RocksDBStorePartition._adopt_v3240_stamps_enabled``.
        # Stored under a distinct attribute so it does NOT shadow the private
        # :meth:`_adopt_v3240_stamps` method. When False (default) a 100%-quorum
        # stamp detection logs a CRITICAL and stays legacy; when True it flips +
        # adopts in place. MemoryStore has no persistent options surface, so it is
        # set only via direct construction (``create_new_partition`` does not
        # forward it, exactly like ``legacy_records_ttl`` /
        # ``ttl_changelog_tombstones``), so production memory is detection-only.
        self._adopt_v3240_stamps_enabled: bool = adopt_v3240_stamps
        self._state: Dict[str, Dict[bytes, Any]] = {
            "default": {},
            METADATA_CF_NAME: {},
        }
        self._closed = False
        self._max_evictions_per_flush = max_evictions_per_flush
        # Optional uniform expiry for leftover legacy records completed during a
        # MIXED-changelog recovery (spec §15.4 parity with
        # ``RocksDBOptions.legacy_records_ttl``). ``None`` = auto-finish at the
        # §15.2 survivor-derived default; a positive ``timedelta`` = complete at
        # ``wallclock_now + legacy_records_ttl``. MemoryStore has no persistent
        # options surface, so this is set only via direct construction.
        self._legacy_records_ttl: Optional[timedelta] = legacy_records_ttl
        self._high_water_ms: Optional[int] = None
        # Wallclock reference captured once per changelog-recovery session
        # (lazily, on the first stamped default-CF replay). Mirrors
        # ``RocksDBStorePartition._recovery_now_ms``: used to judge whether a
        # replayed TTL entry is already expired and to seed the post-recovery
        # high-water (see :meth:`recover_from_changelog_message`,
        # ``spec-recovery-wallclock.md`` §3.2/§3.3). ``None`` means no stamped
        # message has been replayed yet in this partition's lifetime.
        self._recovery_now_ms: Optional[int] = None
        # Count of wallclock-expired stamped records dropped during this
        # recovery's changelog replay (Rule 4 / latest-record-wins). Surfaced as
        # one aggregate INFO at :meth:`complete_recovery` so an operator sees the
        # deletions instead of records silently vanishing. Mirrors
        # ``RocksDBStorePartition._recovery_expired_drops``.
        self._recovery_expired_drops: int = 0
        # Incomplete-migration detection (spec §8.8 / §15.4), mirroring
        # ``RocksDBStorePartition``. Set True on the first header-true default-CF
        # replay; combined with a non-empty ``__ttl_backfill_pending__`` census at
        # end of recovery it marks a MIXED changelog whose leftover legacy records
        # :meth:`complete_recovery` must stamp. False (all-legacy replay) never
        # triggers completion.
        self._recovery_saw_stamped: bool = False
        # §15.2 survivor-derived completion default: max absolute expiry among
        # replayed default-CF records that are stamped (header-true), non-SENTINEL,
        # and NOT dropped by the Rule 4 wallclock filter. Used to complete an
        # incomplete migration when ``legacy_records_ttl`` is absent; ``None`` (no
        # surviving future stamp) falls back to SENTINEL_NEVER + a WARN.
        self._recovery_max_survivor_expiry_ms: Optional[int] = None
        # Durable done-flag latch (spec §13.1), mirroring
        # ``RocksDBStorePartition``. Set True when the replicated
        # ``__ttl_system__`` marker is replayed; makes :meth:`complete_recovery`
        # flip + discard pending + never re-run completion.
        self._recovery_saw_migration_done: bool = False
        # Warn-once guard for the populated-legacy live ``ttl=`` write path
        # (spec §7.3 row I-target); flipped True after the first WARNING.
        self._populated_legacy_warned: bool = False
        # Warn-once guard for the fail-safe degraded TTL read (spec item d),
        # scoped to the partition so it fires once per partition lifetime rather
        # than once per checkpoint transaction.
        self._unstamped_read_warned: bool = False

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
        MIXED-changelog recovery (spec §15.4). Mirrors
        ``RocksDBStorePartition.legacy_records_ttl``. ``None`` = auto-finish at
        the §15.2 default.
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

    @property
    def adopt_v3240_stamps(self) -> bool:
        """
        Whether the opt-in v3.24.0 stamp adoption is enabled (M1). When ``False``
        (default) a 100%-quorum stamp detection on recovery logs a CRITICAL and
        leaves the store legacy; when ``True`` it flips + adopts the stamps in
        place. Parity with ``RocksDBStorePartition.adopt_v3240_stamps``.
        """
        return self._adopt_v3240_stamps_enabled

    def advance_high_water(self, timestamp: Optional[int]) -> None:
        if timestamp is None:
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
        Memory mirror of ``RocksDBStorePartition.has_incomplete_ttl_migration``
        (Fix B). Always ``False``: the memory backend has no persistent on-disk
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
        # Done-flag consumption (spec §13.1), mirroring
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

        # Recovery flip-discovery (spec §8.7, mirrors
        # ``RocksDBStorePartition.recover_from_changelog_message``).
        #
        # Every stamped ``default``-CF record produced while the source partition
        # was in TTL mode carries the out-of-band ``__ttl_stamped__`` changelog
        # header (set in the base ``_prepare``), surfaced here as ``ttl_stamped``.
        # On the first header-true default-CF record we flip this recovery
        # partition into TTL mode and latch for the rest of the session. This
        # REPLACES the old value-content heuristic (``_looks_like_stamped_value``),
        # which false-positived on legacy 8-byte epoch-ms values and dropped them
        # (OP-2 / OP-3). Header absent → the record is legacy / un-stamped and
        # replays verbatim below, so a purely legacy changelog never latches
        # (back-compat option (a), §9). The flip is session-local: memory
        # partitions are non-persistent and re-recover from the changelog on every
        # open, so there is no ``_stamp_flip_metadata`` mirror (§8).
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

        # Incomplete-migration detection / census (spec §8.8 / §15.4), mirroring
        # ``RocksDBStorePartition``. Gated on the CLASS-level flag (not the
        # instance flag): a MIXED changelog replays header-absent legacy records
        # BEFORE the first stamped record flips the partition, so they must be
        # censused into ``__ttl_backfill_pending__`` now (and land verbatim below).
        # A header-true replay sets the MIXED-detection latch and supersedes any
        # earlier legacy census entry for the same key (compaction ordering).
        if type(self).uses_ttl_stamps and cf_name == "default":
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
            # §15.4 item 3: a header-absent (legacy) default-CF record replayed on
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
            # each entry's stamp, collapsed uniform-expiry backfilled stores;
            # see spec-recovery-wallclock.md §2/§3). Capture lazily on the first
            # stamped default-CF replay. Finding 3 (§7.4): the wallclock is the
            # Rule 4 replay drop clock ONLY; it is NO LONGER seeded into the live
            # ``_high_water_ms`` clock (removed for parity with RocksDB, so an
            # event-time-lagging workload never over-expires its own writes).
            # Sentinel-stamped entries are never compared and always survive (§7).
            expired = False
            if stamp != SENTINEL_NEVER:
                if self._recovery_now_ms is None:
                    self._recovery_now_ms = self._now_ms()
                recovery_now_ms = self._recovery_now_ms
                expired = stamp <= recovery_now_ms
            if expired:
                # Already-expired against wallclock-at-recovery (Fix A —
                # latest-record-wins, parity with RocksDB). An OLDER copy of
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
                    # §15.2: surviving future non-sentinel stamp on a header-true
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
        Recovery-finalize hook (spec §8.8 / §15.4), mirroring
        :meth:`RocksDBStorePartition.complete_recovery` at the semantics level
        (in-RAM dicts instead of CFs). There is **no reject branch** — the memory
        backend never had one, and the §15 revision auto-finishes the migration.

        Completes an interrupted legacy-TTL migration replayed from a MIXED
        changelog: leftover legacy records landed VERBATIM during replay and were
        censused into ``__ttl_backfill_pending__``; here they are stamped with a
        uniform expiry, indexed, produced as header-true stamped records (so a
        subsequent restore sees an all-stamped changelog and never re-enters
        completion), and drained from the census.

        - NOT ``_recovery_saw_stamped`` → all-legacy replay: discard the orphan
          pending census and no-op (Bug 3 hygiene parity).
        - pending empty → all-stamped / fully migrated: no-op.
        - else (MIXED) → stamp exactly the pending leftovers with:
            - ``legacy_records_ttl`` set → ``wallclock_now + legacy_records_ttl``
              (parity; explicit config wins);
            - absent → the §15.2 survivor-derived expiry, or ``SENTINEL_NEVER``
              (never-expire) + a WARN in the all-expired fallback.
        """
        if self._recovery_expired_drops > 0:
            # #8 (review batch 2): make the Rule-4 wallclock-expired replay drops
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
            # Durable done-flag present (spec §13.1, parity with RocksDB): the
            # migration is complete and is never redone. Ensure flipped, discard
            # any pending census, run no completion.
            if not self.uses_ttl_stamps:
                self.uses_ttl_stamps = True
                self._state.setdefault(TTL_INDEX_CF_NAME, {})
            self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)
            return
        if self._all_pending_values_are_stamped():
            # M1 opt-in gate (parity with ``RocksDBStorePartition.complete_recovery``
            # :meth: gate). This all-stamped census is the exact byte-shape of a
            # stock v3.24.0 cold restore, but it is INDISTINGUISHABLE from a legacy
            # ``set_bytes()`` store whose values begin with plausible-stamp bytes.
            # Adopt ONLY with the explicit opt-in flag; otherwise log a CRITICAL
            # naming the flag and stay legacy (values byte-identical), then discard
            # the orphan census and return. Fires before the §15.2 disposition so a
            # MIXED-C census is adopted (with the flag) rather than double-wrapped;
            # discarding here (no flag) never touches default-CF user data.
            if self._adopt_v3240_stamps_enabled:
                self._adopt_v3240_stamps()
                return
            logger.critical(
                "Recovery replayed an in-memory header-absent changelog whose %d "
                "censused value(s) ALL decode as 8-byte TTL stamps. This is the "
                "exact shape of a store created on the v3.24.0 TTL preview "
                "(stamped values, no __ttl_stamped__ header) -- BUT it is "
                "indistinguishable from a pre-TTL legacy store whose values happen "
                "to begin with 8 plausible big-endian bytes (e.g. epoch-ms or "
                "counters written via set_bytes()). Automatic adoption is DISABLED "
                "by default: adopting a legacy store by mistake would turn the "
                "first 8 bytes of every value into an expiry stamp and delete the "
                "data on the next sweep. NOTHING has been changed -- the store "
                "stays in legacy mode and every value reads back byte-identical. "
                "If you are CERTAIN this store was created on v3.24.0, construct "
                "the MemoryStorePartition with adopt_v3240_stamps=True and restart "
                "to adopt the stamps in place (MemoryStore has no RocksDBOptions "
                "surface). If this is a genuine pre-TTL store, leave the option "
                "unset -- this message is informational and safe to ignore.",
                len(self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})),
            )
            self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)
            return
        if not self._recovery_saw_stamped:
            # Pure-legacy replay: the class-level census may hold orphan legacy
            # keys with no completion pass to drain them — discard them wholesale
            # (Bug 3 hygiene parity with RocksDB's _discard_backfill_pending).
            # Finding-1 loud detection (spec §7.1) runs FIRST: this population is
            # indistinguishable from a stock-v3.24.0 cold restore, so WARN if the
            # censused values mostly look like stamps (detection only — no data
            # is stripped, flipped, or re-routed).
            self._warn_if_looks_like_v3240_upgrade()
            self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)
            return
        if not self.uses_ttl_stamps:
            return

        pending = self._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
        if not pending:
            return

        legacy_records_ttl = self._legacy_records_ttl
        if legacy_records_ttl is not None:
            # Explicit config wins (parity): wallclock-at-rebuild + ttl. Finding 3
            # (§7.4): the wallclock derives this completion expiry only; it is NOT
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
        elif (
            survivor_expiry := (
                self._recovery_max_survivor_expiry_ms
                if self._recovery_max_survivor_expiry_ms is not None
                else self._max_index_stamp_ms()
            )
        ) is not None:
            # §15.2 survivor-derived default (config absent). Fix B parity with
            # RocksDB: fall back to the max in-RAM __ttl_index__ stamp when no
            # replayed survivor was tracked (unreachable for the always-full-
            # replay memory backend, kept for structural symmetry).
            expires_at_ms = survivor_expiry
            logger.warning(
                "Recovery: completing interrupted legacy-TTL migration in memory "
                "WITHOUT legacy_records_ttl; %d leftover legacy record(s) stamped "
                "with expiry=%d, derived from the max surviving stamped record.",
                len(pending),
                expires_at_ms,
            )
        else:
            # §15.2 all-expired fallback: never-expire rather than mass-delete.
            expires_at_ms = SENTINEL_NEVER
            logger.warning(
                "Recovery: completing interrupted legacy-TTL migration in memory "
                "WITHOUT legacy_records_ttl and with NO surviving future stamp; "
                "%d leftover legacy record(s) stamped as never-expiring "
                "(SENTINEL_NEVER) to avoid silently deleting them.",
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
                # Fix C: recovery-completion runs with no open checkpoint
                # transaction, so use the non-transactional migration route under
                # exactly-once (parity with RocksDB).
                self._changelog_producer.produce(
                    key=key, value=stamped, headers=headers, migration=True
                )
                produced = True
            pending.pop(key, None)

        if produced and self._changelog_producer is not None:
            self._changelog_producer.flush(migration=True)

        # Drain the (now-empty) census.
        self._state.pop(TTL_BACKFILL_PENDING_CF_NAME, None)

        # Flag-last (spec §13.1): the migration is complete, so record the
        # durable done-flag marker AFTER the last stamped record.
        self._produce_migration_done_marker()

    def _produce_migration_done_marker(self) -> None:
        """
        Record + produce the durable "migration done" marker (spec §13.1),
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
                # Fix C: non-transactional migration route (parity with RocksDB).
                migration=True,
            )
            unproduced = self._changelog_producer.flush(
                timeout=_MIGRATION_MARKER_FLUSH_TIMEOUT_S, migration=True
            )
            # Fail loudly only on a concrete positive backlog; a non-int return
            # (e.g. an unconfigured test double) is treated as indeterminate.
            if isinstance(unproduced, int) and unproduced > 0:
                raise ChangelogFlushError(
                    f"{unproduced} __ttl_migration_done__ marker record(s) still "
                    f"undelivered after {_MIGRATION_MARKER_FLUSH_TIMEOUT_S}s; "
                    "not recording the marker locally so the next completion "
                    "retries it (the local store must not mark done ahead of the "
                    "changelog)."
                )
        self._state.setdefault(TTL_SYSTEM_CF_NAME, {})[TTL_MIGRATION_DONE_KEY] = (
            marker_value
        )

    def _warn_if_looks_like_v3240_upgrade(self, sample_size: int = 256) -> None:
        """
        Memory mirror of
        ``RocksDBStorePartition._warn_if_looks_like_v3240_upgrade`` (spec §7.1).
        Sample the censused values; if most decode as plausible 8-byte stamps,
        WARN that this LOOKS like a stock-v3.24.0 cold restore. DETECTION ONLY:
        the sampled bytes gate the log line, never data (spec §2/§6).
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
        Total-quorum strict stamp validation over the in-RAM pending census
        (spec §7.1 self-heal), mirroring
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

    def _adopt_v3240_stamps(self) -> None:
        """
        Adopt the pending census as v3.24.0-stamped records (spec §7.1), mirroring
        ``RocksDBStorePartition._adopt_v3240_stamps``. Flip into TTL mode, keep
        each default value verbatim (no re-wrap — §13.2 preserves the v3.24.0
        stamp), rebuild the ``__ttl_index__`` entry from each non-sentinel stamp,
        discard the census, and emit one INFO line. No changelog re-production.
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

        logger.info(
            "Recovery: adopted %d v3.24.0-stamped record(s) in memory (flipped "
            "into TTL mode; values kept verbatim, __ttl_index__ rebuilt from the "
            "adopted stamps).",
            adopted,
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
        # Route through the same strict validator as the live read path (spec
        # item c); see RocksDBStorePartition._normalize_replay_value. A value that
        # does not validate as a stamp (too short, zero, or out-of-range) is
        # treated as never-expires and sentinel-wrapped so it round-trips on read
        # and is never dropped by the Rule 4 recovery-drop filter.
        decoded = _safe_decode_stamp(value)
        if decoded is None:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        stamp, _ = decoded
        return value, stamp

    def _looks_like_stamped_value(self, value: bytes) -> bool:
        """See ``RocksDBStorePartition._looks_like_stamped_value``.

        NOTE: as of the §8.7 header-routing change this is **no longer on the
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
        if empty (Fix B parity with RocksDB; see
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

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index = self._state.setdefault(TTL_INDEX_CF_NAME, {})
        main = self._state.setdefault("default", {})

        evicted = 0
        visited = 0
        # Iterate in sorted order — index keys are big-endian-stamped so
        # ascending byte order equals ascending expiry order. #7: the budget
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

            # Decode through the same strict validator as the live read path
            # (spec item c): a value that does not validate as a stamp (too short,
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
        ``RocksDBStorePartition.sweep_expired_into_cache``, spec §3.1).

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
        ``cache.delete`` (§3.1 prefix note).
        """
        if self._high_water_ms is None:
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
        # #7: budget counts every index-entry visit (ghost or genuine), bounding
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

    def _stamps_enabled(self, cf_name: str) -> bool:
        return cf_name == "default" and self._partition.uses_ttl_stamps

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
        return timestamp + _ttl_to_ms(ttl)

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

        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

        super().set_bytes(
            key=key, value=value_serialized, prefix=prefix, cf_name="default"
        )

        if ttl is not None:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            self._batch_has_ttl_writes = True
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)

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

        super().set_bytes(key=key, value=value, prefix=prefix, cf_name="default")

        if ttl is not None:
            stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
            self._batch_has_ttl_writes = True
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._pending_stamps[(prefix, key_serialized)] = stamp
            if timestamp is not None:
                self._partition.advance_high_water(timestamp)

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
        stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        stamped = encode_ttl_value(stamp, value_serialized)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
            key_serialized = self._serialize_key(key, prefix=prefix)
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
        stamp = self._compute_stamp(ttl=ttl, timestamp=timestamp)
        stamped = encode_ttl_value(stamp, value)
        super().set_bytes(key=key, value=stamped, prefix=prefix, cf_name="default")
        if stamp != SENTINEL_NEVER:
            key_serialized = self._serialize_key(key, prefix=prefix)
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
        # Mirror the base-class contract (spec item b): the flip hook runs before
        # ``super().prepare()``, so an exception here must set status=FAILED rather
        # than leave the transaction STARTED / reusable.
        try:
            self._maybe_flip_or_reject()
            self._sweep_expired_into_cache_if_enabled()
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise
        super().prepare(processed_offsets=processed_offsets)

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

    def _maybe_flip_or_reject(self) -> None:
        if not self._batch_has_ttl_writes:
            return
        if self._partition.uses_ttl_stamps:
            return
        if self._partition.main_cf_has_user_data():
            # §15.4 open-scope decision: memory parity is recovery-completion
            # ONLY (no live in-RAM backfill). A populated legacy memory store
            # arises solely from an all-legacy changelog replay; it re-recovers
            # from the changelog on every open, so migrating its pre-existing
            # records is owned by the recovery-completion path
            # (:meth:`MemoryStorePartition.complete_recovery`), not a live flip.
            # The §15 revision removed the reject, so we neither reject nor flip
            # here: staying legacy keeps the pre-existing raw records readable
            # verbatim (flipping without a backfill would mis-strip them). The
            # ttl= write lands as legacy this session; a changelog-driven flip on
            # a later open migrates the store.
            #
            # Row I-target (spec §7.3): this used to be a fully SILENT no-op. Emit
            # ONE warn-once WARNING so an operator sees that a ttl= write landed on
            # a populated legacy in-memory store and was stored un-stamped (the
            # migration is owned by the next changelog rebuild, not this write).
            if not self._partition._populated_legacy_warned:  # noqa: SLF001
                self._partition._populated_legacy_warned = True  # noqa: SLF001
                logger.warning(
                    "A state.set(..., ttl=...) write landed on a POPULATED LEGACY "
                    "in-memory store; the ttl= write is stored as a legacy "
                    "(un-stamped) record this session and no live in-RAM backfill "
                    "runs. The pre-existing legacy records migrate on the next "
                    "changelog rebuild via complete_recovery(). To assign a "
                    "uniform expiry to those legacy records, construct the store "
                    "with legacy_records_ttl=timedelta(...)."
                )
            return

        # Empty-store fast path: re-encode default-CF cache entries.
        cache = self._update_cache
        updates = cache.get_updates(cf_name="default")
        prefixes = list(updates.keys())
        for prefix in prefixes:
            entries = list(updates[prefix].items())
            for serialized_key, raw_value in entries:
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
        # Durable done-flag (spec §13.1), staged last so it is produced flag-last.
        # The replicated ``__ttl_system__`` CF is not local-only, so the base
        # ``_prepare`` produces it to the changelog and ``write()`` lands it in
        # the in-RAM dict — a cold rebuild then learns "done, never redo".
        cache.set(
            key=TTL_MIGRATION_DONE_KEY,
            value=int_to_bytes(STATE_FORMAT_VERSION),
            prefix=b"",
            cf_name=TTL_SYSTEM_CF_NAME,
        )
        self._partition.uses_ttl_stamps = True
        self._partition._state.setdefault(TTL_INDEX_CF_NAME, {})  # noqa: SLF001
        logger.info(
            "Flipping memory state partition into TTL mode (empty-store fast path)."
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
        # ``RocksDBPartitionTransaction._get_bytes`` (spec item c): only strip the
        # 8-byte stamp when it robustly looks like one. A value that does not
        # validate (raw legacy value, zero/out-of-range prefix) degrades to raw
        # (never-expires) rather than being mis-stripped or mis-expired, so reads,
        # the sweep, and recovery all agree.
        decoded = _safe_decode_stamp(raw_bytes)
        if decoded is None:
            # Warn once per PARTITION (spec item d), not per checkpoint tx.
            if not self._partition._unstamped_read_warned:  # noqa: SLF001
                self._partition._unstamped_read_warned = True  # noqa: SLF001
                logger.warning(
                    "Fail-safe TTL read: a flipped partition holds a value that "
                    "does not decode to a valid stamp (key prefix=%r) at "
                    "path=%s; returning it raw and treating it as never-expires "
                    "rather than stripping 8 bytes. This should be impossible on "
                    "a fully backfilled store and signals a pre-Fix-A store or "
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
