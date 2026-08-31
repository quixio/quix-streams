import logging
import os
import time
from datetime import timedelta
from threading import Event
from typing import (
    Dict,
    Iterator,
    List,
    Literal,
    NamedTuple,
    Optional,
    Union,
    cast,
)

from rocksdict import AccessType, ColumnFamily, Options, Rdict, ReadOptions, WriteBatch

from quixstreams.exceptions.base import QuixException
from quixstreams.kafka.exceptions import KafkaProducerDeliveryError
from quixstreams.models import HeadersMapping
from quixstreams.state.base import (
    PartitionTransactionCache,
    StorePartition,
)
from quixstreams.state.base.migration_flush import (
    MigrationDeliveryPhase,
    MigrationFlushVerdict,
    confirm_migration_delivery,
)
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    LOCAL_ONLY_CFS,
    METADATA_CF_NAME,
    TTL_ADOPT_BACKUP_CF_NAME,
    Marker,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.transaction import (
    RocksDBPartitionTransaction,
    _safe_decode_stamp,
    _ttl_to_ms,
)
from quixstreams.state.serialization import int_from_bytes, int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

from ..exceptions import ChangelogFlushError
from .exceptions import (
    IncompatibleStateStoreError,
    RocksDBCorruptedError,
    RocksDBOpenAborted,
)
from .metadata import (
    CHANGELOG_OFFSET_KEY,
    MIN_UPGRADEABLE_STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ADOPT_PENDING_KEY,
    TTL_BACKFILL_IN_PROGRESS_KEY,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_ROLLBACK_ENV_VAR,
    TTL_SYSTEM_CF_NAME,
)
from .open_deadline import OpenDeadline
from .options import RocksDBOptions
from .ttl_codec import (
    _MAX_PLAUSIBLE_STAMP_MS,
    SENTINEL_NEVER,
    TTL_STAMP_BYTES,
    clamp_additive_expiry,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)
from .types import RocksDBOptionsType

__all__ = ("RocksDBStorePartition",)


logger = logging.getLogger(__name__)

# Census-size threshold above which the in-memory key list is logged as a
# future spill-to-disk concern.
# At ~80 B per held key, this is ~240 MB — large enough to threaten a small
# container. The backfill still proceeds in memory; the warning only flags
# that a multi-million-key store should grow a disk-spill census in future.
_CENSUS_SPILL_WARN_THRESHOLD = 3_000_000

# Progress-based bounded flush for the backfill / recovery-completion changelog
# sites. Each stamped chunk must be confirmed
# on the changelog BEFORE its stamps land in the local DB, or a crash would leave
# the local store ahead of the changelog (a peer rebuild would then diverge). We
# flush in repeated slices and fail only when a full slice delivers ZERO messages
# (no progress) — measuring lack of progress, not total time — so a large chunk
# that legitimately needs more than one slice to deliver does not trip a spurious
# ``ChangelogFlushError``. A total slice cap bounds a pathological ever-shrinking
# trickle so the loop always terminates within the consumer's poll budget.
#
# One slice; kept below the 30 s producer poll interval
# (``quixstreams.kafka.producer.PRODUCER_POLL_TIMEOUT``).
_BACKFILL_CHANGELOG_FLUSH_SLICE_S: float = 25.0
# Runaway cap: max slices before aborting even if the backlog keeps shrinking.
# Invariant: MAX_SLICES × SLICE_S — the worst-case time the flush loop can
# block — must stay BELOW the default Kafka ``max.poll.interval.ms`` (300 s,
# ``quixstreams.app._default_max_poll_interval_ms``). The flush runs INLINE on
# the consumer thread (``prepare()`` / the recovery-completion loop), so a
# longer block would breach the poll budget and trigger a rebalance
# mid-migration. 10 × 25 s = 250 s keeps a safety margin under that budget.
# Zero-progress is the primary trip (a wedged broker aborts after ~2 slices);
# this cap only bounds a pathological ever-shrinking trickle, where
# abort-and-retry is the correct outcome — a healthy per-chunk flush (bounded
# by ``legacy_backfill_chunk_size``) finishes in one or two slices.
_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES: int = 10


class _PendingCensusSurvey(NamedTuple):
    """
    Result of ONE pass over the ``__ttl_backfill_pending__`` census — every
    store-level signal the cold auto-adopt decision (``complete_recovery``
    Branch B) branches on. Computed together so the decision inputs are
    mutually consistent by construction: ``covers_default_cf`` is only proven
    when ``all_stamped`` is True, because the same pass that proves the quorum
    also point-verifies a live default-CF value per censused key (census ⊆
    default keys), which is what turns the coverage COUNT equality into SET
    equality. ``pending_count`` / ``all_past`` / ``covers_default_cf`` are
    only meaningful when ``all_stamped`` is True (the survey short-circuits on
    the first quorum failure).
    """

    pending_count: int
    all_stamped: bool
    all_past: bool
    covers_default_cf: bool


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
        stop_event: Optional[Event] = None,
        open_deadline: Optional[OpenDeadline] = None,
    ):
        if not options:
            options = RocksDBOptions()

        super().__init__(options.dumps, options.loads, changelog_producer)
        self._path = path
        self._options = options
        self._stop_event = stop_event
        self._open_deadline = open_deadline
        self._rocksdb_options = self._options.to_options()
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._max_evictions_per_flush = self._options.max_evictions_per_flush
        # Opt-in for backfilling a populated legacy store on TTL enable.
        # ``None`` preserves the current reject-on-populated-store behavior;
        # a strictly positive ``timedelta`` enables the in-place backfill
        # (see :meth:`backfill_legacy_records`). Read by the transaction layer
        # through this partition in ``_maybe_flip_or_reject``.
        self._legacy_records_ttl: Optional[timedelta] = self._options.legacy_records_ttl
        # Number of pre-existing records re-stamped per write-batch during the
        # one-time legacy backfill. Bounds peak transient memory to one chunk
        # (see :meth:`backfill_legacy_records`). Only consulted on the single
        # backfilling flush.
        self._legacy_backfill_chunk_size: int = self._options.legacy_backfill_chunk_size
        # When True (default), TTL evictions are produced to the changelog as
        # tombstones (via the transaction cache at prepare-time) so compaction
        # reclaims expired keys in step with the local store; when False, the
        # eviction stays local-only (the pre-change ``_run_sweep``-in-``write()``
        # path). Read once at open, immutable thereafter.
        self._ttl_changelog_tombstones: bool = self._options.ttl_changelog_tombstones
        # Operational rollback lever for the COLD-heuristic provisional adoption,
        # read ONCE at open from the environment (modelled on the
        # ``QUIXSTREAMS_STATE_LOG_LEVEL`` pattern — transient, Portal-settable, not
        # a ``RocksDBOptions`` field). Governs ONLY the reversible cold path: on a
        # warm restart of a provisionally-adopted store it restores the originals
        # byte-identical; on a fresh volume it suppresses the cold provisional
        # adopt. Never touches the sound warm-deterministic path.
        self._ttl_rollback: bool = os.environ.get(TTL_ROLLBACK_ENV_VAR) == "1"
        self._db = self._init_rocksdb()
        self._cf_cache: Dict[str, Rdict] = {}
        self._cf_handle_cache: Dict[str, ColumnFamily] = {}
        self._high_water_ms: Optional[int] = None
        # Wallclock reference captured once per changelog-recovery session
        # (lazily, on the first stamped default-CF replay). Used to judge
        # whether a replayed TTL entry is already expired and to seed the
        # post-recovery high-water (see :meth:`recover_from_changelog_message`).
        # ``None`` means no
        # stamped message has been replayed yet in this partition's lifetime;
        # a fresh partition instance per assignment is a fresh recovery session.
        self._recovery_now_ms: Optional[int] = None
        # Event-time replay-drop frontier. Snapshot of the
        # partition's persisted event-time high-water (:attr:`high_water_ms`) as
        # it stood BEFORE any changelog record replayed, captured ONCE (lazily,
        # on the first stamped default-CF replay) and frozen for the whole
        # recovery session. The replay-drop decides expiry against THIS value
        # with the exact clock + condition the live read filter uses
        # (``transaction.py``: drop iff ``high_water_ms is not None and stamp <=
        # high_water_ms``), so recovery never mass-deletes a record the live path
        # would keep. Because ``recover_from_changelog_message`` never advances
        # high-water, the frontier cannot ratchet up from replayed stamps (the
        # collapse the old ``recovery_now = high_water`` per-record clock caused).
        # ``None`` is a VALID frontier (cold restore / fresh volume) meaning
        # "drop nothing"; the ``_recovery_frontier_captured`` latch distinguishes
        # "captured as None" from "not yet captured".
        self._recovery_frontier_ms: Optional[int] = None
        self._recovery_frontier_captured: bool = False
        # Count of event-time-frontier-expired stamped records dropped during this
        # recovery's changelog replay (the latest-record-wins recovery-drop
        # filter, :meth:`recover_from_changelog_message`). Surfaced as one aggregate INFO at
        # :meth:`complete_recovery` so an operator sees the deletions instead of
        # records silently vanishing across a recovery.
        self._recovery_expired_drops: int = 0
        # Incomplete-migration detection. Set True on the first
        # header-true default-CF replay (the same condition that flips the
        # partition into TTL mode). Combined with a non-empty
        # ``__ttl_backfill_pending__`` CF at end of recovery, it marks a MIXED
        # (incomplete-migration) changelog whose leftover legacy records must be
        # completed by :meth:`complete_recovery`. False (the all-legacy first-
        # enablement case) never triggers completion.
        self._recovery_saw_stamped: bool = False
        # Survivor-derived completion default. Tracks the MAX absolute
        # expiry among replayed default-CF records that are (i) stamped /
        # header-true, (ii) non-SENTINEL, and (iii) NOT dropped by the
        # latest-record-wins wallclock filter (i.e. still in the future at
        # rebuild). When an
        # incomplete migration is completed WITHOUT ``legacy_records_ttl`` in
        # config, the leftover legacy records inherit this expiry — aligning them
        # with the surviving siblings of their own backfill cohort. ``None`` = no
        # surviving future stamp was observed (the degenerate all-expired case),
        # in which case completion falls back to SENTINEL_NEVER + a WARN rather
        # than deriving a past expiry that would mass-delete on the next sweep.
        self._recovery_max_survivor_expiry_ms: Optional[int] = None
        # Durable done-flag latch. Set True when the replicated
        # ``__ttl_system__`` marker (``TTL_MIGRATION_DONE_KEY``) is replayed this
        # recovery session. It means the source store's migration completed, so
        # :meth:`complete_recovery` flips the partition, discards any pending
        # census, and runs NO backfill/completion — the migration is never redone
        # even across a full cold rebuild. Marker absent (all pre-marker
        # changelogs) leaves this False and the existing header/pending logic
        # applies unchanged (additive back-compat).
        self._recovery_saw_migration_done: bool = False

        # Warn-once guard for the fail-safe degraded TTL read.
        # Scoped to the PARTITION, not a single checkpoint transaction, so the
        # warning fires once per partition lifetime instead of once per flush
        # (a transaction-scoped flag re-warned on every checkpoint). Set by the
        # transaction read path when a flipped partition holds a value that does
        # not decode to a valid stamp.
        self._unstamped_read_warned: bool = False

        # Warn-once guard for an implausibly large event-time
        # timestamp ignored by :meth:`advance_high_water`. Partition-scoped (same
        # rationale as ``_unstamped_read_warned``) so a stream of mis-scaled
        # timestamps logs once, not once per record.
        self._high_water_warned: bool = False

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

        # Runtime mirror of the COLD-heuristic provisional-adoption marker
        # (``__ttl_adopt_pending__``). True == this store was provisionally
        # cold-adopted and is NOT yet corroborated: the pre-adoption originals live
        # in ``__ttl_adopt_backup__`` and the TTL sweep is suppressed until a
        # live ``ttl=`` write corroborates the adoption or an operator rolls back
        # via the rollback lever.
        # Loaded from disk so a warm restart of a provisional store re-arms the
        # guard. Always False for the sound warm-deterministic path (no marker) and
        # for a genuine legacy store. Set/cleared by
        # :meth:`_adopt_v3240_stamps` / :meth:`corroborate_adoption` /
        # :meth:`_rollback_provisional_adopt`.
        self._adopt_provisional: bool = class_uses_ttl_stamps and (
            self._load_adopt_pending_flag()
        )

        # Reconcile a corroboration whose LOCAL teardown was
        # interrupted by a crash. :meth:`corroborate_adoption` persists the durable
        # done-marker FIRST (changelog-first), THEN deletes the pending marker and
        # (post-commit-barrier) drops the backup CF in separate writes. Two crash
        # windows can strand local state even though corroboration already succeeded:
        #  - crash before the pending delete -> pending marker survives, which
        #    re-armed ``_adopt_provisional`` above and would pin the TTL sweep off
        #    forever;
        #  - crash after the pending delete but before the backup drop -> an orphaned
        #    ``__ttl_adopt_backup__`` CF lingers as dead state.
        # The done-marker is the durable proof of success and takes precedence over
        # either leftover: force the store out of provisional mode and finish the
        # interrupted teardown (delete pending + drop backup — both idempotent).
        # Runs BEFORE the rollback resolution below so a corroborated store
        # (done-marker present) is never rolled back. The marker probe is
        # read-only (an absent ``__ttl_system__`` CF means no marker and is never
        # created by the probe); the ``cfs_at_open`` check just short-circuits the
        # call. A cleanly-corroborated store (pending absent, backup already
        # dropped) skips the reconciliation.
        cfs_at_open = self.list_column_families()
        if (
            class_uses_ttl_stamps
            and TTL_SYSTEM_CF_NAME in cfs_at_open
            and (self._adopt_provisional or TTL_ADOPT_BACKUP_CF_NAME in cfs_at_open)
            and self._has_local_migration_done_marker()
        ):
            self._finish_interrupted_corroboration()

        # Warm/cold classification + rollback resolution, evaluated at
        # open BEFORE the persisted-flip snapshot. Three outcomes:
        #  1. rollback set on a provisionally cold-adopted store -> restore legacy;
        #  2. warm TTL artifacts present but the ``__ttl_enabled__`` flag is absent
        #     (a preview that kept ``__ttl_index__`` / a format marker without the
        #     flag) -> deterministic in-place flip (sound positive ID);
        #  3. otherwise leave the loaded flag as-is (already-flipped warm store, or
        #     a legacy / fresh-volume store whose cold census is handled at
        #     :meth:`complete_recovery`).
        if class_uses_ttl_stamps and self._adopt_provisional and self._ttl_rollback:
            # Warm restart of a cold-heuristic-adopted store with the
            # rollback lever set -> restore the originals byte-identical.
            self._rollback_provisional_adopt()
        elif (
            class_uses_ttl_stamps
            and not self.uses_ttl_stamps
            and self._has_warm_ttl_artifacts()
        ):
            # Sound warm signal without the enabled flag. Flip
            # deterministically and persist the flip so the store resumes TTL mode.
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            self._stamp_flip_metadata()
            logger.info(
                "Detected v3.24.0 TTL store at path=%s (local __ttl_index__ / TTL "
                "metadata present, no __ttl_enabled__ flag); resuming TTL mode "
                "deterministically.",
                self._path,
            )
            # A preview detected only via a format/high-water marker
            # may have no (or an empty) local __ttl_index__ while the default CF
            # holds stamped values. Rebuild it verbatim from those stamps so the
            # sweep can expire the records (else they are unindexed and never
            # expire). No-op when the index is already populated.
            self._rebuild_index_from_default_cf()

        # Remember whether the partition is
        # flipped on disk at open time (post warm/rollback resolution), before any
        # changelog replay could set the runtime flag. ``complete_recovery`` uses
        # this to treat an offset-caught-up restart (no stamped record replayed this
        # session, so ``_recovery_saw_stamped`` stays False) of an already-flipped
        # store as saw-stamped-equivalent: its ``__ttl_backfill_pending__`` census is
        # genuine leftovers, not orphans, so it must be COMPLETED rather than
        # discarded. An un-flipped (pure-legacy) store keeps ``False`` and still
        # discards an orphan census.
        self._persisted_flipped_at_open: bool = self.uses_ttl_stamps

        if self.uses_ttl_stamps:
            # Already-flipped (or just warm-flipped) store: validate/upgrade the
            # on-disk format and warm up the TTL bookkeeping (high-water, index CF).
            # For legacy stores (the 99% case) we skip every line of this block — no
            # extra CF is created, no extra metadata read happens beyond the single
            # ``__ttl_enabled__`` probe above.
            self._enforce_format_version()
            self._load_high_water()
            # Pre-create the index CF so writes never race a CF creation.
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            # Drop any dead live-backfill bookkeeping left by the migration that
            # flipped this store (one-time; no-op once cleaned).
            self._cleanup_completed_backfill_bookkeeping()

        # Snapshot ONCE, at the end of ``__init__`` and
        # AFTER the open-time ``_cleanup_completed_backfill_bookkeeping`` above,
        # whether this partition opened with a non-empty live-backfill ledger —
        # ONE HALF of the interrupted-live-backfill signature (the other half is
        # the ``__ttl_backfill_in_progress__`` marker snapshotted just below, which
        # covers the first-chunk window where no chunk committed and this ledger is
        # therefore still empty). Consulted by
        # :meth:`recover_from_changelog_message` to ledger a replayed crash-window
        # chunk so the resume census cannot double-wrap it.
        #
        # The ``__ttl_backfill_stamped__`` CF is a ``LOCAL_ONLY_CFS`` member (never
        # produced to the changelog), so a fresh-volume COLD restore always opens
        # with the CF absent → snapshot False → replay ledgers nothing → the
        # adopt / survivor-derived / offset-skip cold-restore paths are byte-for-byte
        # unchanged. Only an interrupted LIVE backfill (whose earlier chunks wrote
        # real ledger entries on this same volume) opens with the CF non-empty →
        # snapshot True. The ledger probe is read-only (an absent ledger CF is
        # treated as empty and is never created by the probe); the
        # ``list_column_families()`` guard just short-circuits the call — the
        # same ``list_cf`` call the cleanup paths already make.
        # Snapshot-at-open (not a live re-probe)
        # is deliberate: a live probe is self-fulfilling — the first ledgered
        # record would make every subsequent one ledger too, re-introducing the
        # cold-restore false positive the gate exists to prevent.
        self._ledger_nonempty_at_open: bool = (
            TTL_BACKFILL_STAMPED_CF_NAME in self.list_column_families()
            and self._live_backfill_ledger_has_any()
        )

        # Snapshot the durable ``__ttl_backfill_in_progress__`` marker at open —
        # the SECOND half of the interrupted-live-backfill signature, and the only
        # half that exists when the crash landed inside the FIRST chunk's
        # produce→commit window: nothing committed locally there, so the ledger
        # above is still EMPTY while that chunk is already durable on the
        # changelog. Consulted alongside ``_ledger_nonempty_at_open`` by
        # :meth:`recover_from_changelog_message` to ledger the replayed
        # crash-window records, which is what lets the ledger-driven resume in
        # :meth:`complete_recovery` fire at all — and what stops it re-wrapping
        # the records that just replayed.
        #
        # The marker lives in ``__metadata__`` (a ``LOCAL_ONLY_CFS`` member, never
        # produced to the changelog), so a fresh-volume COLD restore always opens
        # without it → snapshot False → replay ledgers nothing → the adopt /
        # survivor-derived / offset-skip cold-restore paths are byte-for-byte
        # unchanged. Only a live backfill that ran on THIS volume and never
        # finished leaves it set. Snapshot-at-open (not a live re-probe) mirrors
        # the ledger snapshot above and is taken AFTER the open-time
        # ``_cleanup_completed_backfill_bookkeeping``, which clears the marker on a
        # genuinely-completed migration.
        self._backfill_in_progress_at_open: bool = (
            class_uses_ttl_stamps and self._backfill_in_progress()
        )

        # Snapshot whether the durable migration done-marker
        # is already local at open (a warm restart of a fully-migrated store). When
        # True, changelog replay skips the pending-CF census entirely — the marker
        # means the migration is complete, so ``complete_recovery`` would discard
        # any censused entries anyway. The marker probe is read-only (an absent
        # ``__ttl_system__`` CF means no marker, never created by the probe); the
        # ``list_column_families()`` guard just short-circuits the call (parity
        # with the ledger snapshot above).
        # A fresh-volume COLD restore opens with the CF absent → False and
        # relies on the ``_recovery_saw_migration_done`` replay latch instead.
        self._migration_done_at_open: bool = (
            type(self).uses_ttl_stamps
            and TTL_SYSTEM_CF_NAME in self.list_column_families()
            and self._has_local_migration_done_marker()
        )

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

    @property
    def ttl_changelog_tombstones(self) -> bool:
        """
        Whether TTL evictions are produced to the changelog as tombstones
        (default ``True``) or kept local-only (``False`` — the pre-change sweep).
        Consulted by the transaction layer at prepare-time to decide the sweep
        path (see :meth:`sweep_expired_into_cache`).
        """
        return self._ttl_changelog_tombstones

    def advance_high_water(self, timestamp: Optional[int]) -> None:
        """
        Advance the partition's high-water mark monotonically. Called by the
        transaction layer on every TTL-aware ``set`` / ``get`` that carries a
        timestamp. Late-arriving timestamps never roll the high-water back.
        """
        if timestamp is None:
            return
        if timestamp < 0:
            # A negative timestamp (Kafka NO_TIMESTAMP = -1
            # or a pre-epoch event-time) never represents a real event-time
            # position, so it must not advance — or establish — the high-water.
            # Left unguarded it set a negative high-water that int_to_bytes (an
            # unsigned >Q packer) then failed to persist with a raw struct.error.
            return
        if timestamp >= _MAX_PLAUSIBLE_STAMP_MS:
            # An implausibly large event-time (e.g. a ns/µs
            # timestamp fed where epoch-ms is expected) must not poison the shared
            # high-water clock that drives the read-expiry filter AND the
            # destructive sweep (``now_ms = self._high_water_ms``). Symmetric with
            # the write-side reject in ``transaction.py::_compute_stamp``; IGNORE
            # (do not advance) rather than raise — matching the negative-timestamp
            # guard above — so a stray read / no-ttl write can neither crash nor
            # mass-evict every other still-valid record. Real event-times (~1.7e12)
            # are far below the 1e15 cap, so valid data is unaffected.
            if not self._high_water_warned:
                logger.warning(
                    "Ignoring implausibly large event-time timestamp %d (>= %d) "
                    "for the TTL high-water at path=%s; advancing it would poison "
                    "the read-expiry filter and the sweep. Check for a mis-scaled "
                    "(nanosecond/microsecond) timestamp on a state read/write.",
                    timestamp,
                    _MAX_PLAUSIBLE_STAMP_MS,
                    self._path,
                )
                self._high_water_warned = True
            return
        if self._high_water_ms is None or timestamp > self._high_water_ms:
            self._high_water_ms = timestamp

    def _now_ms(self) -> int:
        """
        Current wallclock time in epoch milliseconds. Isolated behind a method
        purely as a test seam so changelog-recovery determinism cases can
        inject a fixed ``now`` without sleeping.
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

        # Done-flag consumption. The replicated ``__ttl_system__``
        # CF carries a single reserved marker produced LAST when a migration
        # completes — base ``_prepare`` orders the system CF after all others on
        # the live flip path, and the recovery-completion / backfill paths sequence
        # the marker produce after the last stamped record. Seeing it means the
        # source store was definitively
        # TTL-enabled AND fully migrated: latch it so ``complete_recovery`` flips,
        # discards any pending census, and NEVER re-runs the backfill (idempotent
        # "never redo"). This is the clean structural signal that closes the
        # stock-v3.24.0 mis-classification class for every store written after the
        # marker landed. The marker record itself still lands verbatim in the
        # local ``__ttl_system__`` CF below (replicated redundancy for warm opens).
        # Gated on the class-level flag so windowed/timestamped opt-outs ignore it.
        if (
            type(self).uses_ttl_stamps
            and cf_name == TTL_SYSTEM_CF_NAME
            and key == TTL_MIGRATION_DONE_KEY
        ):
            self._recovery_saw_migration_done = True

        # Recovery flip-discovery.
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
        # epoch-ms values and dropped them. Header absent → the
        # record is legacy / un-stamped and replays verbatim below, so a purely
        # legacy changelog never latches (the requirement that a purely legacy
        # changelog stays legacy).
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

        # Incomplete-migration detection / census. The
        # stamped-vs-legacy decision is per-record on the ``ttl_stamped`` header,
        # NOT on the latched ``uses_ttl_stamps`` flag: a MIXED changelog replays
        # header-absent legacy records AFTER the partition has flipped, and those
        # must still land verbatim (never re-wrapped) and be censused into
        # ``__ttl_backfill_pending__`` for the completion backfill. The pending
        # bookkeeping rides the SAME WriteBatch as the default-CF write so it is
        # atomic with the replay.
        if (
            type(self).uses_ttl_stamps
            and cf_name == "default"
            and not (self._migration_done_at_open or self._recovery_saw_migration_done)
        ):
            # Once the migration done-marker is known — local at open (warm
            # restart) or latched the moment the marker record replays (produced
            # last, so it arrives after every record it certifies) —
            # stop censusing pending-CF entries. ``complete_recovery`` discards /
            # no-ops the census in that case, so the entries would be dropped
            # anyway; skipping avoids the wasted per-record census writes. The
            # gate's else-branch (no marker) is unchanged → byte-identical
            # classification for stores without the marker.
            pending_handle = self.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)
            if ttl_stamped:
                # A header-true default-CF record. Mark that the partition
                # replayed at least one stamped record (the MIXED-detection
                # half) and drop any earlier legacy census entry for this key —
                # a later stamped write supersedes it (compaction ordering).
                self._recovery_saw_stamped = True
                batch.delete(key, pending_handle)
                if self._ledger_nonempty_at_open or self._backfill_in_progress_at_open:
                    # This partition opened carrying the
                    # interrupted-live-backfill signature: a non-empty
                    # ``__ttl_backfill_stamped__`` ledger, OR the durable
                    # ``__ttl_backfill_in_progress__`` marker that a live
                    # :meth:`backfill_legacy_records` arms before its first
                    # chunk's produce.
                    # A crash between a chunk's changelog flush-confirm and its
                    # local write leaves that chunk on the changelog but absent
                    # from the ledger — so replaying it here would otherwise leave
                    # the census invariant (``on-disk − staged − ledger``) broken
                    # and the resume would re-census and DOUBLE-WRAP the (now
                    # already-stamped) key. The MARKER term is what covers the
                    # FIRST chunk's window specifically, where the ledger is still
                    # EMPTY because no chunk ever committed locally: ledgering the
                    # replayed keys here is what makes :meth:`complete_recovery`'s
                    # ledger-driven resume branch fire for that window, and drives
                    # it over the correct (not-yet-stamped) complement.
                    # Ledger the replayed key in the SAME
                    # WriteBatch as the value apply + index rebuild + offset
                    # advance (committed at the end of this method), restoring the
                    # invariant atomically. Idempotent (re-ledgering an existing
                    # member is a no-op put); never inspects value content. Both
                    # terms are LOCAL_ONLY facts about THIS volume, so a cold
                    # restore on a fresh volume has neither → gate False → skipped
                    # entirely.
                    batch.put(
                        key,
                        b"",
                        self.get_column_family_handle(TTL_BACKFILL_STAMPED_CF_NAME),
                    )
            elif value is not None:
                # A header-absent (legacy) default-CF record. Census the key as a
                # leftover-legacy candidate; it lands verbatim below.
                batch.put(key, b"", pending_handle)
            else:
                # A header-absent TOMBSTONE. The verbatim replay below deletes
                # the key from the default CF, so any earlier census entry for
                # it must be removed too (symmetric with the stamped-supersession
                # delete above, riding the same atomic WriteBatch). Invariant:
                # the pending census tracks only keys LIVE in the default CF —
                # a censused-but-deleted key would fail the store-wide adoption
                # quorum (``_all_pending_values_are_stamped`` point-gets every
                # censused key and fails on the first missing value), blocking
                # auto-adopt for an otherwise fully-censused v3.24.0 store.
                batch.delete(key, pending_handle)

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
            # (MIXED changelog). It MUST land verbatim (no stamp wrap, no
            # index entry, no recovery-drop filter): its key was just censused
            # into the pending CF and the completion backfill
            # (:meth:`complete_recovery`) will stamp it at end of recovery.
            # Routing here on the per-record header — not on the latched flag —
            # is what keeps the leftover legacy records intact.
            batch.put(key, value, cf_handle)
        elif is_main_cf:
            stamped, stamp = self._normalize_replay_value(value)
            # Judge expiry against the EVENT-TIME frontier — the partition's
            # persisted high-water snapshotted ONCE before replay — using the
            # exact clock and condition the live read filter uses
            # (``transaction.py``: drop iff ``high_water_ms is not None and
            # stamp <= high_water_ms``). Judging by event-time matters: the old
            # wallclock drop deleted records whose event-time expiry stamp was
            # behind wallclock even though the live filter (event-time) would
            # keep them, mass-deleting a lagging store's dedup set on cold
            # restore.
            #   - WARM restore: the frontier is the loaded persisted high-water;
            #     a record at/below it would also be hidden by the live read and
            #     reclaimed by the sweep, so dropping it here is consistent and
            #     bounds memory.
            #   - COLD restore / store that flips mid-replay: high-water is
            #     ``None`` -> frontier ``None`` -> NOTHING is dropped, identical
            #     to the live filter; genuinely-expired records are reclaimed by
            #     the first post-recovery sweep once live events advance
            #     high-water past their stamps.
            # The frontier is FROZEN for the session (replay never advances
            # high-water) and captured via a distinct latch (``None`` is a valid
            # frontier), so it can never ratchet up from the replayed stamps (the
            # collapse the old ``recovery_now = high_water`` per-record clock
            # caused). ``_recovery_now_ms`` is still captured here, UNCHANGED, for
            # the migration-completion / adoption / heuristic wallclock consumers;
            # it no longer drives the replay drop. Sentinel-stamped entries are
            # never compared and always survive.
            expired = False
            if stamp != SENTINEL_NEVER:
                if self._recovery_now_ms is None:
                    self._recovery_now_ms = self._now_ms()
                if not self._recovery_frontier_captured:
                    self._recovery_frontier_ms = self._high_water_ms
                    self._recovery_frontier_captured = True
                frontier = self._recovery_frontier_ms
                expired = frontier is not None and stamp <= frontier
            if expired:
                # Already-expired against the event-time frontier (matches the
                # live read filter; latest-record-wins). A compacted changelog
                # can carry several
                # pre-compaction copies of one key; an OLDER copy of ``key``
                # replayed earlier this session (a verbatim header-absent legacy
                # value, or an older unexpired stamped copy) may already sit in
                # the main CF. Skipping (the old bare ``pass``) let that stale
                # copy survive — and in the MIXED shape its pending census entry
                # was just deleted above, so ``complete_recovery`` could never
                # repair it, resurrecting the expired record as a never-expiring
                # unswept legacy value. Explicitly DELETE the key so this newest
                # (expired) copy supersedes any older survivor; the index write
                # is still skipped (a dropped entry indexes nothing). We do NOT
                # try to delete an older copy's __ttl_index__ pointer — its stamp
                # is unknown here; the sweep's ghost/orphan handling GCs any index
                # entry whose main-CF key is gone (see _run_sweep).
                batch.delete(key, cf_handle)
                self._recovery_expired_drops += 1
            else:
                batch.put(key, stamped, cf_handle)
                if stamp != SENTINEL_NEVER:
                    index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
                    batch.put(encode_index_key(stamp, key), b"", index_handle)
                    # This is a surviving (future, non-sentinel) stamp on
                    # a header-true record — a candidate source for the leftover
                    # completion expiry when config is absent. Track the max.
                    if (
                        self._recovery_max_survivor_expiry_ms is None
                        or stamp > self._recovery_max_survivor_expiry_ms
                    ):
                        self._recovery_max_survivor_expiry_ms = stamp
        else:
            batch.put(key, value, cf_handle)

        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def complete_recovery(self) -> None:
        """
        Recovery-finalize hook. Called once by the recovery manager
        after this partition has replayed its changelog up to the high-watermark
        and before it is handed to live processing.

        Completes an **interrupted legacy-TTL migration**. During replay a MIXED
        changelog (some ``__ttl_stamped__``-header records + some header-absent
        legacy records) flips the partition into TTL mode on the first stamped
        record (so ``_recovery_saw_stamped`` is True) and lands the leftover
        legacy records verbatim while censusing their keys into
        ``__ttl_backfill_pending__``. Those leftovers are otherwise stranded as
        never-expiring forever (the live ``ttl=`` write sees an already-flipped
        partition and the backfill gate short-circuits).

        Trigger (only the MIXED shape):

        - if NOT ``_recovery_saw_stamped`` → all-legacy first-enablement;
          the live first-``ttl=``-write backfill owns it. No-op.
        - if the pending CF is empty AND no live backfill is durably marked in
          flight (``__ttl_backfill_in_progress__`` absent) → all-stamped /
          fully-migrated; nothing to complete beyond recording the done-marker.
          With that marker still armed the empty census proves nothing (a crash in
          the first backfill chunk's produce→commit window censuses nothing at
          all), so the done-marker is NOT latched — see the guard on that branch.
        - else (stamped seen AND pending non-empty) → incomplete migration;
          **auto-finish** (revised from the removed reject):
          chunk-backfill exactly the pending keys, stamping each with a uniform
          ``expires_at_ms``, writing the ``__ttl_index__`` entry, producing a
          header-bearing stamped record to the changelog, and deleting the key
          from the pending CF as the chunk commits (the delete IS the durable
          progress cursor). The uniform expiry is:
            - ``legacy_records_ttl`` set → ``self._recovery_now_ms +
              _ttl_to_ms(legacy_records_ttl)`` (wallclock-at-rebuild;
              explicit config unchanged and always wins);
            - ``legacy_records_ttl`` absent → the survivor-derived expiry
              ``self._recovery_max_survivor_expiry_ms`` (a future stamp shared
              with the leftovers' backfill cohort), or ``SENTINEL_NEVER`` +
              a WARN in the degenerate all-expired case (no surviving future
              stamp). This replaces the removed config-absent reject — a rebuilt
              node cannot know the original flip ttl (it lived in a LOCAL_ONLY_CF
              never on the changelog), so it derives a safe finite value from the
              replayed cohort instead of erroring.

        Un-gated by the live flip flag (the partition is already flipped). Idempotent
        and convergent across interrupts: an interrupted run leaves the still-pending
        keys in the CF; the next cold restore rebuilds pending from the (now-more-
        stamped) changelog and resumes over exactly the remainder.
        """
        if self._recovery_expired_drops > 0:
            # Make the event-time-frontier replay drops observable — ONE
            # aggregate INFO per partition per recovery (no per-record logging).
            # Emitted at the once-per-recovery finalize seam before any early
            # return; a non-zero count implies the partition flipped, so no
            # early-return branch below can skip a non-zero log. A cold restore
            # has frontier None and 0 drops, so this does not fire there.
            logger.info(
                "Recovery at path=%s dropped %d already-expired stamped record(s) "
                "during changelog replay (expired against the recovery event-time "
                "frontier high_water=%d ms; matches the live read filter).",
                self._path,
                self._recovery_expired_drops,
                self._recovery_frontier_ms or 0,
            )
        if not type(self).uses_ttl_stamps:
            # A subclass opted out (windowed / timestamped): the class-level
            # census gate never fired, so there is nothing to complete or clean.
            return
        if self._recovery_saw_migration_done:
            # Durable done-flag present: the source store was
            # definitively TTL-enabled and its backfill completed, so the
            # migration is NEVER redone. Ensure the partition is flipped (the
            # marker can outlive, via compaction, the stamped records that would
            # otherwise flip it), discard any pending census, and run no
            # backfill/completion. This is the "survives reconstruction"
            # guarantee for post-marker stores.
            if not self.uses_ttl_stamps:
                self.uses_ttl_stamps = True
                self.get_or_create_column_family(TTL_INDEX_CF_NAME)
                self._stamp_flip_metadata()
            # Done-marker index rebuild: a cold restore of a CORROBORATED
            # cold-adopted store replays the header-absent adopted records (they
            # were never re-produced as header-true) plus the replicated done-marker.
            # Their ``__ttl_index__`` is LOCAL_ONLY and does not survive a fresh
            # volume, so rebuild it verbatim from the still-censused all-stamped
            # records before discarding the census — else the adopted records would
            # never expire. Self-distinguishing: a completed legacy->TTL migration
            # drains its census to empty on replay, so the gate is False there (and
            # a partial / non-stamp census is likewise skipped), leaving that path
            # byte-identical.
            if self._all_pending_values_are_stamped():
                self._rebuild_index_from_stamped_census()
            # This is the only census-discard path that was otherwise silent.
            # Log a sibling-consistent INFO with the discarded count before the
            # drop (unconditionally — count 0 is a useful "nothing to discard"
            # signal, matching the other discard paths that log even at 0).
            logger.info(
                "Recovery at path=%s: durable migration done-marker present; "
                "discarding %d orphan pending-census entry(ies) (store fully "
                "migrated, no completion needed).",
                self._path,
                self._count_backfill_pending(),
            )
            self._discard_backfill_pending()
            return
        if self.uses_ttl_stamps and self._live_backfill_ledger_has_any():
            # Warm-restart resume — evaluated immediately AFTER the
            # done-flag short-circuit and BEFORE the all-stamped byte gate
            # below (definitive ledger evidence beats the byte
            # heuristic). The store is flipped and holds a non-empty
            # ``__ttl_backfill_stamped__`` ledger with NO done-marker (the
            # done-marker case already returned above): the exact signature of an
            # in-place live :meth:`backfill_legacy_records` that was interrupted
            # and then flipped via changelog replay on a warm restart, leaving
            # un-stamped legacy leftovers below the replayed offset range that were
            # never censused. Resume the
            # backfill over the ledger complement and finish the migration.
            #
            # The ledger reaches here two ways, and the complement is the correct
            # key set for both: chunks that COMMITTED locally ledgered themselves,
            # and — when the crash landed in a chunk's produce→commit window, up to
            # and including the FIRST chunk (nothing committed, ledger empty at
            # open) — :meth:`recover_from_changelog_message` ledgered the replayed
            # crash-window records under the ``__ttl_backfill_in_progress__``
            # marker. Either way every already-stamped key is a ledger member and
            # is excluded from the resumed census, so nothing is double-wrapped.
            #
            # Ordered ABOVE the all-stamped gate on purpose: a warm restart
            # re-replays the stored
            # offset INCLUSIVELY, so one boundary header-absent legacy record can
            # be re-censused into ``__ttl_backfill_pending__`` even while the ledger
            # is non-empty — the ledger and the pending census are NOT mutually
            # exclusive on a warm restart. If that lone orphan's value happened to
            # byte-decode as a plausible stamp, the all-stamped gate below would
            # fire first
            # and permanently strand the interrupted backfill (CRITICAL + discard,
            # no resume, ledger kept → re-strands on every restart). This resume
            # branch
            # cannot hijack a cold-restore census case: the ledger is a LOCAL_ONLY
            # CF, absent on a fresh volume (where adopt / survivor-derived live),
            # so a
            # non-empty ledger only ever means a live backfill ran on THIS volume.
            self._resume_interrupted_live_backfill()
            return
        # Discriminate an interrupted THIS-BRANCH
        # completion from opt-in v3.24.0 adoption using the flip latches plus the
        # one store-level all-past heuristic. Per-record byte routing stays banned.
        if not (self._recovery_saw_stamped or self._persisted_flipped_at_open):
            # BRANCH B — no this-branch evidence: the store is genuinely unflipped,
            # so it is pure-legacy (v3.23.6) OR a stock v3.24.0 cold restore. The
            # v3.24.0-stamp adoption is now AUTOMATIC and REVERSIBLE:
            # a 100%-stamped, not-all-past census is provisionally adopted with a
            # backup + sweep-guard instead of logging a CRITICAL and staying legacy.
            # Every decision input (census size, stamp quorum, all-past, coverage)
            # comes from ONE census pass so the inputs are mutually consistent.
            survey = self._survey_backfill_pending()
            if survey.all_stamped:
                if self._ttl_rollback:
                    # The operational rollback lever suppresses the
                    # cold provisional adopt on a fresh volume — stay legacy,
                    # quarantine the census (byte-identical), WARN.
                    logger.warning(
                        "Recovery at path=%s: cold v3.24.0 auto-adopt SUPPRESSED by "
                        "QUIXSTREAMS_STATE_TTL_ROLLBACK=1; the store stays in legacy "
                        "mode, every value reads back byte-identical, and the %d "
                        "censused key(s) are quarantined (unset the env var and "
                        "restart to re-enable auto-adopt).",
                        self._path,
                        survey.pending_count,
                    )
                    return
                if survey.all_past:
                    # QUARANTINE (downgraded from CRITICAL to WARN): every censused
                    # stamp is already in the past — the exact shape of a legacy
                    # set_bytes() dedup store (past epoch-ms). Adopting would rebuild
                    # the index with past stamps and the next sweep would DELETE
                    # every record, so refuse. Stay legacy, preserve the census as
                    # the repair vector, read back byte-identical.
                    logger.warning(
                        "Refused auto-adopt at path=%s: all %d censused stamp(s) are "
                        "already in the past (legacy dedup shape); the store stays "
                        "legacy, byte-identical, and the census is preserved "
                        "(quarantined). If this really is a v3.24.0 store, re-seed "
                        "the state from source.",
                        self._path,
                        survey.pending_count,
                    )
                    return
                if not survey.covers_default_cf:
                    # Completeness invariant: adoption flips read semantics
                    # STORE-WIDE (once ``uses_ttl_stamps`` is set, every
                    # default-CF read strips a leading 8-byte stamp), so the
                    # census the decision is based on must cover every key the
                    # flip will affect. A warm restart behind its changelog
                    # replays only the TAIL, censusing a strict SUBSET of the
                    # default CF — flipping off that subset would make every
                    # NON-censused key read 8 bytes short with no
                    # ``__ttl_adopt_backup__`` entry to restore from. Stay
                    # legacy, byte-identical; preserve the census (quarantine,
                    # parity with the refusal exits above). A preserved census
                    # can never cause a later false adopt: any future decision
                    # re-proves both the quorum and this coverage against the
                    # then-current default CF.
                    logger.warning(
                        "Refused cold v3.24.0 auto-adopt at path=%s: the pending "
                        "census (%d key(s)) does not cover the whole default CF "
                        "— a partial changelog replay (e.g. a warm restart "
                        "behind the changelog) censuses only the replayed tail "
                        "and cannot prove the non-censused keys are "
                        "v3.24.0-stamped. The store stays legacy, every value "
                        "reads back byte-identical, and the census is preserved "
                        "(quarantined). A full cold rebuild (fresh state volume "
                        "/ clear_state) replays and censuses the complete "
                        "changelog and will auto-adopt safely.",
                        self._path,
                        survey.pending_count,
                    )
                    return
                # Not-all-past, 100%-stamped, full-coverage census: provisional
                # (reversible) auto-adopt.
                self._adopt_v3240_stamps()
                return
            # Sub-100% "looks-like": a v3.24.0 store would be 100% stamped, so a
            # sub-100% census is genuine legacy and must not leave a persistent
            # census burdening a pure-legacy store. Discard after the heuristic WARN.
            self._warn_if_looks_like_v3240_upgrade()
            self._discard_backfill_pending()
            return

        # BRANCH A — this-branch evidence present (saw_stamped or persisted-flipped
        # at open): the census is interrupted-migration leftovers → completion.
        if not self.uses_ttl_stamps:
            # Defensive: a stamped record was seen but the partition is somehow
            # not flipped (the same header flips it, so this is unreachable in
            # practice). Do not run completion on an unflipped partition.
            return

        pending_count = self._count_backfill_pending()
        if pending_count == 0:
            if self._backfill_in_progress():
                # DEFENSIVE: the census is empty AND the ledger is empty (the
                # resume branch above already tested it and fell through), yet a
                # live backfill is durably marked in flight. An empty census
                # cannot be read as "fully migrated" here, so do NOT latch the
                # done-marker — latching it would permanently strand any
                # un-stamped legacy leftover on a flipped store with "never redo".
                #
                # Nothing is repaired either: the ledger is the only sound driver
                # for a resume (its complement is exactly the not-yet-stamped
                # keys), and an empty one would drive the resume over the WHOLE
                # default CF and double-wrap anything already stamped. Refusing to
                # lie is non-destructive and leaves the store re-completable; a
                # value-sniffing full-CF scan is not an option on this path.
                #
                # This should be unreachable: the ledger is written in the same
                # WriteBatch as every value the backfill stamps, and while this
                # marker is set changelog replay ledgers every header-true
                # default-CF record it applies — so a flipped store with this
                # marker cannot have an empty ledger unless something stamped a
                # value outside both paths.
                logger.warning(
                    "Recovery at path=%s: the __ttl_backfill_pending__ census is "
                    "empty but __ttl_backfill_in_progress__ is set with an EMPTY "
                    "__ttl_backfill_stamped__ ledger — an interrupted live "
                    "backfill with no usable resume cursor. NOT producing the "
                    "migration-done marker (it would latch 'done, never redo' "
                    "over any un-stamped leftover); leaving the store "
                    "re-completable. Re-seed the state from source if reads look "
                    "truncated.",
                    self._path,
                )
                return
            # Fully-migrated MIXED changelog: the census drained to empty during
            # replay and no live backfill is durably in flight (the guard above),
            # so the empty census really does mean "nothing left to stamp". The
            # migration IS complete but no done-marker was ever
            # produced, so every future cold restore would re-walk the census.
            # Produce the marker now to record "done, never redo".
            # Best-effort: this session stamped nothing, so a failed marker flush
            # must NOT fail recovery — it only forgoes the optimization (the next
            # restart re-derives the same empty census and retries the marker).
            try:
                self._produce_migration_done_marker()
            except (ChangelogFlushError, KafkaProducerDeliveryError):
                # The marker routes through
                # ``InternalProducer.produce()`` / ``flush()``, which raise
                # ``KafkaProducerDeliveryError`` (NOT ``ChangelogFlushError``) on a
                # latched delivery error from a sibling partition on the shared
                # migration producer. Both must be swallowed here: this best-effort
                # branch stamped nothing this session, so a failed marker must NOT
                # fail recovery — it only forgoes the optimization (the next restart
                # re-derives the same empty census and retries). Widened to the
                # empty-census branch ONLY; the critical backfill/completion paths
                # keep propagating.
                logger.warning(
                    "Recovery at path=%s: state fully migrated (empty pending "
                    "census) but the done-marker changelog flush/delivery failed; "
                    "leaving the store unmarked. Recovery continues; the marker "
                    "will be retried on the next restart.",
                    self._path,
                )
            return

        if (
            self._all_pending_values_are_stamped()
            and not self._pending_all_stamps_in_past()
        ):
            # Future-stamped ambiguous census on an ALREADY-FLIPPED store: the
            # header-absent all-8-byte leftovers are either genuine legacy that
            # completion should wrap once OR already-stamped v3.24.0 that completion
            # would DOUBLE-WRAP — byte-indistinguishable.
            if self._legacy_records_ttl is None:
                # Branch-A reconciliation: no explicit
                # wrap-once override, so keep the values VERBATIM via the reversible
                # provisional adopt (backup + sweep-guard + corroboration) instead
                # of HALTing. ``legacy_records_ttl`` remains the explicit "wrap once"
                # completion override (the fall-through below when it is set).
                if self._ttl_rollback:
                    # The rollback lever suppresses the (cold-heuristic)
                    # auto-adopt; keep the leftovers verbatim, quarantine the census.
                    logger.warning(
                        "Recovery at path=%s: ambiguous flipped all-stamped census "
                        "auto-adopt SUPPRESSED by QUIXSTREAMS_STATE_TTL_ROLLBACK=1; "
                        "the %d leftover value(s) are kept verbatim and the census "
                        "is quarantined. Set legacy_records_ttl to complete as a "
                        "legacy migration, or unset the env var to keep the v3.24.0 "
                        "stamps.",
                        self._path,
                        self._count_backfill_pending(),
                    )
                    return
                # No census-completeness gate here (unlike Branch B): the
                # partition is ALREADY flipped on independent evidence (the
                # persisted flag or a header-true replay), so this adopt cannot
                # change read semantics for any non-censused key — it only
                # backs up / indexes the censused leftovers verbatim.
                self._adopt_v3240_stamps()
                return
            # else: legacy_records_ttl is set -> the operator asserted legacy-
            # migration intent; fall through to the completion derivation below.

        legacy_records_ttl = self._legacy_records_ttl
        if legacy_records_ttl is not None:
            # Explicit config wins (unchanged): wallclock-at-rebuild + ttl.
            # ``_recovery_now_ms`` was captured on the first stamped default-CF
            # replay (exactly when ``_recovery_saw_stamped`` was set), so it is
            # normally populated here; capture defensively if a stamped record was
            # seen but no non-sentinel stamp ever set it. The recovery
            # wallclock is used ONLY to derive this recovery-completion expiry, it
            # is NOT seeded into the live ``_high_water_ms`` clock (that seed is
            # removed so an event-time-lagging workload never over-expires its own
            # post-recovery writes).
            if self._recovery_now_ms is None:
                self._recovery_now_ms = self._now_ms()
            # Clamp the ADDITIVE sum: mirror the backfill /
            # per-write bound so a large legacy_records_ttl cannot push the
            # recovery-completion expiry ``>= _MAX_PLAUSIBLE_STAMP_MS`` and strand
            # the leftover records as unreadable over-range stamps. Over-range
            # clamps to never-expire (readable, never mass-deleted).
            raw_expiry_ms = self._recovery_now_ms + _ttl_to_ms(legacy_records_ttl)
            expires_at_ms = clamp_additive_expiry(raw_expiry_ms)
            if expires_at_ms != raw_expiry_ms:
                logger.warning(
                    "Recovery-completion expiry wallclock(%d) + legacy_records_ttl "
                    "= %d exceeds the maximum readable stamp (%d) at path=%s; "
                    "clamping to never-expire (SENTINEL) so the leftover legacy "
                    "record(s) stay readable.",
                    self._recovery_now_ms,
                    raw_expiry_ms,
                    _MAX_PLAUSIBLE_STAMP_MS,
                    self._path,
                )
            logger.info(
                "Recovery: completing interrupted legacy-TTL migration at "
                "path=%s; %d leftover legacy record(s) will be stamped with "
                "expiry=%d (wallclock-at-rebuild + legacy_records_ttl).",
                self._path,
                pending_count,
                expires_at_ms,
            )
        else:
            # Config absent: derive a uniform expiry from the surviving stamped
            # cohort, CLAMPED against the recovery clock —
            # the survivor-derived value can be in the PAST. On an
            # offset-caught-up warm restart after downtime longer than the TTL
            # window there is NO replay, so ``_recovery_max_survivor_expiry_ms`` is
            # unset and the fallback is the max ON-DISK ``__ttl_index__`` stamp,
            # which the downtime may have carried into the past. Stamping the
            # leftovers with a past expiry would mass-delete them on the next sweep
            # (forbidden — Quix Cloud has no state reset), while the SAME changelog
            # cold-restored yields SENTINEL_NEVER (kept forever) — nondeterministic
            # opposite outcomes. The reference clock is the recovery wallclock, the
            # same clock the replay latest-record-wins drop filter used, so warm
            # and cold restores of one changelog converge; over-clamping to
            # SENTINEL_NEVER only ever over-keeps (safe), never deletes.
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
                # A genuine FUTURE survivor stamp: align the leftovers with the
                # max surviving future stamp of their backfill cohort.
                expires_at_ms = survivor_expiry
                logger.warning(
                    "Recovery: completing interrupted legacy-TTL migration at "
                    "path=%s WITHOUT legacy_records_ttl configured; %d leftover "
                    "legacy record(s) will be stamped with expiry=%d, derived "
                    "from the max surviving future stamp of their backfill cohort "
                    "(the original flip ttl is not recoverable on a cold "
                    "restore). This completion produces the migration-done marker "
                    "at the end, which blocks re-entry, so the derived window "
                    "cannot be changed after the fact.",
                    self._path,
                    pending_count,
                    expires_at_ms,
                )
            else:
                # No surviving stamp, OR the only derivable expiry (a past
                # ``_max_index_stamp_ms()`` on a warm restart after downtime, or
                # an all-expired cohort) is already <= the recovery clock. Keep
                # the leftovers never-expiring rather than stamping a past expiry
                # the next sweep would delete.
                expires_at_ms = SENTINEL_NEVER
                logger.warning(
                    "Recovery: completing interrupted legacy-TTL migration at "
                    "path=%s WITHOUT legacy_records_ttl configured; the only "
                    "available expiry to derive from is already in the past "
                    "relative to the recovery clock (%d), so stamping the %d "
                    "leftover legacy record(s) with it would delete them on the "
                    "next sweep. Retaining them as never-expiring (SENTINEL_NEVER) "
                    "to honor the never-mass-delete guarantee. NOTE: these records "
                    "cannot be retroactively re-stamped — the migration-done "
                    "marker produced at the end of this completion blocks "
                    "re-entry — so they next carry a finite expiry only when the "
                    "application overwrites those keys with an explicit ttl=.",
                    self._path,
                    now,
                    pending_count,
                )
        completed = self._complete_pending_backfill(
            expires_at_ms=expires_at_ms,
            chunk_size=self._legacy_backfill_chunk_size,
            total_pending=pending_count,
        )
        # Flag-last: the migration is now complete, so produce +
        # persist the durable done-flag marker AFTER the last stamped record. A
        # crash before this leaves the store re-completable from the (now-more-
        # stamped) changelog; a subsequent restore that sees the marker never
        # re-enters completion.
        self._produce_migration_done_marker()
        logger.info(
            "Recovery: completed legacy-TTL migration at path=%s; stamped %d "
            "leftover record(s); __ttl_backfill_pending__ is now empty.",
            self._path,
            completed,
        )

    def _produce_migration_done_marker(self) -> None:
        """
        Produce + persist the durable "migration done" marker.

        **Changelog-first ordering** — mirroring :meth:`_flush_backfill_changelog`
        and the invariant it enforces everywhere else: the marker is produced to
        the changelog and its delivery confirmed with a bounded flush BEFORE it is
        written to the local ``__ttl_system__`` CF. So the local store never
        records "migration done" ahead of the changelog:

        - a failed / timed-out flush raises :class:`ChangelogFlushError`, leaving
          the store **unmarked** so the next completion retries the marker;
        - a crash AFTER the flush but before the local write is safe — the marker
          replays from the changelog on the next recovery and re-latches
          ``_recovery_saw_migration_done`` (idempotent, same reserved key).

        The metadata flip flag is local-only (lost on a fresh volume), so the
        changelog-carried marker is what a cold rebuild learns "TTL enabled +
        backfill done, never redo" from. Used by the recovery-completion path; the
        live-enable paths instead stage the marker into the transaction cache so
        it rides the flip flush.
        """
        marker_value = int_to_bytes(STATE_FORMAT_VERSION)
        if self._changelog_producer is not None:
            # PER-PHASE delivery accounting: this marker phase gets its own
            # counter object, so its confirm judges only its own produce/ack
            # pair. An ack from any earlier phase on this partition (the
            # empty-census best-effort marker whose delivery failure
            # ``complete_recovery`` swallowed, an earlier chunked backfill) is
            # credited to THAT phase's object and cannot be observed here —
            # neither to wedge this marker nor to falsely confirm it.
            phase = MigrationDeliveryPhase()
            self._changelog_producer.produce(
                key=TTL_MIGRATION_DONE_KEY,
                value=marker_value,
                headers={
                    CHANGELOG_CF_MESSAGE_HEADER: TTL_SYSTEM_CF_NAME,
                    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                },
                # This recovery-completion done-marker is produced with no
                # open checkpoint transaction, so it MUST use the non-transactional
                # migration route under exactly-once (a transactional produce
                # outside a transaction is invalid).
                migration=True,
                # Count this record against THIS phase's outstanding.
                on_delivery=phase.on_delivery,
            )
            phase.record_produced()
            # Confirm the marker is durably on the changelog BEFORE the local
            # write; a stuck broker raises rather than marking the store done
            # ahead of the changelog (which would defeat the once-only guarantee
            # on a later cold rebuild that never sees the marker).
            self._flush_backfill_changelog(self._changelog_producer, phase)
        batch = WriteBatch(raw_mode=True)
        batch.put(
            TTL_MIGRATION_DONE_KEY,
            marker_value,
            self.get_column_family_handle(TTL_SYSTEM_CF_NAME),
        )
        self._write(batch)

    def _resume_interrupted_live_backfill(self) -> None:
        """
        Resume an interrupted in-place live legacy backfill after a warm restart.
        Entered from :meth:`complete_recovery` when the store is flipped, the
        ``__ttl_backfill_stamped__`` ledger is non-empty, and no done-marker
        exists — an in-place :meth:`backfill_legacy_records` that produced some
        chunks to the changelog but crashed before the flag-last flip, then flipped
        via changelog replay on this warm restart. The
        un-stamped legacy leftovers sit below the replayed offset range and were
        never censused, so they must be finished here.

        The ledger it drives off is populated by committed chunks AND — for a
        chunk lost in its produce→commit window, including the very FIRST chunk,
        where nothing committed and the ledger opened EMPTY — by
        :meth:`recover_from_changelog_message`, which ledgers replayed header-true
        records while ``__ttl_backfill_in_progress__`` is armed. Both routes leave
        the same invariant: ledger == the keys already stamped on disk.

        Re-invokes the existing :meth:`backfill_legacy_records` over the ledger
        complement (its census excludes ledger members, so it re-stamps exactly the
        not-yet-stamped remainder — chunked, changelog-first, flush-confirmed,
        ledger + progress updated atomically per chunk). This is inherently
        resumable: interrupting the resume and restarting again re-derives the
        (now-smaller) complement and converges, never double-wrapping an
        already-stamped value.

        Expiry: the resumed leftovers inherit the SAME uniform window their
        already-migrated cohort received, derived from the surviving stamped cohort
        (deterministic, a fixed on-disk value):

        - ``_recovery_max_survivor_expiry_ms`` (max replayed future stamp) if replay
          happened this session; else
        - ``_max_index_stamp_ms()`` (max on-disk ``__ttl_index__`` stamp — the
          persisted survivor cohort) on an offset-caught-up second restart with no
          replay, but ONLY while it is still in the future relative to the recovery
          clock; else
        - ``SENTINEL_NEVER`` + a WARN when there is no surviving future stamp —
          the index is empty OR its max stamp is already in the PAST after
          downtime (clamped against the recovery clock) — never a
          past/derived expiry that would mass-delete the leftovers on the next
          sweep.

        ``legacy_records_ttl`` config is deliberately NOT preferred here (unlike the
        cold-restore completion path): the survivors on this same volume were
        stamped at event-time ``high_water + ttl``, so matching them keeps the whole
        backfilled cohort on one uniform window.
        """
        # Step 1 — defensive flip guard (normally already flipped by replay).
        if not self.uses_ttl_stamps:
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            self._stamp_flip_metadata()

        # Step 2 — derive the uniform expiry (survivor-derived), CLAMPED against
        # the recovery clock.
        now = (
            self._recovery_now_ms
            if self._recovery_now_ms is not None
            else self._now_ms()
        )
        if self._recovery_max_survivor_expiry_ms is not None:
            # A replayed survivor: the latest-record-wins drop filter already
            # dropped past-dated stamps this session, so this is future by
            # construction — no clamp needed.
            expires_at_ms = self._recovery_max_survivor_expiry_ms
        elif (
            max_index_stamp := self._max_index_stamp_ms()
        ) is not None and max_index_stamp > now:
            # Offset-caught-up second restart (no replay): the max ON-DISK index
            # stamp is the persisted survivor cohort — but only usable while it is
            # still in the future relative to the recovery clock.
            expires_at_ms = max_index_stamp
        else:
            # No replayed survivor AND the max on-disk index stamp is absent OR
            # already in the past relative to the recovery clock (a warm restart
            # after downtime past the window). Stamp the complement never-expiring
            # rather than deleting it on the next sweep.
            expires_at_ms = SENTINEL_NEVER
            logger.warning(
                "TTL legacy backfill RESUME at path=%s: no surviving future stamp "
                "to derive the cohort expiry from (the max on-disk index stamp is "
                "absent or already in the past relative to the recovery clock %d); "
                "stamping the un-stamped complement as never-expiring "
                "(SENTINEL_NEVER) rather than deleting it on the next sweep. NOTE: "
                "the resume path deliberately does not consult legacy_records_ttl; "
                "these records next carry a finite expiry only when overwritten "
                "with an explicit ttl=.",
                self._path,
                now,
            )

        # Step 3 — RESUME STARTED, then re-run the existing backfill over the
        # ledger complement (census excludes ledger members).
        logger.info(
            "TTL legacy backfill RESUME STARTED: interrupted live migration "
            "detected at path=%s (flipped, ledger non-empty, no done-marker); "
            "resuming over the un-stamped complement with expiry=%d.",
            self._path,
            expires_at_ms,
        )
        resumed = self.backfill_legacy_records(
            expires_at_ms=expires_at_ms,
            changelog_producer=self._changelog_producer,
            processed_offsets=None,
            staged_default_keys=set(),
            chunk_size=self._legacy_backfill_chunk_size,
        )

        # Step 4 — done-marker flag-last (changelog-first, non-transactional).
        self._produce_migration_done_marker()

        # Step 5 — cleanup (now marker-gated): the marker is present, so this
        # drops the ledger + progress counter.
        self._cleanup_completed_backfill_bookkeeping()
        # Also drop any orphan recovery pending census. Warm-restart recovery
        # re-replays the stored changelog offset INCLUSIVELY (recovery.py, back-
        # compat), so a header-absent legacy record on the replay boundary can be
        # re-censused into ``__ttl_backfill_pending__`` even though the resume
        # drives its census from the ledger, not from pending. That entry is a
        # harmless orphan once the migration is done, but leaving it trips census-
        # based hygiene checks. Draining it here (AFTER the resume has stamped the
        # ledger complement and produced the done-marker — never before/during the
        # resume decision) keeps the completed store's pending CF empty.
        self._discard_backfill_pending()

        # Step 6 — RESUME COMPLETED.
        logger.info(
            "TTL legacy backfill RESUME COMPLETED: stamped %d leftover record(s) "
            "at path=%s; done-marker produced, backfill bookkeeping cleaned.",
            resumed,
            self._path,
        )

    def _warn_if_looks_like_v3240_upgrade(self, sample_size: int = 256) -> None:
        """
        Emit ONE prominent WARNING when a censused-but-never-flipped population
        LOOKS like a stock-v3.24.0 cold restore.

        v3.24.0 wrote 8-byte-stamped values but produced changelog records
        WITHOUT the ``__ttl_stamped__`` header and WITHOUT ``__ttl_index__``
        records, so a cold restore has no structural signal to tell it apart from
        a genuine v3.23.6 legacy store. Sample up to ``sample_size`` censused
        keys, point-get their default-CF values, and count how many pass the
        strict stamp validator :func:`_safe_decode_stamp`. If at least half look
        like stamps, warn.

        This method now fires only for the sub-100% "looks-like-but-not-all"
        case: the 100%-quorum case is handled at the :meth:`complete_recovery`
        gate, which auto-adopts provisionally (not-all-past) or quarantines
        (all-past) — it returns before reaching this WARN. A sub-100% census is
        genuine legacy (a v3.24.0 store is 100% stamped), so it is never adopted.

        HARD CONSTRAINT: the sampled bytes are used ONLY to decide whether to log —
        never to strip, flip, or re-route data. The values still land verbatim as
        legacy, and the census is discarded after this WARN. False positives (a
        genuine legacy store whose values start with plausible-stamp bytes) are
        benign; the message says it is heuristic.
        """
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        sampled = 0
        plausible = 0
        for raw_key in pending_cf.keys():
            value = default_cf.get(cast(bytes, raw_key), default=None)
            if value is not None:
                sampled += 1
                if _safe_decode_stamp(cast(bytes, value)) is not None:
                    plausible += 1
            if sampled >= sample_size:
                break
        if sampled and plausible / sampled >= 0.5:
            logger.warning(
                "Recovery at path=%s replayed a header-absent (legacy) changelog "
                "whose values mostly decode as 8-byte TTL stamps (%d/%d sampled). "
                "This LOOKS like a cold restore of a stock v3.24.0 store, which "
                "wrote stamped values WITHOUT the __ttl_stamped__ header; such "
                "records land verbatim as legacy and may read back with a "
                "spurious 8-byte prefix. This is a HEURISTIC detection only: NO "
                "automatic repair is applied and no value is modified (there is "
                "no reliable structural signal to classify these records). If "
                "this is a genuine v3.24.0 upgrade, re-seed the state from source "
                "or follow the release-note guidance / contact support; if it is "
                "a genuine pre-TTL v3.23.6 store, this warning is a benign false "
                "positive.",
                self._path,
                plausible,
                sampled,
            )

    def _all_pending_values_are_stamped(self) -> bool:
        """
        Total-quorum strict stamp validation over the pending census (the
        self-heal path). Point-get each pending key's default-CF value and
        require EVERY one to pass ``_safe_decode_stamp``; **short-circuit on the
        first failure**, so a pure-legacy store (population 1) typically pays a
        single point-get and the 99% legacy path is not taxed.

        This byte inspection makes ONLY the store-level, all-or-nothing adoption
        decision — never a per-record routing choice (per-record byte-heuristics
        are banned). Returns ``False`` on an empty census (no
        adoption without positive evidence).
        """
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        saw_any = False
        for raw_key in pending_cf.keys():
            saw_any = True
            value = default_cf.get(cast(bytes, raw_key), default=None)
            if value is None or _safe_decode_stamp(cast(bytes, value)) is None:
                return False
        return saw_any

    def _pending_all_stamps_in_past(self) -> bool:
        """
        Precondition: :meth:`_all_pending_values_are_stamped` is True. Decode the
        MAX plausible stamp across the pending census and return True iff it is
        ``<= the recovery clock`` (EVERY censused stamp is in the past). A
        ``SENTINEL_NEVER`` stamp counts as future (never past), so a census
        containing any never-expire record is NOT all-past.

        This is the store-level, all-or-nothing heuristic that
        separates the dangerous auto-actions from the safe ones. Raw dedup epoch-ms
        ``set_bytes`` leftovers are ALL in the past relative to the recovery clock,
        whereas a live v3.24.0 store has at least one future (or sentinel) surviving
        stamp. This is one store-wide decision, never a per-record routing choice
        (per-record byte routing stays banned). The recovery clock is shared with
        the survivor-expiry clamp: ``_recovery_now_ms`` when a stamped record was
        replayed this session, else the current wallclock.
        """
        now = (
            self._recovery_now_ms
            if self._recovery_now_ms is not None
            else self._now_ms()
        )
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        max_stamp: Optional[int] = None
        for raw_key in pending_cf.keys():
            value = default_cf.get(cast(bytes, raw_key), default=None)
            if value is None:
                continue
            decoded = _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                continue
            stamp, _ = decoded
            if stamp == SENTINEL_NEVER:
                # A never-expire stamp is future — the census is not all-past.
                return False
            if max_stamp is None or stamp > max_stamp:
                max_stamp = stamp
        if max_stamp is None:
            return False
        return max_stamp <= now

    def _survey_backfill_pending(self) -> _PendingCensusSurvey:
        """
        ONE-pass survey of the ``__ttl_backfill_pending__`` census for the cold
        auto-adopt decision (Branch B of :meth:`complete_recovery`): a single
        census walk (one default-CF point-get per key) computes the census
        size, the total stamp quorum, and the all-past heuristic together, then
        a single default-CF key walk proves census completeness. Replaces the
        separate count / all-stamped / all-past / coverage walks, three of
        which each point-got the default CF per censused key.

        Quorum (``all_stamped``): EVERY censused key must have a live
        default-CF value that passes ``_safe_decode_stamp``. The survey
        **short-circuits on the first failure**, so a pure-legacy store
        (population 1) typically pays a single point-get and the 99% legacy
        path is not taxed. An empty census fails the quorum (no adoption
        without positive evidence). This byte inspection makes ONLY the
        store-level, all-or-nothing adoption decision — never a per-record
        routing choice (per-record byte-heuristics are banned).

        All-past (``all_past``): True iff the MAX censused stamp is ``<= the
        recovery clock``. A ``SENTINEL_NEVER`` stamp counts as future (a
        never-expire record is not past), so any sentinel makes the census
        not-all-past.

        Coverage (``covers_default_cf``): True iff the census covers EVERY
        default-CF key. Invariant: adoption flips read semantics store-wide,
        so the census it is decided on must cover exactly the keys the flip
        will affect. The proof REQUIRES census ⊆ default keys (else count
        equality is not set equality) — that subset fact is established by the
        quorum loop of THIS SAME pass (each censused key point-verified a live
        default-CF value), so the precondition cannot be reordered away at a
        call site: coverage is only computed after the quorum has passed
        inside this method. The default-CF key walk short-circuits as soon as
        its count exceeds the census count.
        """
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        count = 0
        max_stamp: Optional[int] = None
        saw_never = False
        for raw_key in pending_cf.keys():
            value = default_cf.get(cast(bytes, raw_key), default=None)
            decoded = None if value is None else _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                # Quorum failure (or a censused key with no live default-CF
                # value): short-circuit — the remaining fields are unused.
                return _PendingCensusSurvey(
                    pending_count=count,
                    all_stamped=False,
                    all_past=False,
                    covers_default_cf=False,
                )
            count += 1
            stamp, _ = decoded
            if stamp == SENTINEL_NEVER:
                saw_never = True
            elif max_stamp is None or stamp > max_stamp:
                max_stamp = stamp
        if count == 0:
            # Empty census: no positive evidence, quorum fails.
            return _PendingCensusSurvey(
                pending_count=0,
                all_stamped=False,
                all_past=False,
                covers_default_cf=False,
            )
        now = (
            self._recovery_now_ms
            if self._recovery_now_ms is not None
            else self._now_ms()
        )
        all_past = not saw_never and max_stamp is not None and max_stamp <= now
        # Coverage proof — runs only under the quorum proven above (census ⊆
        # default keys), turning count equality into set equality.
        covers = True
        default_count = 0
        for _ in default_cf.keys():
            default_count += 1
            if default_count > count:
                covers = False
                break
        covers = covers and default_count == count
        return _PendingCensusSurvey(
            pending_count=count,
            all_stamped=True,
            all_past=all_past,
            covers_default_cf=covers,
        )

    def _adopt_v3240_stamps(self) -> None:
        """
        PROVISIONAL, REVERSIBLE adoption of the pending census as
        v3.24.0-stamped records (the cold-heuristic path). The total
        quorum has already been proven by :meth:`_all_pending_values_are_stamped`
        and the census is known not-all-past.

        Flip the partition into TTL mode, then for each pending key **keep the
        default-CF value verbatim** — it is already ``8B‖value`` on disk, so there
        is NO re-wrap (each record's own v3.24.0 stamp is preserved). Per key, in
        one ``WriteBatch``:

        - ``__ttl_adopt_backup__.put(key, verbatim original)`` — the reversible
          restore source for the ``QUIXSTREAMS_STATE_TTL_ROLLBACK`` lever;
        - ``__ttl_index__.put(index_key)`` for non-sentinel stamps so the sweep can
          later reclaim the record;
        - ``__ttl_backfill_pending__.delete(key)`` — the durable per-chunk cursor.

        The ``__ttl_adopt_pending__`` marker (== provisional marker) is written and
        ``_adopt_provisional`` set True **BEFORE the first chunk** so the sweep
        suppression is armed throughout adoption: a crash mid-adoption re-loads
        ``_adopt_provisional = True`` from the marker on the next open, so no
        partially-indexed adopted original is ever swept before
        :meth:`complete_recovery` resumes over the remaining census. Backup, index
        and marker are all LOCAL_ONLY (never on the changelog). Idempotent: an
        uncorroborated fresh-volume restart re-derives the census and re-runs
        adoption (the marker rewrite and per-key backup are both idempotent).

        **Chunked.** Committed in bounded chunks of ``legacy_backfill_chunk_size``
        (no new constant), mirroring the proven :meth:`_complete_pending_backfill`
        skeleton minus the changelog produce/flush (adoption is changelog-silent).
        """
        if not self.uses_ttl_stamps:
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            self._stamp_flip_metadata()

        # Arm the sweep guard FIRST: persist the provisional marker
        # and set the runtime flag BEFORE the first chunk. Written after the flip
        # metadata but before any index entry exists, so there is never a window
        # in which an INDEXED adopted record is unguarded — and a crash mid-drain
        # re-arms the guard on restart (the marker reloads _adopt_provisional).
        # Value = adoption wallclock so an operator can see when it happened.
        now_ms = (
            self._recovery_now_ms
            if self._recovery_now_ms is not None
            else self._now_ms()
        )
        marker_batch = WriteBatch(raw_mode=True)
        marker_batch.put(
            TTL_ADOPT_PENDING_KEY,
            int_to_bytes(now_ms),
            self.get_column_family_handle(METADATA_CF_NAME),
        )
        self._write(marker_batch)
        self._adopt_provisional = True

        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        # Ensure the backup CF exists before its handle is fetched.
        self.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        pending_handle = self.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)
        backup_handle = self.get_column_family_handle(TTL_ADOPT_BACKUP_CF_NAME)

        chunk_size = self._legacy_backfill_chunk_size
        adopted = 0
        # Seek-based continuation cursor (mirrors _complete_pending_backfill): the
        # per-chunk pending DELETE is the cursor, so an inclusive ``from_key`` seek
        # on the previous chunk's last (now-deleted) key lands on the next live key
        # and the scan never re-walks the accumulating tombstones.
        seek_from: Optional[bytes] = None
        while True:
            chunk_keys: list[bytes] = []
            chunk_iter = (
                pending_cf.items()
                if seek_from is None
                else pending_cf.items(from_key=seek_from)
            )
            for raw_key, _ in chunk_iter:
                chunk_keys.append(cast(bytes, raw_key))
                if len(chunk_keys) >= chunk_size:
                    break
            if not chunk_keys:
                break
            seek_from = chunk_keys[-1]

            batch = WriteBatch(raw_mode=True)
            for key in chunk_keys:
                value = default_cf.get(key, default=None)
                if value is not None:
                    decoded = _safe_decode_stamp(cast(bytes, value))
                    if decoded is not None:
                        stamp, _ = decoded
                        # Back up the pre-adoption original verbatim so a rollback
                        # can restore it byte-identical.
                        batch.put(key, cast(bytes, value), backup_handle)
                        # Sentinel-stamped adopted records are correct never-expire
                        # entries and (per the codec invariant) get no index entry.
                        # Values are NOT re-written — kept verbatim.
                        if stamp != SENTINEL_NEVER:
                            batch.put(encode_index_key(stamp, key), b"", index_handle)
                        adopted += 1
                    # decoded is None is unreachable after the quorum check; stay
                    # defensive and just advance the cursor (delete below).
                # The pending DELETE is the durable per-chunk cursor.
                batch.delete(key, pending_handle)
            self._write(batch)
            del batch

        # The census is drained; drop the CF wholesale as an O(1) belt-and-braces
        # cleanup (harmless if already empty).
        self._discard_backfill_pending()

        logger.warning(
            "Auto-adopted %d v3.24.0-shaped record(s) from the changelog at "
            "path=%s (REVERSIBLE): values kept verbatim, __ttl_index__ rebuilt, "
            "originals backed up to __ttl_adopt_backup__, sweep-deletion suppressed "
            "until corroborated by a live ttl= write. If this is actually a pre-TTL "
            "legacy store, set QUIXSTREAMS_STATE_TTL_ROLLBACK=1 and restart to roll "
            "back (originals restored byte-identical).",
            adopted,
            self._path,
        )

    def _rebuild_index_from_stamped_census(self) -> None:
        """
        Done-marker index rebuild. On a cold restore of a CORROBORATED
        cold-adopted store the header-absent adopted records replay verbatim but
        their LOCAL_ONLY ``__ttl_index__`` did not survive the fresh volume, so
        rebuild it from the still-censused all-stamped records (their stamps are
        kept verbatim) before the census is discarded. Precondition:
        :meth:`_all_pending_values_are_stamped` is True. Chunked over the pending
        CF; the census itself is discarded by the caller.
        """
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)

        chunk_size = self._legacy_backfill_chunk_size
        rebuilt = 0
        chunk_keys: list[bytes] = []
        batch = WriteBatch(raw_mode=True)
        for raw_key in pending_cf.keys():
            key = cast(bytes, raw_key)
            value = default_cf.get(key, default=None)
            if value is None:
                continue
            decoded = _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                continue
            stamp, _ = decoded
            if stamp != SENTINEL_NEVER:
                batch.put(encode_index_key(stamp, key), b"", index_handle)
                rebuilt += 1
            chunk_keys.append(key)
            if len(chunk_keys) >= chunk_size:
                self._write(batch)
                batch = WriteBatch(raw_mode=True)
                chunk_keys = []
        if chunk_keys:
            self._write(batch)
        if rebuilt:
            logger.info(
                "Recovery at path=%s: rebuilt %d __ttl_index__ entry(ies) from the "
                "corroborated cold-adopted census before discard (done-marker "
                "path).",
                self._path,
                rebuilt,
            )

    def _rebuild_index_from_default_cf(self) -> None:
        """
        Warm-adopt index completion. A v3.24.0 preview detected via a
        warm signal (a ``__ttl_format_version__`` / ``__ttl_high_water_ms__``
        marker, or the ``__ttl_index__`` CF) may carry NO index CF, or an EMPTY
        one — a preview build that flipped but never maintained the secondary
        expiry index. That index is ``LOCAL_ONLY`` (never on the changelog), so it
        does not survive a fresh volume either. If the index is absent/empty while
        the default CF holds stamped values, rebuild it verbatim from those stamps
        (non-sentinel only; values are NEVER rewritten) so the sweep can reclaim
        expired records — else the warm-flipped records are unindexed and never
        expire.

        A no-op when the index already holds ≥1 entry (the common warm case: the
        preview maintained it). Chunked over the default CF, ``LOCAL_ONLY``.
        """
        index_cf = self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        # Cheap emptiness probe: a populated index means the preview maintained it
        # — keep it verbatim and skip the default-CF scan entirely.
        if next(iter(index_cf.keys()), None) is not None:
            return
        default_cf = self.get_or_create_column_family("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)

        chunk_size = self._legacy_backfill_chunk_size
        rebuilt = 0
        pending_in_chunk = 0
        batch = WriteBatch(raw_mode=True)
        for raw_key, value in default_cf.items():
            decoded = _safe_decode_stamp(cast(bytes, value))
            if decoded is None:
                continue
            stamp, _ = decoded
            if stamp == SENTINEL_NEVER:
                # Never-expire stamps get no index entry (codec invariant).
                continue
            batch.put(encode_index_key(stamp, cast(bytes, raw_key)), b"", index_handle)
            rebuilt += 1
            pending_in_chunk += 1
            if pending_in_chunk >= chunk_size:
                self._write(batch)
                batch = WriteBatch(raw_mode=True)
                pending_in_chunk = 0
        if pending_in_chunk:
            self._write(batch)
        if rebuilt:
            logger.info(
                "Warm restart at path=%s: rebuilt %d __ttl_index__ entry(ies) from "
                "the stamped default values (v3.24.0 preview with no local index).",
                self._path,
                rebuilt,
            )

    def corroborate_adoption(self) -> None:
        """
        Adoption corroboration: a live ``state.set(..., ttl=...)`` write (non-sentinel)
        confirms a PROVISIONAL cold-heuristic adoption is genuine. Called from the
        transaction's :meth:`prepare` after :meth:`_maybe_flip_or_reject` and BEFORE
        ``super().prepare()`` (the changelog-commit barrier). One-time per partition:

        1. produce the durable migration-done marker (changelog-first,
           confirm-or-raise) so any FUTURE cold rebuild is deterministic via the
           done-marker index-rebuild path;
        2. clear ``__ttl_adopt_pending__`` and set ``_adopt_provisional = False`` —
           the sweep re-enables and reclaims now-past adopted records (the
           corroborating flush's own sweep runs right after this hook).

        **The backup drop is DEFERRED.** The irreversible
        ``__ttl_adopt_backup__`` drop is NOT done here; it runs from
        :meth:`finalize_corroboration_teardown`, which the transaction calls ONLY
        after ``super().prepare()`` succeeds. If ``super().prepare()`` fails (e.g. a
        changelog producer error) the transaction is FAILED and the backup CF is
        left intact, so a subsequent rollback is still possible. A crash that
        interrupts the teardown is reconciled at the next open by
        :meth:`_finish_interrupted_corroboration` (the durable done-marker proves
        corroboration succeeded).
        """
        self._produce_migration_done_marker()
        batch = WriteBatch(raw_mode=True)
        batch.delete(
            TTL_ADOPT_PENDING_KEY, self.get_column_family_handle(METADATA_CF_NAME)
        )
        self._write(batch)
        self._adopt_provisional = False
        logger.info(
            "Corroborated v3.24.0 adoption at path=%s on a live ttl= write; "
            "produced the durable migration-done marker, cleared the pending "
            "marker, lifted sweep suppression (backup dropped after the commit "
            "barrier).",
            self._path,
        )

    def finalize_corroboration_teardown(self) -> None:
        """
        Deferred, post-commit-barrier teardown of a corroboration.

        Drops the reversible ``__ttl_adopt_backup__`` CF. The transaction invokes
        this ONLY after ``super().prepare()`` has succeeded, so an aborted prepare
        never destroys the backup and rollback stays possible until corroboration
        actually reaches the commit barrier. Idempotent: the open-time reconciliation
        (:meth:`_finish_interrupted_corroboration`) repeats the drop if a crash
        interrupts this step.
        """
        self._drop_local_cf_if_exists(TTL_ADOPT_BACKUP_CF_NAME)

    def _finish_interrupted_corroboration(self) -> None:
        """
        Complete a corroboration whose LOCAL teardown was
        interrupted (durable done-marker present, but the pending-marker delete
        and/or the backup drop did not finish before a crash). Deletes the surviving
        ``__ttl_adopt_pending__`` marker, drops ``__ttl_adopt_backup__``, and clears
        the runtime provisional flag so the TTL sweep is no longer suppressed. Only
        called at open when the done-marker is present, so it can never demote a
        genuinely-still-provisional (uncorroborated) store.
        """
        batch = WriteBatch(raw_mode=True)
        batch.delete(
            TTL_ADOPT_PENDING_KEY, self.get_column_family_handle(METADATA_CF_NAME)
        )
        self._write(batch)
        self._drop_local_cf_if_exists(TTL_ADOPT_BACKUP_CF_NAME)
        self._adopt_provisional = False
        logger.info(
            "Reconciled an interrupted v3.24.0 corroboration at path=%s: the durable "
            "migration-done marker is present but the pending marker survived a "
            "crash; cleared the pending marker, dropped the backup, lifted sweep "
            "suppression.",
            self._path,
        )

    def _rollback_provisional_adopt(self) -> None:
        """
        Roll back a PROVISIONAL cold-heuristic adoption (warm restart
        with ``QUIXSTREAMS_STATE_TTL_ROLLBACK=1``). Revert the store to legacy mode
        WITHOUT losing post-adoption data, then delete the flip flag +
        format-version marker + high-water + provisional marker, drop
        ``__ttl_index__`` and ``__ttl_adopt_backup__``, and set the runtime flags
        back to legacy. Idempotent and safe: it only runs when the provisional
        marker is present, so a corroborated store (no marker) and the sound
        warm-deterministic path (no marker/backup) are never touched.

        **Value-aware, not blind backup-restore.** After a provisional
        adoption the default CF holds a mix of (a) UNTOUCHED adopted originals —
        still byte-identical to the ``__ttl_adopt_backup__`` snapshot — and (b)
        POST-ADOPTION writes committed after the flip: updates to an adopted key, or
        brand-new keys that never existed pre-adoption. Every (b) value carries the
        8-byte stamp the flipped write path prepends. Blindly restoring every backup
        entry (the old behavior) both CLOBBERED updates with their stale
        pre-adoption value AND left new keys stamp-prefixed. Instead, per
        default-CF key:

        - current value == its backup entry -> UNTOUCHED: leave the pre-adoption
          bytes verbatim (byte-identical rollback for a false-positive legacy
          store);
        - otherwise (differs from the backup, or the key is absent from the backup)
          -> POST-ADOPTION write: strip the 8-byte stamp the framework added while
          flipped, which reconstructs exactly what a legacy store would have
          persisted for that write, so the latest committed value survives the
          rollback.

        A key DELETED post-adoption is simply absent from the default CF here, so it
        stays deleted (never resurrected from the backup).
        """
        backup_cf = self.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        default_handle = self.get_column_family_handle("default")
        # Materialize both snapshots up front so the restore never writes to the
        # default CF while iterating it. Rollback is a rare, deliberate operator
        # action, so the transient full-key materialization is acceptable.
        backup = {
            bytes(cast(bytes, raw_key)): bytes(cast(bytes, value))
            for raw_key, value in backup_cf.items()
        }
        default_items = [
            (bytes(cast(bytes, raw_key)), bytes(cast(bytes, value)))
            for raw_key, value in default_cf.items()
        ]
        kept = 0
        reverted_puts: list[tuple[bytes, bytes]] = []
        for raw_key, current_value in default_items:
            original = backup.get(raw_key)
            if original is not None and current_value == original:
                # Untouched adopted original: its pre-adoption bytes are already on
                # disk verbatim — leave them (byte-identical rollback).
                kept += 1
                continue
            # Post-adoption write (update of an adopted key, or a brand-new key):
            # drop the 8-byte stamp the flipped write path prepended so the value
            # reads back as legacy user bytes rather than the stale backup or
            # stamp-prefixed garbage.
            #
            # Strip ONLY when the value actually carries a stamp. The same strict
            # validator the read path uses decides, because "differs from the
            # backup" does not imply "stamped": a value absent from the backup
            # snapshot can be a plain legacy value, and blindly removing eight
            # bytes destroys it -- json_dumps(1) is b'1', which would become b''.
            # That matters most here of all places: rollback is the mitigation for
            # a legacy store that was falsely adopted, and such a store is exactly
            # the one full of short un-stamped values.
            if _safe_decode_stamp(current_value) is None:
                logger.warning(
                    "Rollback at path=%s: a default-CF value carries no readable "
                    "TTL stamp; leaving it verbatim rather than stripping bytes "
                    "off a value that was never stamped.",
                    self._path,
                )
                kept += 1
                continue
            reverted_puts.append((raw_key, current_value[TTL_STAMP_BYTES:]))

        # Only the stamp-stripped post-adoption writes need rewriting; untouched
        # keys are already byte-identical on disk. Bounded chunks (no concurrent
        # write-while-iterate; the default CF is already fully materialized).
        chunk_size = self._legacy_backfill_chunk_size
        for i in range(0, len(reverted_puts), chunk_size):
            batch = WriteBatch(raw_mode=True)
            for raw_key, value in reverted_puts[i : i + chunk_size]:
                batch.put(raw_key, value, default_handle)
            self._write(batch)

        # Clear the flip / TTL metadata so the next open loads legacy mode.
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        meta_batch = WriteBatch(raw_mode=True)
        meta_batch.delete(TTL_ENABLED_KEY, metadata_handle)
        meta_batch.delete(STATE_FORMAT_VERSION_KEY, metadata_handle)
        meta_batch.delete(TTL_HIGH_WATER_KEY, metadata_handle)
        meta_batch.delete(TTL_ADOPT_PENDING_KEY, metadata_handle)
        self._write(meta_batch)

        # Drop the TTL index + backup CFs.
        self._drop_local_cf_if_exists(TTL_INDEX_CF_NAME)
        self._drop_local_cf_if_exists(TTL_ADOPT_BACKUP_CF_NAME)

        self.uses_ttl_stamps = False
        self._adopt_provisional = False
        self._high_water_ms = None
        logger.warning(
            "Rolled back v3.24.0 adoption at path=%s (QUIXSTREAMS_STATE_TTL_ROLLBACK);"
            " kept %d untouched original(s) byte-identical, stamp-stripped %d "
            "post-adoption write(s), reverted to legacy mode.",
            self._path,
            kept,
            len(reverted_puts),
        )

    def _has_warm_ttl_artifacts(self) -> bool:
        """
        Warm-signal probe for a store whose ``__ttl_enabled__`` flag is absent.
        Any ONE of these LOCAL, TTL-only artifacts positively identifies a v3.24.0
        (preview) store — a genuine pre-TTL v3.23.6 store never runs any TTL code
        and so never creates any of them:

        - the ``__ttl_index__`` column family exists;
        - the metadata CF holds ``__ttl_format_version__`` or ``__ttl_high_water_ms__``.

        EXCLUSION (critical): a CURRENT-build store that crashed mid-migration also
        has ``__ttl_index__`` / markers but is NOT a v3.24.0 preview — it carries
        this-branch migration bookkeeping the preview predates
        (``__ttl_backfill_pending__`` / ``__ttl_backfill_stamped__`` /
        ``__ttl_system__`` CFs, or the ``__ttl_backfill_progress__`` cursor). Such a
        store must recover via the changelog-replay / resume path (which keeps it
        legacy until the flip metadata lands), NOT via a spurious warm flip, so any
        of that bookkeeping vetoes the warm signal.

        Cheap: one ``list_cf`` (already done at open by the ledger/marker snapshots)
        plus a few metadata point-gets. Only consulted on the legacy-at-open path.
        """
        cfs = self.list_column_families()
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        # Veto: this-branch migration bookkeeping means a current-build store
        # (possibly a crashed migration), never a v3.24.0 preview.
        if (
            TTL_BACKFILL_PENDING_CF_NAME in cfs
            or TTL_BACKFILL_STAMPED_CF_NAME in cfs
            or TTL_SYSTEM_CF_NAME in cfs
            or metadata_cf.get(TTL_BACKFILL_PROGRESS_KEY, default=None) is not None
        ):
            return False
        if TTL_INDEX_CF_NAME in cfs:
            return True
        if metadata_cf.get(STATE_FORMAT_VERSION_KEY, default=None) is not None:
            return True
        if metadata_cf.get(TTL_HIGH_WATER_KEY, default=None) is not None:
            return True
        return False

    def _load_adopt_pending_flag(self) -> bool:
        """Whether the ``__ttl_adopt_pending__`` provisional-adoption marker is set
        on disk (metadata CF). Present == the store was cold-adopted provisionally
        and is not yet corroborated (sweep suppressed, backup live)."""
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        return metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None

    def _backfill_in_progress(self) -> bool:
        """Whether the durable ``__ttl_backfill_in_progress__`` marker is set on
        disk (metadata CF). Present == a live
        :meth:`backfill_legacy_records` armed it before its first chunk reached
        the changelog and has not cleared it, so chunks may be durable on the
        changelog while nothing has committed locally yet.

        Reads the ``__metadata__`` CF unguarded (parity with
        :meth:`_load_adopt_pending_flag` / :meth:`_load_ttl_enabled_flag`): every
        opened store already has it and, unlike the TTL bookkeeping CFs, its mere
        existence is NOT a classification signal, so this probe cannot perturb
        :meth:`_has_warm_ttl_artifacts`."""
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        return metadata_cf.get(TTL_BACKFILL_IN_PROGRESS_KEY, default=None) is not None

    def _set_backfill_in_progress(self, in_progress: bool) -> None:
        """Arm (``True``) or clear (``False``) the durable
        ``__ttl_backfill_in_progress__`` marker in its own committed batch.

        Committed through the raw ``self._db.write`` rather than :meth:`_write`
        on purpose: :meth:`_write` is the per-CHUNK commit seam of
        :meth:`backfill_legacy_records` — its call sequence during a backfill IS
        the chunk-commit sequence — and this marker is not a chunk. It is a
        pre-flight / teardown bookkeeping write that must land independently of,
        and outside, any chunk commit.

        Both directions are idempotent: re-arming an armed marker is a no-op put,
        clearing an absent one is a no-op delete."""
        batch = WriteBatch(raw_mode=True)
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        if in_progress:
            batch.put(TTL_BACKFILL_IN_PROGRESS_KEY, b"\x01", metadata_handle)
        else:
            batch.delete(TTL_BACKFILL_IN_PROGRESS_KEY, metadata_handle)
        self._db.write(batch)

    def _count_backfill_pending(self) -> int:
        """Count keys currently in the ``__ttl_backfill_pending__`` census CF."""
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        count = 0
        for _ in pending_cf.keys():
            count += 1
        return count

    def _backfill_pending_has_any(self) -> bool:
        """Cheap "is the pending census non-empty" probe: short-circuits on the
        first key instead of counting the whole CF.

        Read-only: an absent ``__ttl_backfill_pending__`` CF means an empty
        census, WITHOUT materializing the CF. Invariant: a probe must never
        create what it probes — CF existence is itself a classification signal
        (:meth:`_has_warm_ttl_artifacts` treats the migration-bookkeeping CFs as
        proof of current-build migration activity), so the CF is created only
        where the census is actually written (the recovery census / completion
        write sites)."""
        if TTL_BACKFILL_PENDING_CF_NAME not in self.list_column_families():
            return False
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        for _ in pending_cf.keys():
            return True
        return False

    def _live_backfill_ledger_has_any(self) -> bool:
        """Cheap "is the live-backfill stamped-ledger non-empty" probe:
        short-circuits on the first key instead of counting the whole CF. Mirrors
        :meth:`_backfill_pending_has_any` but scans the
        ``__ttl_backfill_stamped__`` ledger — the durable resume cursor of an
        interrupted in-place :meth:`backfill_legacy_records`.

        Read-only: an absent ledger CF means an empty ledger, WITHOUT
        materializing the CF (same never-create-on-a-read invariant as the
        pending probe — CF existence is a classification signal). The ledger CF
        is created only by the backfill / replay-ledgering write sites."""
        if TTL_BACKFILL_STAMPED_CF_NAME not in self.list_column_families():
            return False
        ledger_cf = self.get_or_create_column_family(TTL_BACKFILL_STAMPED_CF_NAME)
        for _ in ledger_cf.keys():
            return True
        return False

    def _has_local_migration_done_marker(self) -> bool:
        """Whether the durable "migration done" marker is present on disk in the
        replicated ``__ttl_system__`` CF. Its presence means the
        migration completed, so nothing is left to finish.

        Read-only: an absent ``__ttl_system__`` CF means no marker, WITHOUT
        materializing the CF (never-create-on-a-read invariant — CF existence is
        a classification signal in :meth:`_has_warm_ttl_artifacts`). The CF is
        created only where the marker is written / replayed."""
        if TTL_SYSTEM_CF_NAME not in self.list_column_families():
            return False
        system_cf = self.get_or_create_column_family(TTL_SYSTEM_CF_NAME)
        return system_cf.get(TTL_MIGRATION_DONE_KEY, default=None) is not None

    def has_incomplete_ttl_migration(self) -> bool:
        """
        Whether a durably-recorded legacy-TTL migration is flipped-but-unfinished
        and must be completed by :meth:`complete_recovery`. Consulted by
        ``RecoveryPartition.needs_recovery_check`` so an
        offset-caught-up restart (``highwater-1 == offset``) still runs the
        completion pass instead of stranding the leftover legacy records.

        True iff ALL of:

        - the partition is persisted-flipped into TTL mode (``uses_ttl_stamps``
          loaded True at open — the cheap gate that no-ops the 99% legacy path);
        - no durable "migration done" marker exists (else the migration is done);
        - AND any completion track still has work:
          - the ``__ttl_backfill_in_progress__`` metadata marker is set: a live
            :meth:`backfill_legacy_records` armed it BEFORE its first chunk's
            produce and never cleared it. A crash inside that FIRST chunk's
            produce→commit window leaves BOTH CF-based tracks empty — the ledger
            only becomes non-empty once a chunk commits locally, and the
            never-produced leftovers never replay and so never enter the census —
            while the replayed chunk still flips the store store-wide. Without
            this marker that strand is invisible to every completion track; OR
          - the ``__ttl_backfill_pending__`` census holds ≥1 leftover key (the
            recovery-completion / MIXED-changelog track); OR
          - the ``__ttl_backfill_stamped__`` ledger holds ≥1 key: an
            interrupted *live* backfill that flipped via changelog replay but
            never wrote its done-marker. Its leftovers live below the replayed
            offset range (never censused), so the pending census can be empty
            while the migration is genuinely unfinished — the ledger's presence
            is what forces the offset-caught-up second restart to run the resume
            (:meth:`_resume_interrupted_live_backfill`).

        Ordered cheapest-first with short-circuits: a legacy store returns on the
        first check with no CF scans; the in-progress marker (a single metadata
        point-get) is probed before either CF scan, then pending, then the ledger
        (any one satisfies the OR).

        Read-only end to end: every sub-probe treats an absent bookkeeping CF
        (``__ttl_system__`` / ``__ttl_backfill_pending__`` /
        ``__ttl_backfill_stamped__``) as its empty/False answer without creating
        it, so probing a store with no migration activity leaves it
        byte-identical (CF existence is a classification signal elsewhere). The
        marker probe reads ``__metadata__``, which every opened store already has
        and whose existence is not a classification signal.
        """
        if not self.uses_ttl_stamps:
            return False
        if self._has_local_migration_done_marker():
            return False
        return (
            self._backfill_in_progress()
            or self._backfill_pending_has_any()
            or self._live_backfill_ledger_has_any()
        )

    def _max_index_stamp_ms(self) -> Optional[int]:
        """
        Return the maximum expiry stamp among the on-disk ``__ttl_index__``
        entries, or ``None`` if the index is empty.

        Used to derive a survivor expiry for a config-absent completion that had
        NO replay this session (the offset-caught-up restart): with nothing
        replayed, ``_recovery_max_survivor_expiry_ms`` is unset, so the on-disk
        index — one entry per surviving non-sentinel record — is the only
        remaining evidence of the cohort's expiry window. Index keys are
        big-endian-stamp-prefixed, so the LAST key carries the largest stamp; we
        scan backwards and return the first decodable entry (a bounded walk past
        any trailing undecodable junk), falling back to a full forward max scan if
        the backwards seek is unsupported.
        """
        index_cf = self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        try:
            for raw_key in index_cf.keys(backwards=True):
                try:
                    stamp, _ = decode_index_key(cast(bytes, raw_key))
                except ValueError:
                    continue
                return stamp
            return None
        except TypeError:
            # ``keys(backwards=...)`` unsupported on this rocksdict build — fall
            # back to a forward max scan (correct, just O(n)).
            last_stamp: Optional[int] = None
            for raw_key in index_cf.keys():
                try:
                    stamp, _ = decode_index_key(cast(bytes, raw_key))
                except ValueError:
                    continue
                if last_stamp is None or stamp > last_stamp:
                    last_stamp = stamp
            return last_stamp

    def _log_backfill_flush_progress(self, outstanding: int, slice_no: int) -> None:
        """
        Per-slice DEBUG progress line for :meth:`_flush_backfill_changelog`
        (``slice_no`` is 1-based, supplied by the shared flush helper).
        """
        logger.debug(
            "TTL backfill changelog flush progress: %d outstanding after "
            "slice %d (path=%s)",
            outstanding,
            slice_no,
            self._path,
        )

    def _flush_backfill_changelog(
        self,
        changelog_producer: Optional[ChangelogProducer],
        phase: MigrationDeliveryPhase,
    ) -> None:
        """
        Confirm a backfill / recovery-completion chunk's changelog delivery via
        the shared progress-based bounded flush loop
        (:func:`quixstreams.state.base.migration_flush.confirm_migration_delivery`,
        also used by the memory backend's
        ``_confirm_migration_delivery_or_raise``) and map any non-confirmed
        verdict onto :class:`ChangelogFlushError`.

        Callers MUST invoke this AFTER producing a chunk's stamped records and
        BEFORE committing the chunk's local ``WriteBatch``: the stamped chunk has
        to be durably on the changelog before its stamps land locally, else a
        crash would leave the local store ahead of the changelog and a peer
        rebuild would diverge.

        The loop flushes in ``_BACKFILL_CHANGELOG_FLUSH_SLICE_S`` slices, up to
        ``_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES`` of them, and bases the stall
        decision on this phase's ``produced - acked`` counters rather than
        the shared producer's GLOBAL queue depth — see the helper's module
        docstring for the full decision table (per-phase vs global
        accounting, the drained-but-unacked failure signature, the non-int
        "indeterminate" test-double return). The timeout measures *lack of
        progress*, not *total time*, so it is robust to a large
        ``legacy_backfill_chunk_size``. The flush routes through the migration
        path (the dedicated non-transactional producer under exactly-once, else
        the main producer), so a confirmed flush means durable BEFORE the
        caller's local commit.

        :param phase: the :class:`MigrationDeliveryPhase` whose records were
            produced above — REQUIRED, with no default. A defaulted fresh phase
            would report ``produced == 0`` for an un-migrated caller, which on a
            drained queue confirms unconditionally and would silently disable
            changelog-first for that caller.
        """
        outcome = confirm_migration_delivery(
            changelog_producer,
            # A live (re-read per slice) view of this phase's delivery
            # accounting: the acked counter is mutated by the delivery
            # callbacks that each ``flush()`` slice serves.
            counters=phase.counters,
            # Read from this module's globals at CALL time (not captured at
            # import time / as defaults): tests monkeypatch them on this module
            # to shrink the loop.
            slice_timeout_s=_BACKFILL_CHANGELOG_FLUSH_SLICE_S,
            max_slices=_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES,
            on_slice_progress=self._log_backfill_flush_progress,
        )
        verdict = outcome.verdict
        if verdict is MigrationFlushVerdict.CONFIRMED:
            return
        if verdict is MigrationFlushVerdict.INDETERMINATE:
            # A non-int flush return (unconfigured test double / a producer with
            # no delivery accounting): do not block the local commit
            # (pre-existing "flush and proceed" behavior).
            return
        if verdict is MigrationFlushVerdict.DRAINED_UNACKED:
            raise ChangelogFlushError(
                f"{outcome.outstanding} "
                f"legacy-TTL backfill changelog record(s) drained from the "
                f"shared producer queue WITHOUT delivery confirmation (a "
                f"failed delivery never acks) at path={self._path}; aborting "
                f"before the local commit so the local store never gets "
                f"ahead of the changelog."
            )
        if verdict is MigrationFlushVerdict.NO_PROGRESS:
            raise ChangelogFlushError(
                f"{outcome.outstanding} legacy-TTL backfill changelog record(s) "
                f"made no delivery progress in a full "
                f"{_BACKFILL_CHANGELOG_FLUSH_SLICE_S}s "
                f"slice at path={self._path}; aborting before the local commit "
                f"so the local store never gets ahead of the changelog."
            )
        # SLICES_EXHAUSTED: the runaway cap was hit while still progressing.
        raise ChangelogFlushError(
            f"legacy-TTL backfill changelog still has {outcome.outstanding} "
            f"undelivered "
            f"record(s) after {_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES} × "
            f"{_BACKFILL_CHANGELOG_FLUSH_SLICE_S}s slices at path={self._path}; "
            f"aborting before the local commit so the local store never gets "
            f"ahead of the changelog."
        )

    def _discard_backfill_pending(self) -> None:
        """
        Drop the ``__ttl_backfill_pending__`` census CF wholesale (a hygiene
        cleanup).

        Called from :meth:`complete_recovery` when no ``__ttl_stamped__`` record
        was replayed this session: the class-level census gate may have PUT
        header-absent legacy keys into the pending CF (MIXED detection must census
        before it can tell stamped from legacy), but with no completion pass those
        entries are orphans. Dropping the whole CF is O(1) versus O(pending) per-
        key deletes — the right choice for a large pure-legacy store where every
        replayed record was censused. Safe because the pending CF is not consulted
        again in this partition's lifetime: a pure-legacy store's later live
        backfill uses the separate ``__ttl_backfill_stamped__`` ledger, and
        recovery-completion does not re-run.
        """
        self._drop_local_cf_if_exists(TTL_BACKFILL_PENDING_CF_NAME)

    def _drop_local_cf_if_exists(self, cf_name: str) -> None:
        """
        Drop a local-only bookkeeping CF if it exists, evicting it from the CF /
        handle caches so a later :meth:`get_or_create_column_family` recreates it
        empty. Never raises: a cleanup failure must not fail recovery or open.
        """
        try:
            if cf_name in self._db.list_cf(self._path):
                self._db.drop_column_family(cf_name)
        except Exception:
            logger.warning(
                "Failed to drop local bookkeeping CF %s at %s; leaving it in "
                "place (harmless, never consulted after this point).",
                cf_name,
                self._path,
                exc_info=True,
            )
        finally:
            self._cf_cache.pop(cf_name, None)
            self._cf_handle_cache.pop(cf_name, None)

    def _cleanup_completed_backfill_bookkeeping(self) -> None:
        """
        One-time post-migration cleanup, run on the first open of a partition
        that is already flipped into TTL mode. An in-place live backfill
        (:meth:`backfill_legacy_records`) leaves dead artifacts behind: the
        ``__ttl_backfill_stamped__`` ledger CF, the ``__ttl_backfill_progress__``
        counter and — only when it died before its first chunk committed — the
        ``__ttl_backfill_in_progress__`` marker (a clean finish clears that one
        itself). Once the migration is genuinely done the backfill never re-runs,
        so none of them is ever consulted again; drop them so a migrated store
        carries no lasting overhead (parallels the pending-CF hygiene on the
        recovery path).

        Gated FIRST on the durable "migration done" marker: a flipped store
        may be an *interrupted* live backfill whose flip landed via changelog
        replay while un-stamped legacy leftovers remain (no done-marker yet). Such
        a store MUST keep its resume ledger + progress so
        :meth:`complete_recovery` can finish the migration
        (:meth:`_resume_interrupted_live_backfill`) — dropping them here would
        permanently strand the leftovers. So we return early (keep everything)
        whenever the done-marker is absent; only a genuinely-completed migration
        (marker present) proceeds to drop the dead bookkeeping. This makes the
        docstring invariant true by *enforcement* (the marker check), not by
        assertion. The done-marker path (:meth:`complete_recovery`) always writes
        the marker before invoking this cleanup, so an already-completed migration
        (marker present at open) still cleans up exactly as before — no regression.

        Then gated on the progress counter / in-progress marker being present, so
        the common path (empty-store flip / already-cleaned) does two metadata
        point-gets and no-ops, and the work runs at most once (both keys are
        deleted here).
        """
        if not self._has_local_migration_done_marker():
            # Interrupted (flipped-but-unfinished) migration: keep the resume
            # ledger + progress counter + in-progress marker for
            # :meth:`complete_recovery`.
            return
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        progress = metadata_cf.get(TTL_BACKFILL_PROGRESS_KEY, default=None)
        in_progress = metadata_cf.get(TTL_BACKFILL_IN_PROGRESS_KEY, default=None)
        if progress is None and in_progress is None:
            return
        self._drop_local_cf_if_exists(TTL_BACKFILL_STAMPED_CF_NAME)
        batch = WriteBatch(raw_mode=True)
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        if progress is not None:
            batch.delete(TTL_BACKFILL_PROGRESS_KEY, metadata_handle)
        if in_progress is not None:
            # Backstop clear for a backfill that armed the marker and died before
            # its first chunk committed (so it never reached its own disarm) on a
            # store that has since been genuinely completed. This is the last
            # writer of the marker's lifecycle: with the done-marker present it
            # can never be needed again, and leaving it would keep
            # :meth:`has_incomplete_ttl_migration` reporting True forever.
            batch.delete(TTL_BACKFILL_IN_PROGRESS_KEY, metadata_handle)
        self._write(batch)

    def _complete_pending_backfill(
        self,
        expires_at_ms: int,
        chunk_size: int,
        total_pending: int,
    ) -> int:
        """
        Chunk-backfill the leftover legacy keys censused in
        ``__ttl_backfill_pending__``. Mirrors
        :meth:`backfill_legacy_records` but drives its census from the pending CF
        instead of the full default CF, and uses the pending-CF delete as its
        durable progress cursor (no integer cursor needed — a key leaves pending
        only once it has been stamped + indexed + produced atomically).

        Per chunk (up to ``chunk_size`` pending keys, byte-sorted):

        1. Point-get the key's current default-CF value; wrap it whole with
           ``encode_ttl_value(expires_at_ms, value)``, write the ``__ttl_index__``
           entry, and delete the key from the pending CF — all in one WriteBatch.
        2. Produce the chunk's stamped + header-bearing default-CF records to the
           changelog, then flush with a bounded timeout and raise
           :class:`ChangelogFlushError` if the chunk is not confirmed delivered —
           the local commit must never get ahead of the changelog.
        3. Commit the WriteBatch with the raw writer (no sweep — the leftovers
           expire in the future).

        The pending CF is scanned ONCE end-to-end via a seek-based continuation
        cursor: after each chunk the scan resumes from the chunk's
        last (now-deleted) key instead of re-reading from the head, so RocksDB
        never re-walks the accumulating tombstones (the old head-rescan was
        O(n^2) in the pending size).

        :param total_pending: leftover count already computed by the caller
            (:meth:`complete_recovery`), reused here as the DEBUG progress
            denominator so the pending CF is not rescanned.
        :return: count of leftover records stamped on this run.
        """
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        default_handle = self.get_column_family_handle("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        pending_handle = self.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)

        headers: Optional[HeadersMapping] = None
        if self._changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                # Completion runs at recovery with no triggering live message, so
                # there are no processed offsets to encode. The live backfill
                # (:meth:`backfill_legacy_records`) also encodes ``None`` here so its
                # migration re-stamps are likewise always-apply on a later restore.
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                # Completion records are stamped, so they carry the stamped bit; a
                # subsequent restore then sees an all-stamped changelog and never
                # re-enters completion.
                CHANGELOG_TTL_STAMPED_HEADER: b"\x01",
            }

        stamped_count = 0
        # ONE delivery-accounting phase for the whole completion operation,
        # constructed BEFORE the chunk loop: accounting is CUMULATIVE across the
        # chunks of a single operation, so each chunk's confirm sees
        # ``produced - acked`` over every chunk produced so far (a per-chunk
        # object would reset the count and stop detecting a stall carried over
        # from an earlier chunk).
        phase = MigrationDeliveryPhase()
        # Seek-based continuation cursor. ``seek_from`` is the last
        # key of the previous chunk; that chunk deleted it from the pending CF, so
        # an inclusive ``from_key`` seek lands on the next live key and the scan
        # never re-walks the tombstones the old head-rescan re-visited each chunk.
        # Crash-safety is unchanged: a fresh recovery call restarts the scan from
        # the head (``seek_from is None``) and redoes any keys whose chunk did not
        # commit — re-stamping a raw legacy value is idempotent (whole-value-once,
        # never an already-stamped one, since a stamped key has left pending).
        seek_from: Optional[bytes] = None
        while True:
            # CENSUS one chunk from the pending CF in byte-sorted order, resuming
            # from ``seek_from`` so each key is visited at most once per session.
            chunk_keys: list[bytes] = []
            if seek_from is None:
                chunk_iter = pending_cf.items()
            else:
                chunk_iter = pending_cf.items(from_key=seek_from)
            for raw_key, _ in chunk_iter:
                chunk_keys.append(cast(bytes, raw_key))
                if len(chunk_keys) >= chunk_size:
                    break
            if not chunk_keys:
                break
            seek_from = chunk_keys[-1]

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
                # Sentinel-stamped (never-expire) records skip the expiry index,
                # per the codec invariant (the all-expired fallback stamps
                # leftovers with SENTINEL_NEVER); every other expiry is indexed.
                if expires_at_ms != SENTINEL_NEVER:
                    batch.put(encode_index_key(expires_at_ms, key), b"", index_handle)
                batch.delete(key, pending_handle)
                produce.append((key, stamped))
                stamped_count += 1

            if self._changelog_producer is not None and produce:
                for key, stamped in produce:
                    # Migration route (non-transactional under exactly-once).
                    self._changelog_producer.produce(
                        key=key,
                        value=stamped,
                        headers=headers,
                        migration=True,
                        # Per-phase delivery accounting.
                        on_delivery=phase.on_delivery,
                    )
                    phase.record_produced()
                # Confirm the chunk is durably on the changelog BEFORE the local
                # commit; a stuck broker raises rather than writing local-ahead.
                self._flush_backfill_changelog(self._changelog_producer, phase)

            # COMMIT atomically: default puts + index puts + pending deletes. The
            # pending deletes are the durable cursor — a crash before this commit
            # leaves the chunk's keys in pending and the next pass redoes them.
            self._write(batch)

            # PROGRESS: one DEBUG line per chunk on the recovery-completion
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
        # Keys re-written into the default CF in this same flush. The TTL sweep
        # reads committed disk state (not this uncommitted batch), so it must not
        # delete a key the batch just refreshed — otherwise the stale-read delete
        # clobbers the fresh write. Track TTL-index keys too: a refreshed value
        # can legitimately stage the same expiry index key the sweep is visiting.
        staged_default_keys: set[bytes] = set()
        staged_ttl_index_keys: set[bytes] = set()
        # Only track staged keys when a sweep will actually consume them. For
        # legacy / unflipped partitions (the 99% no-TTL workload) the sweep never
        # runs, so this keeps the inner write loop byte-identical to v3.23.6.
        track_staged = self.uses_ttl_stamps
        for cf_name in column_families:
            cf_handle = self.get_column_family_handle(cf_name)

            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    batch.put(key, value, cf_handle)
                    if track_staged:
                        if cf_name == "default":
                            staged_default_keys.add(key)
                        elif cf_name == TTL_INDEX_CF_NAME:
                            staged_ttl_index_keys.add(key)

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                batch.delete(key, cf_handle)

        if self.uses_ttl_stamps and self._high_water_ms is not None:
            batch.put(
                TTL_HIGH_WATER_KEY,
                int_to_bytes(self._high_water_ms),
                self.get_column_family_handle(METADATA_CF_NAME),
            )

        if self.uses_ttl_stamps and not self._ttl_changelog_tombstones:
            # OFF path (escape hatch): local-only sweep, exactly the pre-change
            # behavior. On the ON path (default) eviction + index GC were already
            # staged into the transaction cache at prepare-time
            # (:meth:`sweep_expired_into_cache`) and are applied by the cache walk
            # above — main-CF evictions also produced as changelog tombstones —
            # so no sweep runs here. ``staged_*`` tracking above stays for this
            # OFF path.
            self._run_sweep(
                batch=batch,
                staged_default_keys=staged_default_keys,
                staged_ttl_index_keys=staged_ttl_index_keys,
            )

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
        the empty-store flip path and the populated-store auto-backfill path.

        ``seek_to_first`` on the default CF runs once per partition lifetime
        (only on the flush that flips), so its cost is irrelevant.
        """
        return self._main_cf_has_user_data()

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
        Provably-complete backfill: census the full default-CF key list FIRST,
        then chunk over that
        frozen list, point-getting each value fresh and re-stamping it with a
        uniform ``expires_at_ms`` expiry.

        **Peak memory.** Peak transient memory is
        **O(census key count)** — the full sorted ``key_list`` is materialized up
        front (~80 B/key), which is exactly what ``_CENSUS_SPILL_WARN_THRESHOLD``
        guards. Only the *values* are chunk-bounded: each chunk point-gets and
        holds one ``chunk_size`` batch of values at a time, persisting and
        producing it before reading the next. (An earlier docstring incorrectly
        claimed peak memory ≈ one chunk; the key list dominates.)

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

        **Stamped-key ledger resume (crash-safe against interleaved writes).**
        This **supersedes** the original integer-cursor-over-a-re-sorted-list
        resume, which double-wrapped/skipped keys when a legacy
        write landed between a crash and the resume (the sorted census shifted
        under a stale integer index). Instead, each chunk records the
        keys it stamped in the local-only ``__ttl_backfill_stamped__`` ledger CF,
        PUT in the **same WriteBatch** as the stamped values so ledger and data
        commit atomically. The census then excludes both ``staged_default_keys``
        and every ledger member. **Invariant:** on any (re-)run the census is
        exactly ``{keys on disk} − {staged} − {already stamped}``, which is
        insensitive to population changes across the crash gap — a fresh legacy
        key written during the gap is simply a not-yet-stamped census key and
        gets stamped; an already-stamped key is a ledger member and is excluded,
        so it is never re-read and never re-wrapped (no double-wrap). No integer
        index into a re-sorted list is ever consulted for resume.

        **In-progress marker (first-chunk crash window).** The ledger above only
        becomes non-empty once a chunk COMMITS LOCALLY, but this method is
        changelog-first (produce + flush-confirm, THEN commit). A crash inside the
        FIRST chunk's produce→commit window therefore leaves that chunk durable on
        the changelog with an empty ledger, an empty pending census (the leftovers
        were never produced, so they never replay and never get censused) and a
        still-legacy store — and the next warm restart replays the chunk, flips
        the store STORE-WIDE, and finds nothing to complete. To keep that window
        detectable, the durable local-only ``__ttl_backfill_in_progress__`` marker
        is armed in its OWN commit BEFORE the chunk loop and cleared after the
        last chunk commits. While it is set,
        :meth:`recover_from_changelog_message` ledgers every header-true
        default-CF record it replays, which rebuilds the resume cursor for that
        window; :meth:`has_incomplete_ttl_migration` and
        :meth:`complete_recovery` both consult it too.

        **Overwritten-key rule.** A ledger key that is *overwritten by a plain
        (non-``ttl=``) legacy write during the crash→resume gap* is left as that
        raw value (it stays a ledger member and is excluded from the resumed
        census); the fail-safe read then treats it as never-expires — which
        matches the semantics of the plain write that produced it. An overwrite
        via a ``ttl=`` write cannot happen out-of-band during the gap: the
        partition is still legacy, so such a write itself *resumes* the backfill
        rather than landing a stamped value. No stamp-time value inspection and
        no write-path ledger maintenance is therefore required.

        **No format inference.** Every value read here is wrapped whole with
        ``encode_ttl_value(expires_at_ms, value)`` exactly once (census members
        are by construction not-yet-stamped). ``_looks_like_stamped_value`` is
        **not** used anywhere in this path; it survives only for the recovery
        flag-discovery path.

        Per chunk (up to ``chunk_size`` keys from the frozen census list):

        1. Point-get each key's value fresh; wrap whole into a ``WriteBatch`` of
           default-CF puts + ``__ttl_index__`` puts
           (``encode_index_key(expires_at_ms, key)``) + a ``__ttl_backfill_stamped__``
           ledger put per key. Keys deleted since the census (``get`` → ``None``)
           are skipped and NOT ledgered. ``staged_default_keys`` were already
           excluded by the census.
        2. Produce the chunk's re-stamped default-CF records to the changelog
           (the index and ledger CFs are local-only and are never produced), then
           ``flush()`` the producer so its in-flight queue stays bounded.
        3. Stage the observability progress counter into the same batch and commit
           with the raw writer ``self._write(batch)`` — NOT ``self.write(...)`` —
           so no sweep runs (the partition is still legacy) and the per-chunk
           default + index + ledger puts commit atomically together.
        4. Drop the chunk's structures before reading the next.

        After the last chunk lands the caller writes ``__ttl_enabled__`` / the
        format version **last**, so the flip is durable only once every census
        key has been stamped (flag-last ordering). The ledger + progress
        counter are dead weight once flipped and are dropped on the next flipped
        open (:meth:`_cleanup_completed_backfill_bookkeeping`). The parallel
        empty-store flip path is :meth:`flip_into_ttl_mode`.

        :param expires_at_ms: uniform absolute event-time expiry to stamp on
            every pre-existing record (``high_water + legacy_records_ttl``).
        :param changelog_producer: the partition's changelog producer, or
            ``None`` when changelog topics are disabled (chunks still persist
            locally; production is skipped).
        :param processed_offsets: accepted for call-signature compatibility but
            deliberately NOT encoded into the changelog headers — migration
            re-stamps are always-apply records (see the header build below), so
            they carry no processed offsets, unlike the base ``_prepare`` path.
            Retained because callers still pass their triggering offset.
        :param staged_default_keys: serialized default-CF keys present in the
            current transaction's update cache (genuine in-batch user writes).
            They are skipped here and re-stamped with their own true pending
            stamp by ``_restamp_default_cf_cache_for_flip``.
        :return: count of pre-existing records re-stamped on this run (already-
            stamped ledger members, staged, and deleted-since-census keys are not
            counted).
        """
        # Pre-create the index + stamped-ledger CFs so the per-chunk batch never
        # races a CF creation.
        self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        stamped_ledger = self.get_or_create_column_family(TTL_BACKFILL_STAMPED_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        default_handle = self.get_column_family_handle("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        metadata_handle = self.get_column_family_handle(METADATA_CF_NAME)
        stamped_handle = self.get_column_family_handle(TTL_BACKFILL_STAMPED_CF_NAME)

        headers: Optional[HeadersMapping] = None
        if changelog_producer is not None:
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: "default",
                # Migration re-stamps are ALWAYS-APPLY: encode NO processed offsets
                # (``json_dumps(None)``), matching ``_complete_pending_backfill`` and
                # ``_produce_migration_done_marker``. A re-stamp only adds the
                # deterministic 8-byte TTL prefix to an ALREADY-committed legacy value,
                # so its durability must not hinge on the triggering live write's
                # source offset. Encoding that offset (as ``_prepare`` does for genuine
                # live writes) let ``RecoveryPartition._should_apply_changelog`` SKIP
                # these records on a cold restore of an interrupted migration whose
                # triggering write was never committed; the skipped re-stamp then never
                # ran its ``__ttl_backfill_pending__`` supersession delete, so the
                # already-backfilled key was wrongly re-stamped by
                # ``complete_recovery`` (the recovery-offset-skip bug).
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: json_dumps(None),
                # Backfill records are always stamped (re-stamped legacy values),
                # so they unconditionally carry the stamped bit. The base
                # ``_prepare`` cannot set it for these because they are produced
                # directly here, before ``uses_ttl_stamps`` is flipped to True.
                CHANGELOG_TTL_STAMPED_HEADER: b"\x01",
            }

        # Step 0 — CENSUS: materialize the not-yet-stamped key list ONCE, with no
        # concurrent writes to the default CF (backfill is sequential inside
        # prepare(); processing is paused). Keys only — values are point-got fresh
        # per chunk. ``sorted`` makes the order explicit and reproducible.
        # ``staged_default_keys`` are excluded (re-stamped only by the caller's
        # ``_restamp_default_cf_cache_for_flip``); ``__ttl_backfill_stamped__``
        # ledger members are excluded because a prior interrupted run already
        # stamped them — this is the crash-safe resume that makes the census
        # insensitive to interleaved legacy writes (see the docstring invariant).
        # The ledger membership test is a point-get on the (separate) ledger CF,
        # never on the default CF, and never inspects value content.
        #
        # On the common FIRST-flip path the ledger is empty,
        # so the per-key ``not in stamped_ledger`` point-get would always be a
        # (bloom) negative. Probe the ledger ONCE up front and drop the per-key
        # membership term entirely when it is empty — only a resume over a
        # non-empty ledger pays the per-key check.
        if self._live_backfill_ledger_has_any():
            key_list: list[bytes] = sorted(
                cast(bytes, key)
                for key in default_cf.keys()
                if cast(bytes, key) not in staged_default_keys
                and cast(bytes, key) not in stamped_ledger
            )
        else:
            key_list = sorted(
                cast(bytes, key)
                for key in default_cf.keys()
                if cast(bytes, key) not in staged_default_keys
            )
        total = len(key_list)
        if total > _CENSUS_SPILL_WARN_THRESHOLD:
            logger.warning(
                "TTL legacy backfill censused %d keys at path=%s; the key list "
                "is held in memory (~80 B/key). For multi-million-key stores a "
                "spill-to-disk census will be needed; proceeding "
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

        # Cumulative count of records stamped by any prior interrupted run, used
        # only to keep ``__ttl_backfill_progress__`` a monotonic observability
        # counter across resumes. Resume CORRECTNESS rides the stamped-ledger
        # census exclusion above, NOT this integer (relying on the integer was
        # the original resume bug).
        prior_stamped = self._load_backfill_progress()

        # ONE delivery-accounting phase for the whole backfill operation,
        # constructed BEFORE the chunk loop: accounting is CUMULATIVE across the
        # chunks of a single operation, so each chunk's confirm sees
        # ``produced - acked`` over every chunk produced so far (a per-chunk
        # object would reset the count and stop detecting a stall carried over
        # from an earlier chunk).
        phase = MigrationDeliveryPhase()

        # ARM the durable in-progress marker in its OWN commit, BEFORE the first
        # chunk is produced, so it is on disk ahead of anything this backfill puts
        # on the changelog. This is the only artifact that survives a crash inside
        # the first chunk's produce→commit window (see the docstring). Skipped for
        # an empty census: there is no chunk to crash in, and the unconditional
        # clear below still covers a marker left by an earlier interrupted run.
        # Idempotent — a resume re-arms an already-armed marker as a no-op put.
        if total:
            self._set_backfill_in_progress(True)

        restamped = 0
        run_pos = 0
        while run_pos < total:
            chunk_keys = key_list[run_pos : run_pos + chunk_size]

            # RE-STAMP this chunk: point-get each value fresh and wrap whole.
            batch = WriteBatch(raw_mode=True)
            produce: list[tuple[bytes, bytes]] = []
            for key in chunk_keys:
                raw_value = default_cf.get(key, default=None)
                if raw_value is None:
                    # Deleted since the census — nothing to stamp, no index
                    # entry to create, not ledgered. Skip cleanly.
                    continue
                stamped = encode_ttl_value(expires_at_ms, cast(bytes, raw_value))
                batch.put(key, stamped, default_handle)
                # A SENTINEL_NEVER (never-expires) expiry must
                # NOT be indexed — a sentinel-stamped record never expires, so a
                # ``__ttl_index__`` entry for it is a permanent, never-swept leak
                # (parity with ``_complete_pending_backfill`` / ``_adopt_v3240_stamps``
                # / ``_restamp_default_cf_cache_for_flip``). Reachable here via the
                # resume / ``_resume_interrupted_live_backfill`` fallback and via the
                # additive-sum clamp (``clamp_additive_expiry``).
                if expires_at_ms != SENTINEL_NEVER:
                    batch.put(encode_index_key(expires_at_ms, key), b"", index_handle)
                # LEDGER this key as stamped IN THE SAME batch, so a crash cannot
                # leave a key stamped-on-disk but absent from the resume ledger
                # (which would double-wrap it on the next run).
                batch.put(key, b"", stamped_handle)
                produce.append((key, stamped))
                restamped += 1

            # PRODUCE this chunk's re-stamped default-CF records, then flush with
            # a bounded timeout and confirm delivery BEFORE the local commit: a
            # stuck broker raises rather than letting the local store get ahead of
            # the changelog. This also keeps the in-flight queue
            # bounded across chunks.
            if changelog_producer is not None and produce:
                for key, stamped in produce:
                    # Route via the non-transactional migration producer
                    # under exactly-once so the per-chunk flush below is durable
                    # before the local write.
                    changelog_producer.produce(
                        key=key,
                        value=stamped,
                        headers=headers,
                        migration=True,
                        # Per-phase delivery accounting.
                        on_delivery=phase.on_delivery,
                    )
                    phase.record_produced()
                self._flush_backfill_changelog(changelog_producer, phase)

            # ADVANCE the observability counter IN THE SAME batch as the chunk's
            # puts. It is cumulative (prior runs + this run) so it never regresses
            # across a resume; it is NOT read back for resume decisions.
            run_pos += len(chunk_keys)
            batch.put(
                TTL_BACKFILL_PROGRESS_KEY,
                int_to_bytes(prior_stamped + restamped),
                metadata_handle,
            )

            # COMMIT this chunk atomically (default + index + ledger puts +
            # progress counter).
            self._write(batch)

            # PROGRESS: one DEBUG line per chunk (~chunk_size records, default
            # 10k) so a large/long backfill is observable instead of looking
            # like a hang. ``run_pos`` is this run's census position; the final
            # completion log is still emitted by the caller.
            logger.debug(
                "TTL legacy backfill progress: %d / %d records re-stamped path=%s",
                run_pos,
                total,
                self._path,
            )

            # RELEASE: drop the chunk's structures before the next iteration.
            del batch, produce

        # DISARM: every census key is now stamped BOTH locally and on the
        # changelog, so the crash window the marker guards is closed. Cleared here
        # rather than after the caller's flag-last flip because the flip's own
        # crash window is already covered by the (now fully populated) ledger: a
        # replay-driven flip re-enters this method and censuses the empty
        # complement. Leaving it armed would make every later restart of a
        # migrated store re-report an incomplete migration.
        self._set_backfill_in_progress(False)

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
        Read the persisted backfill progress counter ``__ttl_backfill_progress__``
        from the metadata CF. Absent / undecodable = ``0`` (first run / no
        progress yet, or a stale/garbled value from an older build — handled
        gracefully by restarting the count at 0).

        This is now an **observability counter only** (cumulative
        records stamped so far); resume correctness rides the
        ``__ttl_backfill_stamped__`` ledger census exclusion, not this integer
        (see :meth:`backfill_legacy_records`). A stale integer left by the old
        integer-cursor build is therefore harmless: it can only make the
        counter's starting value wrong, never skip or re-stamp a key.
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
        start = time.monotonic()
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        # Clean the column family caches to drop references
        # Otherwise the Rocksdb won't close properly
        self._cf_handle_cache = {}
        self._cf_cache = {}
        # Stop background flush/compaction before closing so that db.close()
        # does not block waiting for it to wind down. On large DBs this wait
        # dominates the revoke sequence and is what pushes it past
        # max.poll.interval.ms during a rebalance handover; cancelling it
        # releases the OS lock in ~milliseconds. Unflushed memtable data is
        # preserved by the WAL, and cancelled compaction debt simply resumes
        # under the next owner.
        # Guarded with getattr because cancel_all_background is not present in
        # every rocksdict release within our supported range; when it's absent
        # we simply fall back to the plain (slower) close.
        cancel_all_background = getattr(self._db, "cancel_all_background", None)
        cancelled_background = False
        try:
            if cancel_all_background is not None:
                try:
                    cancel_all_background(True)
                    cancelled_background = True
                except Exception as exc:
                    # Never let a cancel failure skip the close() below - that
                    # would leak the OS lock (the exact livelock this PR fixes).
                    logger.warning(
                        f"Failed to cancel background work before closing rocksdb "
                        f'partition on "{self._path}"; attempting close anyway. '
                        f"({exc})"
                    )
            self._db.close()
        except Exception as exc:
            # "Shutdown in progress" is only expected *after* a successful
            # cancel_all_background(); there the DB still closes and the lock is
            # released, so it is benign. On the plain-close fallback (never
            # cancelled) or for any other text it is a real failure and
            # propagates.
            if cancelled_background and "shutdown in progress" in str(exc).lower():
                logger.debug(
                    f'Benign "shutdown in progress" closing rocksdb partition on '
                    f'"{self._path}" after cancelling background work; lock '
                    f"released. ({exc})"
                )
            else:
                raise
        elapsed = round(time.monotonic() - start, 4)
        logger.debug(f'Closed rocksdb partition on "{self._path}" in {elapsed}s')

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

    def _run_sweep(
        self,
        batch: WriteBatch,
        staged_default_keys: "set[bytes] | None" = None,
        staged_ttl_index_keys: "set[bytes] | None" = None,
    ) -> None:
        """
        Bounded-budget sweep over the secondary expiry index.

        Called from :meth:`write` so any deletes go into the same batch as
        the user-driven writes for atomicity.

        ``staged_default_keys`` are the default-CF keys re-written in this same
        flush. The sweep reads committed disk state, which is stale for those
        keys, so it must never delete them here.

        ``staged_ttl_index_keys`` are the TTL-index entries written in this
        batch. If a fresh write reuses the same expiry stamp as the stale index
        entry being swept, deleting that index key would orphan the fresh value.
        """
        staged_default: "set[bytes] | frozenset[bytes]" = (
            staged_default_keys or frozenset()
        )
        staged_ttl_index: "set[bytes] | frozenset[bytes]" = (
            staged_ttl_index_keys or frozenset()
        )
        if self._high_water_ms is None:
            # Cold start: no event-time established yet — skip the sweep.
            return
        if self._adopt_provisional:
            # Provisional-adoption sweep guard: while a COLD-heuristic adoption
            # is provisional (uncorroborated), the sweep is a complete no-op so
            # no misidentified (or past-dated) adopted original is ever deleted
            # before an operator can roll back. Lifted on corroboration.
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index_cf = self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        main_cf = self.get_or_create_column_family("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        main_handle = self.get_column_family_handle("default")

        def delete_index_if_not_staged(index_key: bytes) -> None:
            if index_key not in staged_ttl_index:
                batch.delete(index_key, index_handle)

        # Bound the iterator at the cutoff stamp to skip future expiries
        # without paying for the iterator step. Build the cutoff prefix as
        # 8 BE bytes equal to ``now_ms + 1`` so any entry whose first
        # 8 bytes equal exactly ``now_ms`` is still iterated.
        upper_bound = int_to_bytes(now_ms + 1) if now_ms < (2**64 - 1) else None
        read_opt = ReadOptions()
        if upper_bound is not None:
            read_opt.set_iterate_upper_bound(upper_bound)

        evicted = 0
        visited = 0
        iterator: Iterator[tuple[bytes, bytes]] = cast(
            Iterator[tuple[bytes, bytes]],
            index_cf.items(from_key=b"", read_opt=read_opt),
        )
        # The budget counts every index-entry VISIT (ghost or genuine), not
        # just evictions, so a store dense with refresh-minted ghost index entries
        # cannot pay more than ``budget`` main-CF point-gets per sweep. Convergent:
        # ghosts shrink each sweep until none remain and cease consuming budget.
        for index_key, _ in iterator:
            if visited >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                delete_index_if_not_staged(index_key)
                visited += 1
                continue

            if idx_expires_at > now_ms:
                # Sorted by expiry — the rest is in the future (no point-get, so
                # it is not counted against the visit budget).
                break
            visited += 1

            main_value = main_cf.get(user_key, default=None)
            if main_value is None:
                # Ghost: user deleted the main entry but the index still
                # points at it. GC the orphaned index entry.
                delete_index_if_not_staged(index_key)
                continue

            # Decode through the SAME strict validator as the live read path
            # ``decode_ttl_value`` accepts any 8-byte prefix as a
            # stamp, including ``0`` and implausibly-large values — but the read
            # path (``_safe_decode_stamp``) treats those as never-expires and
            # returns the value raw. Using it here closes the ``stamp==0``
            # divergence where a reader sees never-expires while the sweep would
            # decode ``0 <= now`` and silently evict the value.
            decoded_main = _safe_decode_stamp(cast(bytes, main_value))
            if decoded_main is None:
                # Not a plausible stamp (too short, raw legacy value, zero, or
                # out-of-range): the read path returns it raw as never-expires, so
                # the sweep must NOT delete it. Drop only the orphan index pointer.
                delete_index_if_not_staged(index_key)
                continue
            main_expires_at, _ = decoded_main

            if main_expires_at == SENTINEL_NEVER:
                # Ghost: key was overwritten by a plain ``state.set`` and
                # is now permanent. Drop the stale index pointer only.
                delete_index_if_not_staged(index_key)
                continue

            if main_expires_at == idx_expires_at and user_key not in staged_default:
                batch.delete(user_key, main_handle)
                delete_index_if_not_staged(index_key)
                evicted += 1
            else:
                # Ghost: key was overwritten with a fresh expiry stamp — either
                # already committed, or re-written in this same batch (in which
                # case the committed read above is stale). Drop only the stale
                # index pointer; deleting the key would clobber the fresh write.
                delete_index_if_not_staged(index_key)

        if evicted:
            logger.debug(
                "TTL sweep evicted %d expired entries on partition path=%s "
                "now_ms=%d budget=%d",
                evicted,
                self._path,
                now_ms,
                budget,
            )

    def sweep_expired_into_cache(
        self,
        cache: PartitionTransactionCache,
        staged_default_keys: set[bytes],
        staged_ttl_index_keys: set[bytes],
    ) -> None:
        """
        Prepare-time sweep (the changelog-tombstone ON path).

        Identical eviction logic to :meth:`_run_sweep`, but stages its deletes
        into the transaction ``cache`` instead of a ``WriteBatch``, and takes the
        ``staged_*`` guard sets from the caller (derived from the same cache) so
        the #1129 same-flush protections are preserved byte-for-byte:

        - a main-CF eviction → ``cache.delete(user_key, cf_name="default")``,
          which the base ``_prepare`` turns into a changelog tombstone
          (``value=None`` + ``__ttl_stamped__`` header) AND ``write()`` applies as
          a local delete — the exact route a user ``state.delete()`` takes, so the
          changelog physically shrinks under compaction in step with the store;
        - index-CF GC → ``cache.delete(index_key, cf_name=TTL_INDEX_CF_NAME)``,
          which is LOCAL-ONLY (``__ttl_index__`` ∈ ``LOCAL_ONLY_CFS``, so
          ``_prepare`` skips it) and applied only by ``write()``.

        Runs in ``prepare()`` AFTER ``_maybe_flip_or_reject`` (so the runtime flip
        + freshly-stamped cache writes are visible) and BEFORE ``super().prepare()``
        (so tombstones ride the same changelog batch as the user writes). It reads
        committed disk state (``main_cf.get`` / disk index iterator) exactly as the
        write-time sweep did — this tx has committed nothing yet — so the eviction
        decisions are byte-identical; only the delete sink changes.

        ``prefix=b""`` is passed for every ``cache.delete``: the cache stores
        deletes in a flat, prefix-independent set and only uses ``prefix`` to pop a
        pending update — but the guards below guarantee a staged key is never
        evicted, so that pop is always a no-op.
        """
        if self._high_water_ms is None:
            # Cold start: no event-time established yet — skip the sweep.
            return
        if self._adopt_provisional:
            # Provisional-adoption sweep guard (ON path): no eviction while a
            # COLD-heuristic adoption is provisional (uncorroborated). Lifted
            # on corroboration.
            return

        budget = self._max_evictions_per_flush
        if budget <= 0:
            return

        now_ms = self._high_water_ms
        index_cf = self.get_or_create_column_family(TTL_INDEX_CF_NAME)
        main_cf = self.get_or_create_column_family("default")

        def delete_index_if_not_staged(index_key: bytes) -> None:
            if index_key not in staged_ttl_index_keys:
                cache.delete(index_key, prefix=b"", cf_name=TTL_INDEX_CF_NAME)

        upper_bound = int_to_bytes(now_ms + 1) if now_ms < (2**64 - 1) else None
        read_opt = ReadOptions()
        if upper_bound is not None:
            read_opt.set_iterate_upper_bound(upper_bound)

        evicted = 0
        visited = 0
        iterator: Iterator[tuple[bytes, bytes]] = cast(
            Iterator[tuple[bytes, bytes]],
            index_cf.items(from_key=b"", read_opt=read_opt),
        )
        # Budget counts every index-entry visit (ghost or genuine), bounding
        # main-CF point-gets to <= budget per sweep (parity with _run_sweep).
        for index_key, _ in iterator:
            if visited >= budget:
                break

            try:
                idx_expires_at, user_key = decode_index_key(index_key)
            except ValueError:
                delete_index_if_not_staged(index_key)
                visited += 1
                continue

            if idx_expires_at > now_ms:
                # Sorted by expiry — the rest is in the future (no point-get, so
                # it is not counted against the visit budget).
                break
            visited += 1

            main_value = main_cf.get(user_key, default=None)
            if main_value is None:
                # Ghost: main entry gone but index still points at it.
                delete_index_if_not_staged(index_key)
                continue

            decoded_main = _safe_decode_stamp(cast(bytes, main_value))
            if decoded_main is None:
                # Not a plausible stamp: read path treats it as never-expires, so
                # never evict — drop only the orphan index pointer.
                delete_index_if_not_staged(index_key)
                continue
            main_expires_at, _ = decoded_main

            if main_expires_at == SENTINEL_NEVER:
                # Ghost: overwritten by a plain set → permanent. Drop the pointer.
                delete_index_if_not_staged(index_key)
                continue

            if (
                main_expires_at == idx_expires_at
                and user_key not in staged_default_keys
            ):
                # Genuine eviction: tombstone the main key (changelog + local) and
                # GC its index entry.
                cache.delete(user_key, prefix=b"", cf_name="default")
                delete_index_if_not_staged(index_key)
                evicted += 1
            else:
                # Ghost: overwritten with a fresh stamp (already committed or
                # re-written this flush). Drop only the stale index pointer;
                # deleting the key would clobber the fresh write.
                delete_index_if_not_staged(index_key)

        if evicted:
            logger.debug(
                "TTL sweep tombstoned %d expired entries on partition path=%s "
                "now_ms=%d budget=%d",
                evicted,
                self._path,
                now_ms,
                budget,
            )

    def _enforce_format_version(self) -> None:
        """
        Validate — and, for a v3.24.0 preview, UPGRADE IN PLACE — the on-disk
        format-version marker on a partition that is already flipped into TTL mode
        (``__ttl_enabled__`` was True at open time, or a warm signal flipped it).

        The shipping build carries the SAME
        :data:`STATE_FORMAT_VERSION` (=2) and stamp codec as the v3.24.0 preview,
        so a preview warm restart is forward-COMPATIBLE. Rather than reject a store
        whose marker is missing or below the current version (the old
        operator dead-end: "delete the state directory"), treat it as a preview to
        upgrade in place: rewrite the marker to :data:`STATE_FORMAT_VERSION` and log
        an INFO. This is what makes the warm upgrade "just work" for an earlier
        preview build that persisted the enabled flag with an older / absent marker.

        A marker already ``>= STATE_FORMAT_VERSION`` is accepted unchanged (the
        common case). Stores that never enabled TTL never enter this method — they
        have no marker by design (byte-identical to v3.23.6 on disk).

        Only the KNOWN preview shapes are upgraded: an absent marker, or a marker
        in ``[MIN_UPGRADEABLE_STATE_FORMAT_VERSION, STATE_FORMAT_VERSION)``. Any
        OTHER sub-current marker (``0``, negative, or an undecodable value) is not
        a recognized v3.24.0 preview — the forward-incompatibility guard is kept
        and ``IncompatibleStateStoreError`` is raised rather than silently
        rewriting a marker whose on-disk layout this build cannot vouch for.
        """
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        raw = metadata_cf.get(STATE_FORMAT_VERSION_KEY, default=None)
        if raw is None:
            version: Optional[int] = None
        else:
            try:
                version = int_from_bytes(cast(bytes, raw))
            except Exception:
                version = -1
        if version is not None and version >= STATE_FORMAT_VERSION:
            return
        if version is None or (
            MIN_UPGRADEABLE_STATE_FORMAT_VERSION <= version < STATE_FORMAT_VERSION
        ):
            # Recognized v3.24.0 preview (absent or an upgradeable lower marker):
            # rewrite the marker to the current version in place (no data change —
            # the value layout and stamp codec are identical) instead of rejecting.
            self._stamp_flip_metadata()
            logger.info(
                "Upgraded on-disk TTL format marker %s->%d in place at path=%s "
                "(v3.24.0 preview warm restart; values unchanged).",
                "absent" if version is None else version,
                STATE_FORMAT_VERSION,
                self._path,
            )
            return
        # A sub-current marker that is NOT a recognized preview shape (below the
        # upgradeable floor, or an undecodable value read back as -1): keep the
        # forward-incompatibility protection rather than rewriting it blindly.
        raise IncompatibleStateStoreError(
            f"On-disk TTL state format version {version} at path={self._path!r} is "
            f"not a recognized v3.24.0 preview (expected {STATE_FORMAT_VERSION}, an "
            f"absent marker, or a marker >= {MIN_UPGRADEABLE_STATE_FORMAT_VERSION} "
            f"and < {STATE_FORMAT_VERSION}); refusing to upgrade it in place."
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

        NO LONGER ON THE RECOVERY PATH. Recovery flip-discovery now
        routes purely on the out-of-band ``__ttl_stamped__`` changelog header
        (see ``recover_from_changelog_message``), because this content heuristic
        false-positives on legacy 8-byte epoch-ms values. Retained because
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
            loaded = int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning(
                "Failed to decode persisted TTL high-water at %s; "
                "treating it as undefined.",
                self._path,
            )
            return
        if loaded >= _MAX_PLAUSIBLE_STAMP_MS:
            # Implausible-event-time guard on the load path. A store
            # poisoned by an implausibly large high-water
            # persisted to ``TTL_HIGH_WATER_KEY`` before the
            # ``advance_high_water`` guard existed must not reload the poisoned
            # value verbatim: every finite-stamped record would then read as
            # already-expired (``stamp <= _high_water_ms``) and be swept on the
            # next flush — the exact mass-eviction that guard exists to
            # prevent, resurrected across a restart. IGNORE it (leave the
            # high-water undefined) so the store self-heals on the first
            # restart under the fixed build.
            logger.warning(
                "Ignoring implausibly large persisted TTL high-water %d (>= %d) "
                "at path=%s; loading it would poison the read-expiry filter and "
                "the sweep. The high-water re-establishes from the next "
                "timestamped read/write.",
                loaded,
                _MAX_PLAUSIBLE_STAMP_MS,
                self._path,
            )
            return
        self._high_water_ms = loaded

    def _normalize_replay_value(self, value: bytes) -> tuple[bytes, int]:
        """
        Decode a replayed (header-true, stamped) main-CF value into
        ``(stamped_blob, stamp)`` using the SAME strict validator as the live
        read path (``_safe_decode_stamp``).

        - A value that validates as a real stamp round-trips with its original
          stamp (the common case for a header-bearing stamped record).
        - A value that does NOT validate — too short, or an implausible/zero
          stamp — is treated as never-expires and wrapped with the sentinel, so
          it round-trips on read and the latest-record-wins recovery-drop filter
          never discards it.

        Routing through ``_safe_decode_stamp`` (rather than a bare
        ``decode_ttl_value``, which accepted ``stamp==0`` and out-of-range
        prefixes as genuine expiries) aligns recovery with reads: a value the
        read path reports as never-expires is never silently dropped during
        replay (the ``stamp==0`` divergence). Genuinely-unstamped legacy values
        (e.g. from a pre-v2 changelog topic) still round-trip via the sentinel
        wrap, exactly as before.
        """
        decoded = _safe_decode_stamp(value)
        if decoded is None:
            return encode_ttl_value(SENTINEL_NEVER, value), SENTINEL_NEVER
        stamp, _ = decoded
        return value, stamp

    # ------------------------------------------------------------------
    # RocksDB lifecycle.
    # ------------------------------------------------------------------

    def _open_rocksdict(self) -> Rdict:
        options = self._rocksdb_options
        options.create_if_missing(True)
        options.create_missing_column_families(True)

        def _existing_column_families() -> Optional[Dict[str, Options]]:
            # rocksdict applies the options passed to ``Rdict`` only to the
            # column families it *creates*. Existing CFs must be listed in
            # ``column_families=`` and handed the same options explicitly, or
            # every reopen silently drops the configured block cache and bloom
            # filter for them and falls back to rocksdict's defaults (8 MiB
            # cache, no filter policy) — undoing every ``RocksDBOptions``
            # tuning on the second and every subsequent open of a store.
            #
            # Returns ``None`` -- not ``{}`` -- when the CFs cannot be listed, so
            # the caller omits the argument entirely. An empty map is NOT
            # equivalent to omitting it: against a store that does hold CFs it
            # fails with "Invalid argument: Column families not opened: <cf>",
            # a message starting with neither "Corruption" nor "io error" and so
            # matching no recovery path, which would turn a transient listing
            # failure into an un-retried crash loop on a store that opens
            # perfectly well without the argument.
            try:
                return {name: options for name in Rdict.list_cf(self._path, options)}
            except Exception as exc:
                # Broad by necessity: rocksdict raises a bare ``Exception`` here
                # (an "IO error: No such file or directory" for a path with no
                # database), so there is no narrower type to catch. A missing
                # database is the ordinary cold start and must stay quiet; a
                # listing failure against a path that *does* hold one means the
                # configured options will not reach its existing column
                # families, which must never be invisible.
                # Probe for CURRENT, not the directory: the question is "does a
                # database live here", and a leftover empty directory would
                # otherwise warn about options missing column families that do
                # not exist. The corruption path destroys the whole directory,
                # so it correctly takes the debug branch.
                # "Is there a database here?" -- and if we cannot tell, assume
                # there is. ``os.path.exists`` returns False on ANY OSError, so a
                # populated store that is momentarily unstattable (EACCES, a
                # read-only remount, a too-long path) would otherwise be treated
                # as a cold start and opened degraded.
                try:
                    os.stat(os.path.join(self._path, "CURRENT"))
                    db_exists = True
                except FileNotFoundError:
                    db_exists = False
                except OSError:
                    db_exists = True

                if not db_exists:
                    # Ordinary cold start: this open creates the database, and
                    # options apply to every column family it creates.
                    logger.debug(
                        'No column families to list for the store at "%s" (%s); '
                        "it will be created by this open.",
                        self._path,
                        str(exc).strip(),
                    )
                    return None

                # There IS a database and we could not read its column families.
                # Do NOT open without them: rocksdict would then derive them from
                # the persisted OPTIONS file and apply its own defaults -- an
                # 8 MiB block cache and no bloom filters for every existing
                # column family, for the lifetime of the process. That is exactly
                # the silent degradation this whole change exists to remove, and
                # it is invisible once the open succeeds.
                #
                # Propagate instead. A listing failure is normally transient, and
                # rocksdict reports it as "IO error: ...", which is what
                # ``_init_rocksdb`` already retries with backoff -- so a blip
                # self-heals on the next attempt with the options intact, and only
                # a persistent fault fails the open, loudly.
                logger.warning(
                    'Could not list the column families of the store at "%s" '
                    "(%s); not opening it without them, because the configured "
                    "RocksDB options would not reach any pre-existing column "
                    "family.",
                    self._path,
                    str(exc).strip(),
                )
                raise

        # ``_existing_column_families()`` is called inside ``create_rdict``, NOT
        # hoisted: the corruption path below destroys the database and calls
        # ``create_rdict()`` again, and that second attempt must re-read the CF
        # list (now absent) rather than reuse the pre-destroy one.
        # ``_init_rocksdb`` likewise re-invokes this method per lock retry.
        def _open(cfs: Optional[Dict[str, Options]]) -> Rdict:
            if cfs is None:
                # Omitting the argument is the pre-existing open. Note it is NOT
                # a safe fallback in general: rocksdict then derives the column
                # families from the persisted OPTIONS file, and if that read
                # fails it silently degrades to default-CF-only, so the open
                # fails with "Column families not opened" rather than an
                # "IO error: ..." that the caller would retry. Supplying the
                # list explicitly survives that, which is why the caller below
                # re-reads and tries the other form.
                return Rdict(
                    path=self._path,
                    options=options,
                    access_type=AccessType.read_write(),
                )
            return Rdict(
                path=self._path,
                options=options,
                column_families=cfs,
                access_type=AccessType.read_write(),
            )

        def create_rdict() -> Rdict:
            cfs = _existing_column_families()
            try:
                return _open(cfs)
            except Exception as exc:
                if "Column families not opened" not in str(exc):
                    raise
                # Two ways to land here, and this message matches neither the
                # corruption nor the io-error gate, so without a retry it is an
                # un-retried fatal open:
                #
                # 1. The list went stale between listing and opening --
                #    ``list_cf`` takes no LOCK and column families are created
                #    lazily at runtime, so a process sharing this store can add
                #    one inside that window. Re-reading picks the new one up.
                # 2. We opened WITHOUT the argument (``cfs is None``, the listing
                #    having failed) and rocksdict could not read the persisted
                #    OPTIONS, so it saw only the default CF. Supplying the list
                #    explicitly recovers a store the argument-less open cannot
                #    open at all.
                #
                # The list is re-read ONCE. If that read fails, or still yields a
                # list this store will not open with, the error propagates: the
                # only remaining move would be an argument-less open, and on a
                # store that has column families that is the silent 8 MiB /
                # no-bloom-filter degradation this whole change exists to remove.
                # Failing loudly on a store we cannot open correctly beats
                # serving it in the state the bug used to produce.
                retry = _existing_column_families()
                if retry is None:
                    logger.warning(
                        'Could not open the store at "%s" (%s), and its column '
                        "families could not be listed on the retry either; not "
                        "falling back to an open without them, because that "
                        "would silently drop the configured block cache and "
                        "bloom filters for every existing column family.",
                        self._path,
                        str(exc).strip(),
                    )
                    raise
                logger.warning(
                    'Could not open the store at "%s" (%s); re-read its column '
                    "families and retrying the open once.",
                    self._path,
                    str(exc).strip(),
                )
                return _open(retry)

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
                    # Every other exit from this loop logs and names the path;
                    # without this an unclassified open failure reaches the
                    # operator as a PartitionAssignmentError whose message may
                    # name a column family but never the store directory.
                    #
                    # ERROR, not WARNING: this is the terminal exit. Errors that
                    # carry their own actionable message (``RocksDBCorruptedError``
                    # says the store may be recoverable from the changelog) are
                    # left to speak for themselves rather than being contradicted
                    # by a line calling them unrecoverable.
                    if not isinstance(exc, QuixException):
                        logger.error(
                            'Failed to open rocksdb partition on "%s": %s',
                            self._path,
                            str(exc).strip(),
                        )
                    raise

                # Shared per-assign open budget: when the acquiring consumer has
                # spent its total RocksDB-open budget for this _on_assign, stop
                # retrying and re-raise the underlying lock error (same failure
                # semantics as retry exhaustion, just triggered by wall-clock).
                # NOT RocksDBOpenAborted - that is reserved for the graceful-stop
                # path, so a deadline overrun keeps today's restart
                # behavior, only sooner.
                if self._open_deadline is not None and self._open_deadline.expired():
                    logger.warning(
                        f"Open budget exhausted for rocksdb partition on "
                        f'"{self._path}"; giving up acquiring the lock after '
                        f"{attempt} attempt(s)."
                    )
                    raise

                if self._open_max_retries <= 0 or attempt >= self._open_max_retries:
                    raise

                logger.warning(
                    f'Failed to open rocksdb partition on "{self._path}", cannot '
                    f"acquire a lock (attempt {attempt}/{self._open_max_retries}). "
                    f"Retrying in {self._open_retry_backoff}sec."
                )

                attempt += 1

                # Bail before sleeping if the next backoff would cross the shared
                # open deadline, so we don't overrun the budget by a whole
                # backoff. Re-raise the lock error (not RocksDBOpenAborted).
                if self._open_deadline is not None:
                    remaining = self._open_deadline.remaining()
                    if remaining is not None and remaining <= self._open_retry_backoff:
                        logger.warning(
                            f"Open budget for rocksdb partition on "
                            f'"{self._path}" would be exceeded by the next retry; '
                            f"giving up acquiring the lock."
                        )
                        raise

                # Wait for the backoff, but bail out immediately if the
                # application is stopping so a lock-waiting instance stays
                # promptly killable instead of sleeping through every retry.
                if self._stop_event is not None:
                    if self._stop_event.wait(self._open_retry_backoff):
                        raise RocksDBOpenAborted(
                            f'Aborted opening rocksdb partition on "{self._path}": '
                            f"the application is stopping"
                        ) from exc
                else:
                    time.sleep(self._open_retry_backoff)

    def _update_changelog_offset(self, batch: WriteBatch, offset: int):
        batch.put(
            CHANGELOG_OFFSET_KEY,
            int_to_bytes(offset),
            self.get_column_family_handle(METADATA_CF_NAME),
        )
