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

from quixstreams.models import HeadersMapping
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
    _safe_decode_stamp,
    _ttl_to_ms,
)
from quixstreams.state.serialization import int_from_bytes, int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

from ..exceptions import ChangelogFlushError
from .exceptions import IncompatibleStateStoreError, RocksDBCorruptedError
from .metadata import (
    CHANGELOG_OFFSET_KEY,
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
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
# future spill-to-disk concern.
# At ~80 B per held key, this is ~800 MB — large enough to threaten a small
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
# ``ChangelogFlushError``. A generous total slice cap bounds a pathological
# ever-shrinking trickle so the loop always terminates.
#
# One slice; kept below the 30 s producer poll interval
# (``quixstreams.kafka.producer.PRODUCER_POLL_TIMEOUT``).
_BACKFILL_CHANGELOG_FLUSH_SLICE_S: float = 25.0
# Runaway cap: max slices before aborting even if the backlog keeps shrinking
# (~1000 s at the default slice). Zero-progress is the primary trip; this is the
# belt-and-braces bound against a broker that only ever delivers a trickle.
_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES: int = 40


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
        # Opt-in gate for adopting a store created on the v3.24.0 TTL preview.
        # Stored under a distinct attribute (NOT ``self._adopt_v3240_stamps``)
        # so it does not shadow the private :meth:`_adopt_v3240_stamps` method that
        # performs the adoption. When False (the default) a 100%-quorum stamp
        # detection logs a CRITICAL and stays legacy; when True it flips + adopts.
        # Read once at open, immutable thereafter.
        self._adopt_v3240_stamps_enabled: bool = self._options.adopt_v3240_stamps
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
        # Count of wallclock-expired stamped records dropped during this
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

        # Remember whether the partition was
        # ALREADY flipped on disk at open time, before any changelog replay could
        # set the runtime flag. ``complete_recovery`` uses this to treat an
        # offset-caught-up restart (no stamped record replayed this session, so
        # ``_recovery_saw_stamped`` stays False) of an already-flipped store as
        # saw-stamped-equivalent: its ``__ttl_backfill_pending__`` census is
        # genuine leftovers, not orphans, so it must be COMPLETED rather than
        # discarded. An un-flipped (pure-legacy) store keeps ``False`` and still
        # discards an orphan census.
        self._persisted_flipped_at_open: bool = self.uses_ttl_stamps

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
            # Drop any dead live-backfill bookkeeping left by the migration that
            # flipped this store (one-time; no-op once cleaned).
            self._cleanup_completed_backfill_bookkeeping()

        # Snapshot ONCE, at the end of ``__init__`` and
        # AFTER the open-time ``_cleanup_completed_backfill_bookkeeping`` above,
        # whether this partition opened with a non-empty live-backfill ledger —
        # the interrupted-live-backfill signature. Consulted by
        # :meth:`recover_from_changelog_message` to ledger a replayed crash-window
        # chunk so the resume census cannot double-wrap it.
        #
        # The ``__ttl_backfill_stamped__`` CF is a ``LOCAL_ONLY_CFS`` member (never
        # produced to the changelog), so a fresh-volume COLD restore always opens
        # with the CF absent → snapshot False → replay ledgers nothing → the
        # adopt / survivor-derived / offset-skip cold-restore paths are byte-for-byte
        # unchanged. Only an interrupted LIVE backfill (whose earlier chunks wrote
        # real ledger entries on this same volume) opens with the CF non-empty →
        # snapshot True. The ``list_column_families()`` guard short-circuits so a
        # pure-legacy store never creates the ledger CF (``_live_backfill_ledger_
        # has_any`` would ``get_or_create`` it); it is the same ``list_cf`` call
        # the cleanup paths already make. Snapshot-at-open (not a live re-probe)
        # is deliberate: a live probe is self-fulfilling — the first ledgered
        # record would make every subsequent one ledger too, re-introducing the
        # cold-restore false positive the gate exists to prevent.
        self._ledger_nonempty_at_open: bool = (
            TTL_BACKFILL_STAMPED_CF_NAME in self.list_column_families()
            and self._live_backfill_ledger_has_any()
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

    @property
    def adopt_v3240_stamps(self) -> bool:
        """
        Whether the opt-in v3.24.0 stamp adoption is enabled. When ``False``
        (default) a 100%-quorum stamp detection on recovery logs a CRITICAL and
        leaves the store legacy; when ``True`` it flips + adopts the stamps in
        place. Read-only mirror of ``RocksDBOptions.adopt_v3240_stamps`` for
        test/introspection parity.
        """
        return self._adopt_v3240_stamps_enabled

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
        # CF carries a single reserved marker produced flag-last when a migration
        # completes. Seeing it means the source store was definitively
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
        if type(self).uses_ttl_stamps and cf_name == "default":
            pending_handle = self.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)
            if ttl_stamped:
                # A header-true default-CF record. Mark that the partition
                # replayed at least one stamped record (the MIXED-detection
                # half) and drop any earlier legacy census entry for this key —
                # a later stamped write supersedes it (compaction ordering).
                self._recovery_saw_stamped = True
                batch.delete(key, pending_handle)
                if self._ledger_nonempty_at_open:
                    # This partition opened with a non-empty
                    # live-backfill ledger (interrupted-live-backfill signature).
                    # A crash between a chunk's changelog flush-confirm and its
                    # local write leaves that chunk on the changelog but absent
                    # from the ledger — so replaying it here would otherwise leave
                    # the census invariant (``on-disk − staged − ledger``) broken
                    # and the resume would re-census and DOUBLE-WRAP the (now
                    # already-stamped) key. Ledger the replayed key in the SAME
                    # WriteBatch as the value apply + index rebuild + offset
                    # advance (committed at the end of this method), restoring the
                    # invariant atomically. Idempotent (re-ledgering an existing
                    # member is a no-op put); never inspects value content. The
                    # handle is warm — the open-time ledger probe created/cached
                    # it whenever this gate is True. Gate False (cold restore on a
                    # fresh volume) skips this entirely.
                    batch.put(
                        key,
                        b"",
                        self.get_column_family_handle(TTL_BACKFILL_STAMPED_CF_NAME),
                    )
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
            # (MIXED changelog). It MUST land verbatim (no stamp wrap, no
            # index entry, no recovery-drop filter): its key was just censused
            # into the pending CF and the completion backfill
            # (:meth:`complete_recovery`) will stamp it at end of recovery.
            # Routing here on the per-record header — not on the latched flag —
            # is what keeps the leftover legacy records intact.
            batch.put(key, value, cf_handle)
        elif is_main_cf:
            stamped, stamp = self._normalize_replay_value(value)
            # Judge expiry against the current wallclock captured once per
            # recovery session, NOT against a stamp-ratcheted pseudo-clock
            # (the old ``recovery_now = self._high_water_ms`` ratchet collapsed
            # uniform-expiry backfilled stores). Capture lazily on the first
            # stamped default-CF replay.
            # The recovery wallclock is used ONLY as the latest-record-wins
            # replay-drop clock; it is NO LONGER seeded into the live
            # ``_high_water_ms`` clock. Post-recovery ``_high_water_ms`` is the
            # loaded value or
            # ``None`` and advances only on live event-time writes, so an
            # event-time-lagging workload never over-expires its own writes.
            # Sentinel-stamped entries are never compared and always survive.
            expired = False
            if stamp != SENTINEL_NEVER:
                if self._recovery_now_ms is None:
                    self._recovery_now_ms = self._now_ms()
                recovery_now_ms = self._recovery_now_ms
                expired = stamp <= recovery_now_ms
            if expired:
                # Already-expired against wallclock-at-recovery
                # (latest-record-wins). A compacted changelog can carry several
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
        - if the pending CF is empty → all-stamped / fully-migrated;
          nothing to complete. No-op.
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
            # Make the latest-record-wins wallclock-expired replay drops
            # observable — ONE aggregate INFO per partition per recovery (no
            # per-record logging). Emitted at the once-per-recovery finalize seam
            # before any early return; a non-zero count implies the partition
            # flipped, so no early-return branch below can skip a non-zero log.
            logger.info(
                "Recovery at path=%s dropped %d already-expired stamped record(s) "
                "during changelog replay (expired against recovery wallclock=%d "
                "ms; latest-record-wins).",
                self._path,
                self._recovery_expired_drops,
                self._recovery_now_ms or 0,
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
            # (some chunks committed + produced) and then flipped via changelog
            # replay on a warm restart, leaving un-stamped legacy leftovers below
            # the replayed offset range that were never censused. Resume the
            # backfill over the ledger complement and finish the migration.
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
        if self._all_pending_values_are_stamped():
            # Total-quorum stamp detection (evaluated AFTER the done-flag
            # short-circuit AND the warm-restart resume branch above, and BEFORE the
            # saw_stamped/survivor-derived disposition). A stock-v3.24.0 cold
            # restore has
            # saw_stamped=False and a full pending census whose values are all
            # 8B-stamped-but-header-less; a mixed v3.24.0→this-branch restore
            # leaves the same all-stamped leftovers.
            if self._adopt_v3240_stamps_enabled:
                # Opt-in self-heal. The operator asserted this is a genuine
                # v3.24.0 store: flip + keep values verbatim + rebuild the index +
                # discard the census (now chunked, see _adopt_v3240_stamps). Fires
                # before the survivor-derived path so a mixed v3.24.0 census is
                # adopted, not double-wrapped.
                self._adopt_v3240_stamps()
                return
            # Detection WITHOUT the opt-in flag. Do NOT flip. This all-stamped
            # census is the exact byte-shape of a stock v3.24.0 cold restore, but
            # it is INDISTINGUISHABLE from a legacy set_bytes() store whose values
            # begin with plausible-stamp bytes — auto-flipping the latter would
            # turn the first 8 bytes of every value into an expiry stamp and delete
            # the data on the next sweep. Log CRITICAL naming the flag, then take
            # the pure-legacy disposition: discard the orphan census and leave
            # every value verbatim (byte-identical / raw-readable). MUST discard-
            # and-return here rather than fall through — a mixed v3.24.0 census
            # reaching
            # the completion path would be double-wrapped. Discarding the
            # census (an O(1) pointer-CF drop) never touches default-CF user data.
            logger.critical(
                "Recovery at path=%s replayed a header-absent changelog whose %d "
                "censused value(s) ALL decode as 8-byte TTL stamps. This is the "
                "exact on-disk shape of a store created on the v3.24.0 TTL preview "
                "(stamped values, no __ttl_stamped__ header) -- BUT it is "
                "indistinguishable from a pre-TTL legacy store whose values happen "
                "to begin with 8 plausible big-endian bytes (e.g. epoch-ms or "
                "counters written via set_bytes()). Automatic adoption is DISABLED "
                "by default: adopting a legacy store by mistake would turn the "
                "first 8 bytes of every value into an expiry stamp and delete the "
                "data on the next sweep. NOTHING has been changed -- the store "
                "stays in legacy mode and every value reads back byte-identical. "
                "If you are CERTAIN this store was created on v3.24.0, set "
                "RocksDBOptions(adopt_v3240_stamps=True) and restart to adopt the "
                "stamps in place. If this is a genuine pre-TTL store, leave the "
                "option unset -- this message is informational and safe to ignore.",
                self._path,
                self._count_backfill_pending(),
            )
            self._discard_backfill_pending()
            return
        if not self._recovery_saw_stamped and not self._persisted_flipped_at_open:
            # No __ttl_stamped__ record was replayed this session AND the store
            # was not already flipped on disk at open, so there is no migration to
            # complete: a pure-legacy first-enablement (owned by the live
            # first-``ttl=``-write backfill) or an all-legacy replay on an
            # un-flipped store. Either way the class-level census gate may have PUT
            # header-absent legacy keys into __ttl_backfill_pending__ (MIXED
            # detection has to census BEFORE the first stamped record can
            # distinguish them). Those entries are orphans with no completion pass
            # to drain them, so discard them now — a pure-legacy store then ends
            # recovery with an empty/absent pending CF and no lasting overhead.
            #
            # A store that was ALREADY flipped on
            # disk at open is handled by the ``_persisted_flipped_at_open`` guard
            # above — it falls through to completion even with
            # ``_recovery_saw_stamped`` False. That is the offset-caught-up-restart
            # crash window: the last changelog message was replayed in a prior run
            # (so this run replays nothing new and never re-sets the latch), but
            # the durable pending census holds genuine leftovers that MUST be
            # completed, not discarded. Only an UN-flipped store discards here.
            #
            # Loud detection runs FIRST, before the census is
            # dropped: no done-flag and no stamped record was seen, yet the census
            # is non-empty. That is normally a genuine pure-legacy (v3.23.6) store
            # — but it is ALSO the exact shape of a stock-v3.24.0 changelog
            # (8-byte-stamped values, no ``__ttl_stamped__`` header, no index
            # records — no structural discriminator exists). The 100%-quorum case
            # (EVERY censused value validates) is handled ABOVE at the all-stamped
            # gate: it
            # logs a CRITICAL naming ``adopt_v3240_stamps`` and returns without
            # flipping (or adopts when the flag is set), so it never reaches here.
            # This WARN therefore covers only the sub-100% "looks-like-but-not-all"
            # case: sample the censused values and, if most look like stamps, WARN
            # that this LOOKS like a v3.24.0 cold restore whose values may read
            # back with a spurious 8-byte prefix. DETECTION ONLY: the
            # sampled bytes gate the log line and nothing else — no value is
            # stripped, flipped, or re-routed, and no automatic repair is ever
            # applied without the explicit opt-in flag.
            self._warn_if_looks_like_v3240_upgrade()
            self._discard_backfill_pending()
            return
        if not self.uses_ttl_stamps:
            # Defensive: a stamped record was seen but the partition is somehow
            # not flipped (the same header flips it, so this is unreachable in
            # practice). Do not run completion on an unflipped partition.
            return

        pending_count = self._count_backfill_pending()
        if pending_count == 0:
            # All-stamped / fully-migrated changelog: nothing leftover.
            return

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
            expires_at_ms = self._recovery_now_ms + _ttl_to_ms(legacy_records_ttl)
            logger.info(
                "Recovery: completing interrupted legacy-TTL migration at "
                "path=%s; %d leftover legacy record(s) will be stamped with "
                "expiry=%d (wallclock-at-rebuild + legacy_records_ttl).",
                self._path,
                pending_count,
                expires_at_ms,
            )
        elif (
            survivor_expiry := (
                self._recovery_max_survivor_expiry_ms
                if self._recovery_max_survivor_expiry_ms is not None
                else self._max_index_stamp_ms()
            )
        ) is not None:
            # Survivor-derived default (config absent): align the leftovers
            # with the max surviving future stamp of their backfill cohort. A
            # single uniform value, in the future by construction (the
            # latest-record-wins filter already
            # dropped past-dated stamps during replay, so a tracked survivor is
            # always > wallclock-at-rebuild).
            #
            # On the offset-caught-up restart there
            # is NO replay, so ``_recovery_max_survivor_expiry_ms`` is unset. Fall
            # back to the max ON-DISK ``__ttl_index__`` stamp — the surviving
            # stamped cohort persisted from the interrupted run — so the leftovers
            # still inherit the cohort's window instead of degrading to
            # SENTINEL_NEVER.
            expires_at_ms = survivor_expiry
            logger.warning(
                "Recovery: completing interrupted legacy-TTL migration at "
                "path=%s WITHOUT legacy_records_ttl configured; %d leftover "
                "legacy record(s) will be stamped with expiry=%d, derived from "
                "the max surviving stamped record (the original flip ttl is not "
                "recoverable on a cold restore). To pin a different uniform "
                "window, set RocksDBOptions(legacy_records_ttl=timedelta(...)) "
                "and redeploy.",
                self._path,
                pending_count,
                expires_at_ms,
            )
        else:
            # All-expired fallback (config absent AND no surviving future
            # stamp): stamping with a past/derived expiry would mass-delete the
            # leftovers on the next sweep (silent data loss — forbidden, Quix
            # Cloud has no state reset). Keep them with SENTINEL_NEVER instead and
            # WARN loudly. Rare corner: interrupted backfill + config removed +
            # cold restore + every stamp already expired.
            expires_at_ms = SENTINEL_NEVER
            logger.warning(
                "Recovery: completing interrupted legacy-TTL migration at "
                "path=%s WITHOUT legacy_records_ttl configured and with NO "
                "surviving future stamp to derive from; %d leftover legacy "
                "record(s) will be stamped as never-expiring (SENTINEL_NEVER) "
                "to avoid silently deleting them. To assign a finite expiry, set "
                "RocksDBOptions(legacy_records_ttl=timedelta(...)) and redeploy.",
                self._path,
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
            )
            # Confirm the marker is durably on the changelog BEFORE the local
            # write; a stuck broker raises rather than marking the store done
            # ahead of the changelog (which would defeat the once-only guarantee
            # on a later cold rebuild that never sees the marker).
            self._flush_backfill_changelog(self._changelog_producer)
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
        exists — an in-place :meth:`backfill_legacy_records` that committed some
        chunks (stamps + ledger + produced records) but crashed before the
        flag-last flip, then flipped via changelog replay on this warm restart. The
        un-stamped legacy leftovers sit below the replayed offset range and were
        never censused, so they must be finished here.

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
          replay; else
        - ``SENTINEL_NEVER`` + a WARN in the degenerate case (ledger present but no
          surviving future stamp and an empty index) — never a past/derived expiry
          that would mass-delete the leftovers on the next sweep.

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

        # Step 2 — derive the uniform expiry (survivor-derived).
        if self._recovery_max_survivor_expiry_ms is not None:
            expires_at_ms = self._recovery_max_survivor_expiry_ms
        elif (max_index_stamp := self._max_index_stamp_ms()) is not None:
            expires_at_ms = max_index_stamp
        else:
            expires_at_ms = SENTINEL_NEVER
            logger.warning(
                "TTL legacy backfill RESUME at path=%s found an interrupted live "
                "migration (flipped, ledger non-empty, no done-marker) but NO "
                "surviving future stamp to derive the cohort's expiry from; "
                "stamping the un-stamped complement as never-expiring "
                "(SENTINEL_NEVER) to avoid silently deleting it on the next sweep. "
                "To assign a finite expiry, set "
                "RocksDBOptions(legacy_records_ttl=timedelta(...)) and redeploy.",
                self._path,
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
        gate, which emits a CRITICAL naming the opt-in ``adopt_v3240_stamps`` flag
        and either adopts (flag set) or stays legacy (flag unset) — it returns
        before reaching this WARN.

        HARD CONSTRAINT: the sampled bytes are used ONLY to
        decide whether to log — never to strip, flip, or re-route data. The
        values still land verbatim as legacy. Adoption (the only path that flips +
        rebuilds the index) is an EXPLICIT, operator-requested repair gated on
        ``adopt_v3240_stamps`` — it is never applied automatically, so the
        "no automatic repair" promise holds unconditionally in the default (no-
        flag) configuration. False positives (a genuine legacy store whose values
        start with plausible-stamp bytes) are benign; the message says it is
        heuristic.
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

    def _adopt_v3240_stamps(self) -> None:
        """
        Adopt the pending census as v3.24.0-stamped records (opt-in self-heal,
        gated by ``adopt_v3240_stamps`` at the :meth:`complete_recovery` gate). The
        total quorum has already been proven by
        :meth:`_all_pending_values_are_stamped`.

        Flip the partition into TTL mode, then for each pending key **keep the
        default-CF value verbatim** — it is already ``8B‖value`` on disk, so there
        is NO re-wrap (each record's own v3.24.0 stamp is preserved) — and, when
        its decoded stamp is not the sentinel, rebuild the ``__ttl_index__`` entry
        from that stamp so the bounded sweep can reclaim it. Discard the census and
        emit ONE INFO line. Adoption is local and idempotent: no changelog
        re-production (a later cold restore with the flag still set re-derives the
        same result). Expired adopted entries are reclaimed lazily by the live
        event-time sweep, never by a wallclock reclaim.

        **Chunked.** The index puts + pending deletes are committed in bounded
        chunks of ``legacy_backfill_chunk_size`` (no new constant), mirroring the
        proven :meth:`_complete_pending_backfill` skeleton minus the changelog
        produce/flush (adoption is changelog-silent). The per-chunk pending DELETE
        is the durable cursor: an interrupted adoption resumes over only the
        remaining keys on the next restore instead of redoing every index write.
        """
        if not self.uses_ttl_stamps:
            self.uses_ttl_stamps = True
            self.get_or_create_column_family(TTL_INDEX_CF_NAME)
            self._stamp_flip_metadata()

        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        default_cf = self.get_or_create_column_family("default")
        index_handle = self.get_column_family_handle(TTL_INDEX_CF_NAME)
        pending_handle = self.get_column_family_handle(TTL_BACKFILL_PENDING_CF_NAME)

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

        logger.info(
            "Recovery: adopted %d v3.24.0-stamped record(s) at path=%s (flipped "
            "into TTL mode; values kept verbatim, __ttl_index__ rebuilt from the "
            "adopted stamps).",
            adopted,
            self._path,
        )

    def _count_backfill_pending(self) -> int:
        """Count keys currently in the ``__ttl_backfill_pending__`` census CF."""
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        count = 0
        for _ in pending_cf.keys():
            count += 1
        return count

    def _backfill_pending_has_any(self) -> bool:
        """Cheap "is the pending census non-empty" probe: short-circuits on the
        first key instead of counting the whole CF."""
        pending_cf = self.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
        for _ in pending_cf.keys():
            return True
        return False

    def _live_backfill_ledger_has_any(self) -> bool:
        """Cheap "is the live-backfill stamped-ledger non-empty" probe:
        short-circuits on the first key instead of counting the whole CF. Mirrors
        :meth:`_backfill_pending_has_any` but scans the
        ``__ttl_backfill_stamped__`` ledger — the durable resume cursor of an
        interrupted in-place :meth:`backfill_legacy_records`."""
        ledger_cf = self.get_or_create_column_family(TTL_BACKFILL_STAMPED_CF_NAME)
        for _ in ledger_cf.keys():
            return True
        return False

    def _has_local_migration_done_marker(self) -> bool:
        """Whether the durable "migration done" marker is present on disk in the
        replicated ``__ttl_system__`` CF. Its presence means the
        migration completed, so nothing is left to finish."""
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
        - AND either completion track still has work:
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
        first check with no CF scans; the pending probe runs before the ledger
        probe (either satisfies the OR).
        """
        if not self.uses_ttl_stamps:
            return False
        if self._has_local_migration_done_marker():
            return False
        return self._backfill_pending_has_any() or self._live_backfill_ledger_has_any()

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

    def _flush_backfill_changelog(
        self, changelog_producer: Optional[ChangelogProducer]
    ) -> None:
        """
        Flush a backfill / recovery-completion chunk's changelog records with a
        PROGRESS-based bounded loop and fail loudly only when delivery stalls.

        Callers MUST invoke this AFTER producing a chunk's stamped records and
        BEFORE committing the chunk's local ``WriteBatch``: the stamped chunk has
        to be durably on the changelog before its stamps land locally, else a
        crash would leave the local store ahead of the changelog and a peer
        rebuild would diverge.

        The loop flushes in ``_BACKFILL_CHANGELOG_FLUSH_SLICE_S`` slices and:

        - returns as soon as a slice reports 0 remaining (fully delivered);
        - raises :class:`ChangelogFlushError` when a full slice makes NO delivery
          progress (``remaining >= prev``) — a wedged broker surfaces after ~2
          slices instead of a single fixed window, while a large-but-progressing
          chunk (``10 → 6 → 2 → 0``) keeps going because each slice strictly
          decreases the backlog;
        - raises at ``_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES`` (runaway cap) so an
          ever-shrinking trickle still terminates;
        - returns without blocking on a non-int return (an unconfigured test
          double), matching the pre-existing "flush and proceed" behavior.

        The timeout thus measures *lack of progress*, not *total time*, so it is
        robust to a large ``legacy_backfill_chunk_size``. The flush routes
        through the migration path (the dedicated non-transactional producer under
        exactly-once, else the main producer), so a confirmed flush means durable
        BEFORE the caller's local commit.
        """
        if changelog_producer is None:
            return
        prev: Optional[int] = None
        for slice_no in range(_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES):
            remaining = changelog_producer.flush(
                timeout=_BACKFILL_CHANGELOG_FLUSH_SLICE_S, migration=True
            )
            # A non-int return (e.g. an unconfigured test double) is indeterminate:
            # do not block the local commit (pre-existing behavior for such cases).
            if not isinstance(remaining, int):
                return
            if remaining == 0:
                return
            if prev is not None and remaining >= prev:
                raise ChangelogFlushError(
                    f"{remaining} legacy-TTL backfill changelog record(s) made no "
                    f"delivery progress in a full {_BACKFILL_CHANGELOG_FLUSH_SLICE_S}s "
                    f"slice at path={self._path}; aborting before the local commit "
                    f"so the local store never gets ahead of the changelog."
                )
            logger.debug(
                "TTL backfill changelog flush progress: %d remaining after "
                "slice %d (path=%s)",
                remaining,
                slice_no + 1,
                self._path,
            )
            prev = remaining
        raise ChangelogFlushError(
            f"legacy-TTL backfill changelog still has {prev} undelivered "
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
        that is already flipped into TTL mode. A completed in-place live backfill
        (:meth:`backfill_legacy_records`) leaves two dead artifacts behind: the
        ``__ttl_backfill_stamped__`` ledger CF and the ``__ttl_backfill_progress__``
        counter. Once the migration is genuinely done the backfill never re-runs,
        so neither is ever consulted again; drop them so a migrated store carries
        no lasting overhead (parallels the pending-CF hygiene on the recovery
        path).

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

        Then gated on the progress counter's presence so the common path
        (empty-store flip / already-cleaned) does exactly one extra metadata read
        and no-ops, and the work runs at most once (the counter is deleted here).
        """
        if not self._has_local_migration_done_marker():
            # Interrupted (flipped-but-unfinished) migration: keep the resume
            # ledger + progress counter for :meth:`complete_recovery`.
            return
        metadata_cf = self.get_or_create_column_family(METADATA_CF_NAME)
        if metadata_cf.get(TTL_BACKFILL_PROGRESS_KEY, default=None) is None:
            return
        self._drop_local_cf_if_exists(TTL_BACKFILL_STAMPED_CF_NAME)
        batch = WriteBatch(raw_mode=True)
        batch.delete(
            TTL_BACKFILL_PROGRESS_KEY,
            self.get_column_family_handle(METADATA_CF_NAME),
        )
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
                        key=key, value=stamped, headers=headers, migration=True
                    )
                # Confirm the chunk is durably on the changelog BEFORE the local
                # commit; a stuck broker raises rather than writing local-ahead.
                self._flush_backfill_changelog(self._changelog_producer)

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
        key_list: list[bytes] = sorted(
            cast(bytes, key)
            for key in default_cf.keys()
            if cast(bytes, key) not in staged_default_keys
            and cast(bytes, key) not in stamped_ledger
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
                        key=key, value=stamped, headers=headers, migration=True
                    )
                self._flush_backfill_changelog(changelog_producer)

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
            self._high_water_ms = int_from_bytes(cast(bytes, raw))
        except Exception:
            logger.warning(
                "Failed to decode persisted TTL high-water at %s; "
                "treating it as undefined.",
                self._path,
            )

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
