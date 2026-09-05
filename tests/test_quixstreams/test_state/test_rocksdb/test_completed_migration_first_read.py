"""
Unit tests for the FIRST READ AFTER A COMPLETED legacy-TTL migration:
sc-74843 round 3, reproduced live in Quix Cloud on 2026-09-04.

MECHANISM (two sentences). ``complete_recovery`` re-stamped every leftover
legacy record, produced the durable migration-done marker and dropped its
bookkeeping, but never persisted ``__ttl_enabled__`` -- it relied on whichever
earlier path had flipped the partition in memory to have written the flag -- so a
store whose flip flag was never recorded (or was recorded and later lost) could
finish its migration and be reopened in LEGACY mode over values that are now all
TTL-stamped. Nothing at open claimed that shape: the migration bookkeeping was
gone, ``has_incomplete_ttl_migration`` answers False on a marked store so an
offset-caught-up reopen runs NO recovery pass at all, and the first
``state.get()`` handed ``8B||json`` to the value deserializer --
``StateSerializationError`` (orjson "str is not valid UTF-8: surrogates not
allowed") three seconds after "Assigned store partition", on every restart.

The live sequence, verbatim (replica 1, 10:21Z then 10:25Z):

    Recovery: legacy-TTL migration completion progress: 538029 / 538029 ...
    Recovery: completed legacy-TTL migration at path=...
    Recovery process complete! Resuming normal processing...
    StateSerializationError: Failed to deserialize value:
        "b'\\x00\\x00\\x01\\xa0l\\x1d\\xa0@{"status":"ON",...}'"

The stamp ``0x000001a06c1da040`` is a FRESHLY re-stamped value -- written by the
completion that had just been watched succeed -- read back through the legacy
deserializer.

SCOPE. This is not a cold-start defect. The done-marker is REPLICATED through
the changelog, so the run4 completion appended ~1.07M header-true records plus
the marker to the changelog, and every WARM copy of that store replayed the tail
-- which is how all four replicas crash-looped on a plain warm restart at the
platform state path (live 10:41-10:43Z). Three entry points can each leave
"stamped values on disk, no ``__ttl_enabled__``", and the tests below cover all
three:

1. the RECOVERY-COMPLETION seam: the completion re-stamped every leftover and
   produced the marker without persisting the flip;
2. the MARKER-REPLAY seam: the marker is a ``__ttl_system__`` record, so the
   header-true flip-discovery branch (keyed on the default CF) cannot claim it
   and it was written into the local system CF of a store still flagged LEGACY.
   Everything that would have flipped that store afterwards is skippable -- a
   recovery loop that dies, a revoked partition, or a warm store that is caught
   up and runs no recovery pass at all;
3. the LIVE-FLIP seam: ``_maybe_flip_or_reject`` sets the runtime flag at PREPARE
   while the flag bytes ride the transaction cache and land at COMMIT, so an
   abort between the two leaves the object flipped with nothing on disk and no
   later transaction re-stages the flag. Live 10:25:35Z: a correctly stamped,
   header-true changelog record produced at .359 and a
   ``StateSerializationError`` at .384, in one container on one partition.

The fix under test, four parts:

* ``_produce_migration_done_marker`` persists the flip (``__ttl_enabled__`` +
  ``__ttl_format_version__``) BEFORE it produces or writes the marker, so the
  on-disk invariant "done-marker present => flip flag present" holds at every
  crash point;
* ``recover_from_changelog_message`` flips + persists the moment the marker is
  APPLIED, not at end of recovery, so no skipped finalize step can lose it;
* case 2 of the open-time resolution claims the residual window (marker on disk,
  flag absent): done-marker present + flag absent + no rollback lever + not
  provisionally adopted -> flip and persist, WITHOUT the bounded value sample
  that case 4's repair gates on. The sample is the wrong gate for a completed
  store: a finished dedup store whose short stamps have all gone past has no live
  stamp to show, and refusing to flip it leaves every value unreadable;
* ``RocksDBStorePartition.write`` re-asserts the flag into any batch it commits
  while the flag is not known to be committed, so a flipped partition can never
  persist a stamped value without the flag that makes it readable;
* the open-time resolution is re-ORDERED into a strength-of-evidence order --
  explicit operator intent (rollback, force-flip) and durable proof (the
  done-marker) now outrank inference from on-disk bytes. The ``ttl_force_flip``
  lever used to be the LAST arm of the ``elif`` chain, which made it unreachable
  for any store carrying a migration artifact, because the sample-gated repair
  claimed such a store first and declined on an all-past sample. That is why the
  lever demonstrably did not rescue the live warm stores at 11:55Z.

RED/GREEN on ``9a26227f`` (the unfixed baseline for this defect):

* ``test_first_read_after_completed_migration_returns_the_payload`` -- GREEN
  pre-fix. The in-session read uses the partition object the completion flipped
  in memory, so it never went through the legacy path. Kept as the regression
  guard for the seam the live log pointed at first;
* ``test_completion_persists_the_flip_flag_and_format_marker`` -- RED pre-fix
  (the completion writes neither key);
* ``test_flip_is_persisted_before_the_done_marker`` -- RED pre-fix (same);
* ``test_reopen_after_completed_migration_needs_no_repair`` -- RED pre-fix: the
  reopen survives only because case 4's repair rescues it off the leftover
  bookkeeping, which is luck, not ownership -- strip the bookkeeping (as the
  completion's own cleanup does) and the rescue is gone;
* ``test_done_marker_without_flag_is_flipped_on_open`` -- RED pre-fix with the
  LIVE exception: ``StateSerializationError`` out of ``state.get()``;
* ``test_done_marker_flip_respects_the_rollback_lever`` -- GREEN pre-fix, pins
  that the new case does not undo an explicit rollback;
* ``test_rolled_back_live_stamp_still_raises_the_actionable_error`` -- GREEN
  pre-fix (round 2 added the arming); pins that the new case is not "fixed" by
  weakening the guard instead of flipping;
* ``test_force_flip_still_wins_when_the_marker_is_gone`` -- GREEN pre-fix; case 2
  must not shadow the operator lever when there is no marker;
* ``test_force_flip_lever_is_not_outvoted_by_the_stamp_sample`` -- RED pre-fix.
  The live lever failure: ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`` was set on all
  four replicas at 11:55Z, its INFO fired on every open, and every replica still
  crashed -- because the lever was the LAST arm of the ``elif`` chain and the
  artifact-driven repair claimed the store first (the replicated
  ``__ttl_system__`` CF is an artifact) and then declined on the all-past sample;
* ``test_live_lever_shape_is_claimed_by_the_marker_without_the_lever`` -- RED
  pre-fix; the same shape must recover with NO operator action at all;
* ``test_force_flip_lever_flips_an_empty_store`` -- pins "unconditional" against
  a future re-tightening of the evidence gate;
* ``test_two_partitions_each_flip_independently`` -- RED pre-fix. One replica
  held ``default[0]`` and ``default[1]`` during the live rollout churn, so the
  fix must be per-partition state, not a process-wide latch;
* ``test_flip_survives_repeated_reopens`` -- RED pre-fix at the first reopen; the
  live store reported ``reopens=19``, so the repair must be idempotent;
* ``test_replayed_done_marker_persists_the_flip_immediately`` -- RED pre-fix;
* ``test_marker_replay_interrupted_before_complete_recovery`` -- RED pre-fix
  with the live exception;
* ``test_full_real_changelog_tail_survives_reopen`` -- GREEN pre-fix, and
  deliberately so: the real tail contains header-true completion records, and
  the pre-existing flip-discovery branch flips on the first of them, so the full
  sequence never needed the marker to own anything. The live warm replicas were
  the variant where those records had already been applied in an earlier cycle,
  leaving the marker as the only new record -- the two tests above;
* ``test_a_committed_stamped_write_persists_the_flip_flag`` -- RED pre-fix twice
  (flag absent after the flush; ``StateSerializationError`` after the reopen);
* ``test_legacy_partition_writes_no_ttl_metadata`` -- GREEN pre-fix; the guard
  that the ``write()`` re-assert leaves the 99% legacy workload untouched.
"""

import dataclasses
import logging
from datetime import timedelta
from typing import Optional

import pytest
from rocksdict import WriteBatch

from quixstreams.state.exceptions import (
    ChangelogFlushError,
    StateMigrationError,
    StateSerializationError,
)
from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_ADOPT_BACKUP_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ADOPT_PENDING_KEY,
    TTL_BACKFILL_IN_PROGRESS_KEY,
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_ENABLED_KEY,
    TTL_FORCE_FLIP_ENV_VAR,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
    TTL_ROLLBACK_ENV_VAR,
)
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

PREFIX = b"pfx"

# Two stamp regimes, both fixed constants rather than ``now +/- delta``: the
# open-time evidence gate and the read guard compare stamps against the REAL
# wallclock inside ``__init__``, before a test can patch ``_now_ms``, so a stamp
# derived from a hardcoded "now" would drift into the wrong regime on a calendar
# date instead of on a code change.
#
# FUTURE (~2096): the shape whose values a bounded sample recognises as live, so
# case 4's repair would flip them. Used where the point is the completion seam.
FUTURE_STAMP_MS = 4_000_000_000_000
# PAST (~2023-11): a COMPLETED dedup store whose 1h stamps have all expired but
# whose records the sweep has not reclaimed yet. This is the regime case 4's
# sample-based repair REFUSES (rightly -- on a genuine legacy store those bytes
# are 8-byte epoch-ms dedup values), which is why the done-marker case must not
# consult the sample.
PAST_STAMP_MS = 1_700_000_000_000
# Recovery wallclock for the past-stamp seed, chosen so
# ``max(now + legacy_records_ttl, max surviving stamp)`` resolves to
# ``PAST_STAMP_MS`` and every completion-written stamp is past-dated too.
PAST_NOW_MS = 1_690_000_000_000
# Recovery wallclock for the future-stamp seed (any value below the survivors).
FUTURE_NOW_MS = 1_780_000_000_000

# ``legacy_records_ttl`` exactly as the failing deployment had it: it is what
# routes a MIXED census into the wrap-once completion, which is the path under
# test here.
LEGACY_TTL_OPTIONS = RocksDBOptions(
    legacy_records_ttl=timedelta(hours=1),
    open_max_retries=0,
    open_retry_backoff=3.0,
)


class _FailingDeliveryProducer:
    """A ``ChangelogProducer``-shaped double that fires every recorded
    ``on_delivery`` with an ERROR on ``flush()`` and then returns a real ``0``.

    Same stub as ``test_done_marker_delivery_failure_raises.py``: ``0`` is the
    ``DRAINED_UNACKED`` signature (a failed-but-drained record does leave the
    send queue), so the raise under test comes from the partition's own confirm
    rather than from the stub.
    """

    changelog_name = "cl"
    partition = 0

    def __init__(self) -> None:
        self._pending: list = []
        self.produced_keys: list[bytes] = []

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers=None,
        migration: bool = False,
        on_delivery=None,
    ) -> None:
        self.produced_keys.append(key)
        self._pending.append(on_delivery)

    def flush(self, timeout: Optional[float] = None, migration: bool = False) -> int:
        pending, self._pending = self._pending, []
        for callback in pending:
            if callback is not None:
                callback(RuntimeError("simulated delivery failure"), None)
        return 0


def _raw_key(key_str: str, prefix: bytes = PREFIX) -> bytes:
    """The on-changelog / on-disk key for ``state.get(key_str)`` under ``prefix``."""
    return prefix + b"|" + json_dumps(key_str)


def _replay(partition, msgs) -> None:
    """Replay ``(key, value, ttl_stamped)`` default-CF changelog messages."""
    for offset, (key, value, ttl_stamped) in enumerate(msgs):
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )


def _flip_flag(partition):
    return partition.get_or_create_column_family(METADATA_CF_NAME).get(
        TTL_ENABLED_KEY, default=None
    )


def _format_marker(partition):
    return partition.get_or_create_column_family(METADATA_CF_NAME).get(
        STATE_FORMAT_VERSION_KEY, default=None
    )


def _delete_flip_flag(partition) -> None:
    """Delete ``__ttl_enabled__`` and ``__ttl_format_version__``, leaving every
    value and every other marker untouched.

    This is the disk shape the live crash needed: a store whose flip was never
    recorded, or was recorded and later removed, while its default CF is (about
    to be) fully TTL-stamped. Both keys go together because
    ``_stamp_flip_metadata`` writes them together and ``_has_warm_ttl_artifacts``
    reads the format marker as a warm signal -- leaving it behind would let case
    3 flip the store for the wrong reason and hide the defect under test.

    The handle and batch are released before returning; see
    ``_write_replicated_done_marker`` for why a leaked CF handle breaks the
    close/reopen these tests are built on.
    """
    batch = WriteBatch(raw_mode=True)
    handle = partition.get_column_family_handle(METADATA_CF_NAME)
    batch.delete(TTL_ENABLED_KEY, handle)
    batch.delete(STATE_FORMAT_VERSION_KEY, handle)
    partition._write(batch)
    del batch, handle


def _strip_to_done_marker_only(partition) -> None:
    """Reduce the store to the exact live disk shape: TTL-stamped values plus the
    durable done-marker, and nothing else.

    Everything the completion's own cleanup drops (or that a fresh volume never
    had) goes: the pending census, the resume ledger, the expiry index, the
    adoption backup, the flip flag, the format / high-water markers and the
    backfill cursors. ``__ttl_system__`` and its marker STAY -- that marker is
    the only evidence left, and the only thing case 2 keys on.
    """
    for cf_name in (
        TTL_BACKFILL_PENDING_CF_NAME,
        TTL_BACKFILL_STAMPED_CF_NAME,
        TTL_INDEX_CF_NAME,
        TTL_ADOPT_BACKUP_CF_NAME,
    ):
        partition._drop_local_cf_if_exists(cf_name)
    batch = WriteBatch(raw_mode=True)
    handle = partition.get_column_family_handle(METADATA_CF_NAME)
    for key in (
        TTL_ENABLED_KEY,
        STATE_FORMAT_VERSION_KEY,
        TTL_HIGH_WATER_KEY,
        TTL_BACKFILL_PROGRESS_KEY,
        TTL_BACKFILL_IN_PROGRESS_KEY,
        TTL_ADOPT_PENDING_KEY,
    ):
        batch.delete(key, handle)
    partition._write(batch)
    # Released before returning; see ``_write_replicated_done_marker``.
    del batch, handle

    # The shape is exactly what the live 10:25Z log showed: no warm-preview
    # signal (case 3 is vetoed by ``__ttl_system__`` anyway), no adoption
    # bookkeeping, and the ONLY migration artifact is the system CF holding the
    # done-marker. Pre-fix this store is owned by nobody.
    assert partition._has_warm_ttl_artifacts() is False
    assert partition._migration_artifacts_at_open() == TTL_SYSTEM_CF_NAME
    assert partition._v3240_adopt_artifacts_at_open() == ""
    assert partition._has_local_migration_done_marker() is True


def _pending_keys(partition) -> set:
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _default_cf(partition) -> dict:
    cf = partition.get_or_create_column_family("default")
    return {bytes(key): bytes(value) for key, value in cf.items()}


def _read(partition, key_str: str):
    """The user read path: ``state.get(key)``."""
    return partition.begin().get(key=key_str, prefix=PREFIX)


def _read_bytes(partition, key_str: str):
    """The user read path for raw values: ``state.get_bytes(key)``."""
    return partition.begin().get_bytes(key=key_str, prefix=PREFIX, default=None)


def _messages(caplog):
    return [record.getMessage() for record in caplog.records]


SURVIVORS = {"s0": "stamped-0", "s1": "stamped-1"}
LEFTOVERS = {"l0": "legacy-0", "l1": "legacy-1"}


def _replay_mixed(
    store_partition_factory,
    changelog_producer,
    name,
    stamp_ms,
    now_ms,
    options=LEGACY_TTL_OPTIONS,
):
    """Open a partition and replay a MIXED changelog: header-true stamped
    survivors (keys ``s*``) plus header-absent legacy leftovers (keys ``l*``).

    The header-true records flip the partition and persist the flag, exactly as
    production does; the header-absent ones are censused into
    ``__ttl_backfill_pending__`` as the completion's to-do list. The leftover
    payloads are JSON strings, so their leading 8 bytes are ``b'"legacy-'`` --
    far above the plausible-stamp ceiling -- and the census can never be
    mistaken for an already-stamped v3.24.0 one.
    """
    partition = store_partition_factory(
        name=name,
        options=options,
        changelog_producer=changelog_producer,
    )
    partition._now_ms = lambda: now_ms
    msgs = [
        (_raw_key(key), encode_ttl_value(stamp_ms, json_dumps(payload)), True)
        for key, payload in SURVIVORS.items()
    ]
    msgs += [
        (_raw_key(key), json_dumps(payload), False)
        for key, payload in LEFTOVERS.items()
    ]
    _replay(partition, msgs)

    assert partition.uses_ttl_stamps is True
    assert partition._recovery_saw_stamped is True
    assert _pending_keys(partition) == {_raw_key(key) for key in LEFTOVERS}
    return partition


def _complete_unflagged(
    store_partition_factory,
    changelog_producer,
    name,
    stamp_ms,
    now_ms,
):
    """Run a MIXED-changelog completion on a partition whose flip flag was lost
    just before ``complete_recovery()``, and return the still-open partition.

    Deleting the flag between the replay and the completion is what isolates the
    completion seam: the partition is flipped in MEMORY (so the completion runs
    normally and re-stamps every leftover) while the DISK says legacy. Whether
    the store survives the next open then depends entirely on whether the
    completion persisted the flip.
    """
    partition = _replay_mixed(
        store_partition_factory,
        changelog_producer,
        name=name,
        stamp_ms=stamp_ms,
        now_ms=now_ms,
    )
    _delete_flip_flag(partition)
    assert _flip_flag(partition) is None

    partition.complete_recovery()

    # The completion did its job: every leftover is stamped and the census is
    # drained, so the migration really is finished.
    assert _pending_keys(partition) == set()
    assert partition._has_local_migration_done_marker() is True
    return partition


class TestCompletionPersistsTheFlip:
    def test_first_read_after_completed_migration_returns_the_payload(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The seam the live log pointed at first: a read taken on the SAME
        partition object, immediately after the completion, with no reopen.

        GREEN on the unfixed code, and recorded as such: this read goes through
        the object the replay flipped in memory, so ``_stamps_enabled`` is True
        and the stamp is stripped correctly no matter what is on disk. Kept as a
        regression guard -- it is the assertion that would break if the
        completion ever left the in-memory flag and the values out of step.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="in-session",
            stamp_ms=FUTURE_STAMP_MS,
            now_ms=FUTURE_NOW_MS,
        )
        try:
            assert partition.uses_ttl_stamps is True
            for key, payload in SURVIVORS.items():
                assert _read(partition, key) == payload
            for key, payload in LEFTOVERS.items():
                assert _read(partition, key) == payload
        finally:
            partition.close()

    def test_completion_persists_the_flip_flag_and_format_marker(
        self, store_partition_factory, changelog_producer_mock
    ):
        """Completing a migration must leave ``__ttl_enabled__`` and
        ``__ttl_format_version__`` on disk, so the completed store can never be
        reopened unflagged.

        RED on the unfixed code: the completion path writes the done-marker and
        nothing else, so both keys are still absent when it returns.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="persisted-flip",
            stamp_ms=FUTURE_STAMP_MS,
            now_ms=FUTURE_NOW_MS,
        )
        try:
            assert _flip_flag(partition) is not None
            assert _format_marker(partition) is not None
        finally:
            partition.close()

    def test_flip_is_persisted_before_the_done_marker(self, store_partition_factory):
        """Ordering, pinned at the one crash point that can distinguish it: the
        flip flag must be durable BEFORE the marker, not after it and not in the
        same write.

        The producer fails every delivery, so the marker phase raises
        ``ChangelogFlushError`` between the two writes. Post-fix the flag is
        already on disk while the local marker is not -- the safe asymmetry: the
        store reopens flipped (reads correct) and UNMARKED, so recovery
        re-enters the idempotent completion. The reverse order would leave
        "migration done, never redo" over a store that reads as legacy, which is
        the crash loop itself.

        RED on the unfixed code: the flag is absent after the raise.
        """
        producer = _FailingDeliveryProducer()
        partition = _replay_mixed(
            store_partition_factory,
            producer,
            name="ordering",
            stamp_ms=FUTURE_STAMP_MS,
            now_ms=FUTURE_NOW_MS,
        )
        try:
            _delete_flip_flag(partition)
            assert _flip_flag(partition) is None

            with pytest.raises(ChangelogFlushError):
                partition._produce_migration_done_marker()

            assert producer.produced_keys, "the marker must have been produced"
            # The flip is durable even though the marker never landed.
            assert _flip_flag(partition) is not None
            assert _format_marker(partition) is not None
            # LOAD-BEARING, unchanged from
            # ``test_done_marker_delivery_failure_raises``: changelog-first still
            # holds, so the local store did not record "migration done" ahead of
            # the changelog.
            assert partition._has_local_migration_done_marker() is False
        finally:
            partition.close()

    def test_reopen_after_completed_migration_needs_no_repair(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """A completed store reopens on its OWN persisted flag -- no open-time
        repair, no inferred flip -- and reads every value back.

        This is the property that matters operationally. Pre-fix the same reopen
        happens to work, but only because case 4's sample-based repair rescues it
        off the leftover ``__ttl_backfill_pending__`` / ``__ttl_system__``
        bookkeeping. That is luck: the bookkeeping is transient (the completion's
        own cleanup and the census discard drop it), the sample is bounded, and
        the rescue evaporates on a store whose stamps have gone past -- which is
        how the live deployment ended up unowned. Asserting that NO repair fires
        pins the flag as the store's own durable state.

        RED on the unfixed code: the reopen logs "Repaired an interrupted
        legacy-TTL migration ...", because the flag it should have loaded was
        never written.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="no-repair",
            stamp_ms=FUTURE_STAMP_MS,
            now_ms=FUTURE_NOW_MS,
        )
        partition.close()

        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="no-repair",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        try:
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            # Loaded, not inferred: neither open-time flip case ran.
            assert reopened._ttl_flag_repaired_at_open is False
            assert reopened._persisted_flipped_at_open is True
            assert reopened._ttl_stamped_reads_unflagged is False
            messages = _messages(caplog)
            repaired = [
                message
                for message in messages
                if "Repaired an interrupted legacy-TTL migration" in message
                or "found UNFLAGGED" in message
            ]
            assert repaired == [], repaired

            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
            for key, payload in LEFTOVERS.items():
                assert _read(reopened, key) == payload
        finally:
            reopened.close()


class TestUnflaggedCompletedMigrationOnOpen:
    def test_done_marker_without_flag_is_flipped_on_open(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """The exact live disk shape, 2026-09-04 10:25Z: a COMPLETED migration
        whose done-marker is on disk, whose bookkeeping is gone, whose flip flag
        is absent, and whose stamps have all gone past. Reopening it must flip,
        persist the flip, and read every value back.

        The past stamps are the point. Case 4's repair samples the default CF and
        refuses to flip when nothing in the sample is still live -- correct for a
        store that might be a genuine legacy 8-byte-epoch dedup set, and fatal
        here, where the done-marker already proves the values are stamped. So the
        done-marker case must not consult the sample.

        RED on the unfixed code with the LIVE exception: ``state.get()`` raises
        ``StateSerializationError`` (the arming guard does not fire either -- it
        only refuses stamps that are still LIVE, and these are past).
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="live-shape",
            stamp_ms=PAST_STAMP_MS,
            now_ms=PAST_NOW_MS,
        )
        on_disk = _default_cf(partition)
        _strip_to_done_marker_only(partition)
        partition.close()

        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="live-shape",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        try:
            # The crash, gone: FIRST assertion is the live read itself.
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
            for key, payload in LEFTOVERS.items():
                assert _read(reopened, key) == payload

            # Flipped, and the flip is DURABLE -- the next open loads the flag
            # instead of re-deriving it from the marker.
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            assert _format_marker(reopened) is not None
            assert reopened._ttl_stamped_reads_unflagged is False
            # A done-marker is durable this-branch evidence, so the flip counts
            # as persisted: ``complete_recovery`` must read the empty census as
            # "fully migrated" (BRANCH A) rather than re-survey it (BRANCH B).
            assert reopened._persisted_flipped_at_open is True
            assert reopened._ttl_flag_repaired_at_open is False
            # Values are never rewritten by the flip -- only the flag is.
            assert _default_cf(reopened) == on_disk

            messages = _messages(caplog)
            assert any("found UNFLAGGED" in message for message in messages), messages
        finally:
            reopened.close()

    def test_done_marker_flip_respects_the_rollback_lever(
        self, store_partition_factory, changelog_producer_mock, monkeypatch
    ):
        """``QUIXSTREAMS_STATE_TTL_ROLLBACK=1`` is an explicit "keep this store
        legacy" instruction, so the done-marker flip must not undo it on the next
        restart -- the same exclusion case 4's repair already honours.

        The store stays legacy and every value reads back BYTE-IDENTICAL through
        ``get_bytes``, which is the whole contract of the lever. GREEN pre-fix
        (nothing flipped there either); it exists so the new case cannot grow
        into a silent override.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="rolled-back",
            stamp_ms=PAST_STAMP_MS,
            now_ms=PAST_NOW_MS,
        )
        on_disk = _default_cf(partition)
        _strip_to_done_marker_only(partition)
        partition.close()

        monkeypatch.setenv(TTL_ROLLBACK_ENV_VAR, "1")
        reopened = store_partition_factory(
            name="rolled-back",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is False
            assert _flip_flag(reopened) is None
            # Still ARMED off the ``__ttl_system__`` artifact, so a value that
            # decoded as a LIVE stamp would raise the actionable
            # ``StateMigrationError`` rather than reach the deserializer. These
            # stamps are past, so they read back verbatim instead.
            assert reopened._ttl_stamped_reads_unflagged is True
            for key in SURVIVORS:
                assert _read_bytes(reopened, key) == on_disk[_raw_key(key)]
            for key in LEFTOVERS:
                assert _read_bytes(reopened, key) == on_disk[_raw_key(key)]
        finally:
            reopened.close()

    def test_rolled_back_live_stamp_still_raises_the_actionable_error(
        self, store_partition_factory, changelog_producer_mock, monkeypatch
    ):
        """Companion to the lever test, with LIVE stamps: the rollback lever
        keeps the store legacy, and the armed guard then refuses to hand a
        still-live stamp to the deserializer.

        GREEN pre-fix (round 2 added the arming). It is here so the done-marker
        case cannot be "fixed" by weakening the guard instead of flipping.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="rolled-back-live",
            stamp_ms=FUTURE_STAMP_MS,
            now_ms=FUTURE_NOW_MS,
        )
        _strip_to_done_marker_only(partition)
        partition.close()

        monkeypatch.setenv(TTL_ROLLBACK_ENV_VAR, "1")
        reopened = store_partition_factory(
            name="rolled-back-live",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is False
            with pytest.raises(StateMigrationError) as raised:
                _read(reopened, "s0")
            assert not isinstance(raised.value, StateSerializationError)
            assert "stamped-0" not in str(raised.value)
        finally:
            reopened.close()

    def test_force_flip_still_wins_when_the_marker_is_gone(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The operator lever remains the escape hatch for a store with NO
        evidence at all -- the done-marker case narrows what needs the lever, it
        does not replace it.

        GREEN pre-fix; pinned because case 2 is ordered ahead of the lever's case
        and must not shadow it when the marker is absent.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="no-evidence",
            stamp_ms=FUTURE_STAMP_MS,
            now_ms=FUTURE_NOW_MS,
        )
        _strip_to_done_marker_only(partition)
        # Now drop the last piece of evidence too: no marker, no bookkeeping.
        partition._drop_local_cf_if_exists(TTL_SYSTEM_CF_NAME)
        assert partition._migration_artifacts_at_open() == ""
        partition.close()

        reopened = store_partition_factory(
            name="no-evidence",
            options=dataclasses.replace(LEGACY_TTL_OPTIONS, ttl_force_flip=True),
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
        finally:
            reopened.close()

    def test_force_flip_lever_is_not_outvoted_by_the_stamp_sample(
        self, store_partition_factory, changelog_producer_mock, monkeypatch, caplog
    ):
        """THE LIVE LEVER FAILURE, 2026-09-04 11:55-11:57Z.

        ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`` was confirmed on all four
        replicas, the lever's own INFO fired on every partition open, and every
        replica still died on the first ``state.get()``. The store shape: warm
        platform path, values stamped by v3.24.0 with a 1h TTL at ~10:15Z so
        every stamp was PAST by 11:55Z, the replicated done-marker present in
        ``__ttl_system__``, and NO migration bookkeeping.

        The mechanism was chain ORDER, not the lever itself. Any migration
        artifact -- and the ``__ttl_system__`` CF alone is one -- made
        ``repair_candidate`` True, so the artifact-driven repair claimed the store
        FIRST and then DECLINED, because its evidence gate demands a still-live
        stamp in the sample and every stamp here had expired. The lever's arm of
        the ``elif`` chain was never reached. An explicit operator instruction was
        silently outvoted by a heuristic sample.

        RED on the unfixed code: ``uses_ttl_stamps`` is False after the reopen,
        no flip is logged, and the read raises ``StateSerializationError`` -- the
        live crash, with the lever ON.

        Both INFOs are asserted so an operator can see the lever took effect: the
        lever-detected line and the flip line. Pre-fix only the first appears,
        which is exactly what made the live logs so hard to read.
        """
        partition = _seed_stamped_no_evidence(
            store_partition_factory,
            changelog_producer_mock,
            name="lever-live-shape",
            stamp_ms=PAST_STAMP_MS,
        )
        # The replicated done-marker, written straight into the system CF: the
        # live stores carried it and no other bookkeeping. Via the helper, not
        # inline: the raw CF handle must not outlive ``close()`` or the reopen
        # below loses the LOCK-file race (see the helper's docstring).
        _write_replicated_done_marker(partition)
        assert partition._has_local_migration_done_marker() is True
        assert _flip_flag(partition) is None
        partition.close()

        monkeypatch.setenv(TTL_FORCE_FLIP_ENV_VAR, "1")
        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="lever-live-shape",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        try:
            # The read never raises StateSerializationError again. An expired
            # value may legitimately read back as missing once a high-water
            # exists; here none does, so the payload comes back.
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload

            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            assert _format_marker(reopened) is not None

            messages = _messages(caplog)
            assert any(
                "TTL force-flip lever: ON" in message for message in messages
            ), messages
            # SOMETHING must record that a flip actually happened -- either the
            # lever's own line or the done-marker owner's, depending on which
            # claims this store. Pre-fix neither appears.
            flipped = [
                message
                for message in messages
                if "Forced TTL mode" in message or "found UNFLAGGED" in message
            ]
            assert flipped, messages
        finally:
            reopened.close()

    def test_live_lever_shape_is_claimed_by_the_marker_without_the_lever(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """The SAME live shape, reopened with NO lever set: case 2 must claim it
        off the replicated done-marker alone.

        This is the requirement that matters operationally -- nobody should need
        an env var to bring a completed store back. The lever is the belt; the
        marker is the braces.

        RED on the unfixed code: ``StateSerializationError``, because the
        artifact-driven repair declines on the all-past sample and nothing else
        owns the shape.
        """
        partition = _seed_stamped_no_evidence(
            store_partition_factory,
            changelog_producer_mock,
            name="marker-no-lever",
            stamp_ms=PAST_STAMP_MS,
        )
        _write_replicated_done_marker(partition)
        partition.close()

        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="marker-no-lever",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        try:
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            messages = _messages(caplog)
            assert any("found UNFLAGGED" in message for message in messages), messages
            # No lever was set, so the lever lines must be absent.
            assert not any("force-flip lever: ON" in m for m in messages), messages
        finally:
            reopened.close()

    def test_force_flip_lever_flips_an_empty_store(
        self, store_partition_factory, monkeypatch
    ):
        """The lever must not depend on the default CF being non-empty either: a
        bounded sample of an empty CF yields zero live stamps, which is the same
        input that made the sample-gated repair decline.

        RED-ish on the unfixed code: with no artifacts the lever's arm WAS
        reachable pre-fix, so this may already pass -- it is here to pin
        "unconditional" against a future re-tightening of the gate.
        """
        monkeypatch.setenv(TTL_FORCE_FLIP_ENV_VAR, "1")
        partition = store_partition_factory(
            name="lever-empty",
            options=LEGACY_TTL_OPTIONS,
        )
        try:
            assert partition.uses_ttl_stamps is True
            assert _flip_flag(partition) is not None
        finally:
            partition.close()

    def test_two_partitions_each_flip_independently(
        self, store_partition_factory, changelog_producer_mock
    ):
        """One replica held ``default[0]`` AND ``default[1]`` during the live
        rollout churn, and only one of them crashed -- so the flip must be
        per-partition state resolved from each store's own disk, never a
        process-wide latch.

        Two independent store directories are seeded into the same unflagged
        completed shape and reopened in sequence; both must flip and read.

        RED on the unfixed code: the first reopen raises
        ``StateSerializationError``.
        """
        for name in ("part0", "part1"):
            partition = _complete_unflagged(
                store_partition_factory,
                changelog_producer_mock,
                name=name,
                stamp_ms=PAST_STAMP_MS,
                now_ms=PAST_NOW_MS,
            )
            _strip_to_done_marker_only(partition)
            partition.close()

        for name in ("part0", "part1"):
            reopened = store_partition_factory(
                name=name,
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
            try:
                assert reopened.uses_ttl_stamps is True
                assert _flip_flag(reopened) is not None
                for key, payload in SURVIVORS.items():
                    assert _read(reopened, key) == payload
                for key, payload in LEFTOVERS.items():
                    assert _read(reopened, key) == payload
            finally:
                reopened.close()

    def test_flip_survives_repeated_reopens(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The live store reported ``reopens=19``, so the repair must be
        idempotent: the FIRST reopen persists the flip and every reopen after it
        must load the flag and do nothing (no re-derivation, no second flip).

        RED on the unfixed code at the first reopen.
        """
        partition = _complete_unflagged(
            store_partition_factory,
            changelog_producer_mock,
            name="reopens",
            stamp_ms=PAST_STAMP_MS,
            now_ms=PAST_NOW_MS,
        )
        _strip_to_done_marker_only(partition)
        partition.close()

        for _ in range(3):
            reopened = store_partition_factory(
                name="reopens",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
            try:
                assert reopened.uses_ttl_stamps is True
                assert _flip_flag(reopened) is not None
                assert _read(reopened, "s0") == SURVIVORS["s0"]
                assert _read(reopened, "l0") == LEFTOVERS["l0"]
            finally:
                reopened.close()


# The changelog record sequence read back from the live topic (p0/p1) on
# 2026-09-04: v3.24.0 live records with NO stamped header, then the ~538k
# header-true completion records, then the replicated done-marker, then
# header-true live records produced by later containers. Every WARM copy of the
# store replayed this tail, which is how the crash stopped being a cold-start
# defect and became "every store on the changelog".
V3240_LIVE = {"a0": "adopted-0", "a1": "adopted-1"}
COMPLETION = {"c0": "completed-0", "c1": "completed-1"}
LATER_LIVE = {"d0": "later-0"}


def _replay_real_changelog_tail(partition, stamp_ms) -> None:
    """Replay (a) header-ABSENT v3.24.0 live records, (b) header-true completion
    records, (c) the done-marker, (d) header-true later-container live records.

    Sequence and headers match the records read back from the live topic. The
    marker is a ``__ttl_system__`` record with no stamped header, which is why
    the ``cf_name == "default"`` flip-discovery branch cannot claim it.
    """
    offset = 0
    for key, payload in V3240_LIVE.items():
        partition.recover_from_changelog_message(
            key=_raw_key(key),
            value=encode_ttl_value(stamp_ms, json_dumps(payload)),
            cf_name="default",
            offset=offset,
            ttl_stamped=False,
        )
        offset += 1
    for key, payload in COMPLETION.items():
        partition.recover_from_changelog_message(
            key=_raw_key(key),
            value=encode_ttl_value(stamp_ms, json_dumps(payload)),
            cf_name="default",
            offset=offset,
            ttl_stamped=True,
        )
        offset += 1
    partition.recover_from_changelog_message(
        key=TTL_MIGRATION_DONE_KEY,
        value=int_to_bytes(STATE_FORMAT_VERSION),
        cf_name=TTL_SYSTEM_CF_NAME,
        offset=offset,
        ttl_stamped=False,
    )
    offset += 1
    for key, payload in LATER_LIVE.items():
        partition.recover_from_changelog_message(
            key=_raw_key(key),
            value=encode_ttl_value(stamp_ms, json_dumps(payload)),
            cf_name="default",
            offset=offset,
            ttl_stamped=True,
        )
        offset += 1


def _replay_marker_only(partition, offset=0) -> None:
    """Replay ONLY the replicated done-marker -- the whole of what a warm store
    that is already caught up on the data records receives.

    This is the live shape, not the full tail: the warm v3.24.0 replicas had
    applied everything before the marker in an earlier cycle, so the marker was
    the single new record, and there is no header-true default-CF record after it
    to flip the store by accident.
    """
    partition.recover_from_changelog_message(
        key=TTL_MIGRATION_DONE_KEY,
        value=int_to_bytes(STATE_FORMAT_VERSION),
        cf_name=TTL_SYSTEM_CF_NAME,
        offset=offset,
        ttl_stamped=False,
    )


def _seed_stamped_no_evidence(
    store_partition_factory, changelog_producer, name, stamp_ms
):
    """A WARM copy of a store whose default CF is TTL-stamped while this build
    never flipped it: no ``__ttl_enabled__``, no bookkeeping, nothing armed.

    Written with a raw batch rather than through a replay on purpose -- a replay
    of header-true records would flip and flag the store itself, which is exactly
    the signal these tests must NOT have available. This is the on-disk state of
    the v3.24.0 replicas that had been running all morning, and the state the
    replicated done-marker then arrived into.

    Returns the still-open partition.
    """
    partition = store_partition_factory(
        name=name,
        options=LEGACY_TTL_OPTIONS,
        changelog_producer=changelog_producer,
    )
    partition._now_ms = lambda: PAST_NOW_MS
    batch = WriteBatch(raw_mode=True)
    handle = partition.get_column_family_handle("default")
    for key, payload in SURVIVORS.items():
        batch.put(
            _raw_key(key),
            encode_ttl_value(stamp_ms, json_dumps(payload)),
            handle,
        )
    partition._write(batch)
    del batch, handle

    assert partition.uses_ttl_stamps is False
    assert _flip_flag(partition) is None
    assert partition._migration_artifacts_at_open() == ""
    assert partition._ttl_stamped_reads_unflagged is False
    return partition


def _write_replicated_done_marker(partition) -> None:
    """Write the replicated done-marker straight into ``__ttl_system__`` with a
    raw batch, bypassing changelog replay.

    This is the live warm-replica shape: the marker arrived on a store carrying
    no other migration bookkeeping, and a replay would have flipped the store by
    itself (see ``_seed_stamped_no_evidence``), which is the signal these tests
    must not have.

    RELEASE-BEFORE-CLOSE, and it is why this is a function rather than four
    inline lines in the caller. rocksdict documents that ``Rdict.close()`` "does
    not guarantee the underlying RocksDB to be actually closed. Other Column
    Family ``Rdict`` instances, ``ColumnFamily`` (cf handle) instances, iterator
    instances ... can all keep RocksDB alive." ``RocksDBStorePartition.close()``
    drops the handles the PARTITION owns (it clears ``_cf_handle_cache`` and
    ``_cf_cache`` for exactly this reason) but it cannot reach a
    ``ColumnFamily`` bound in a CALLER's local. A test that keeps one in its own
    frame and then reopens the same path holds the DB alive across the reopen and
    fails on the OS lock -- ``IO error: Failed to create lock file:
    <path>/LOCK`` on Windows -- inside ``Rdict(...)``, before a single assertion
    runs. ``LEGACY_TTL_OPTIONS`` sets ``open_max_retries=0`` on purpose, so
    nothing masks it.

    Scoping the handle and the batch to this frame drops both on return, which is
    the same discipline every other raw-batch helper here relies on
    (``_delete_flip_flag``, ``_strip_to_done_marker_only``,
    ``_seed_stamped_no_evidence``). The explicit ``del`` makes that a stated
    contract rather than an accident of where the lines happen to live.
    """
    handle = partition.get_column_family_handle(TTL_SYSTEM_CF_NAME)
    batch = WriteBatch(raw_mode=True)
    batch.put(TTL_MIGRATION_DONE_KEY, int_to_bytes(STATE_FORMAT_VERSION), handle)
    partition._write(batch)
    del batch, handle


class TestReplicatedDoneMarker:
    def test_replayed_done_marker_persists_the_flip_immediately(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The moment the replicated done-marker is applied, the store must be
        flipped AND flagged on disk -- before ``complete_recovery`` and before
        anything else can go wrong.

        The marker is a ``__ttl_system__`` record, so the header-true
        flip-discovery branch (keyed on the default CF) cannot claim it, and
        pre-fix it was written into the local ``__ttl_system__`` CF of a store
        still flagged LEGACY. Every downstream rescue is skippable: the
        end-of-recovery flip does not happen if the recovery loop dies or the
        partition is revoked first, and a warm store that is already caught up
        runs no recovery pass at all. That combination crash-looped all four
        replicas on a PLAIN WARM RESTART (live 10:41-10:43Z).

        RED on the unfixed code: nothing flips and ``__ttl_enabled__`` stays
        absent after the marker is applied, and the very next read of a stamped
        value raises ``StateSerializationError``.

        The marker is replayed ALONE, which is the live shape and the only shape
        that isolates this seam: a warm store that is caught up on the data
        records receives just the marker. Replaying it as part of the full tail
        would hide the defect, because a header-true default-CF record before or
        after it flips and flags the store on its own -- see
        ``test_full_real_changelog_tail_survives_reopen``.
        """
        partition = _seed_stamped_no_evidence(
            store_partition_factory,
            changelog_producer_mock,
            name="marker-replay",
            stamp_ms=PAST_STAMP_MS,
        )
        try:
            _replay_marker_only(partition)
            assert partition._recovery_saw_migration_done is True
            assert partition.uses_ttl_stamps is True
            assert _flip_flag(partition) is not None
            assert _format_marker(partition) is not None
            # The marker record itself still lands verbatim in the replicated
            # system CF (unchanged: warm opens read it from there).
            assert partition._has_local_migration_done_marker() is True
            # And the store is immediately readable, without waiting for
            # complete_recovery to flip it.
            for key, payload in SURVIVORS.items():
                assert _read(partition, key) == payload
        finally:
            partition.close()

    def test_marker_replay_interrupted_before_complete_recovery(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The marker is applied and then the recovery-finalize seam is never
        reached, so the flip has to have been persisted by the marker itself.

        Both halves of this happened live: one replica lost its recovery loop to
        a ``KeyError`` on a just-revoked partition, and a reassigned partition
        that is already caught up gets no recovery pass at all. The store is left
        with the marker local, the changelog offset advanced, and (pre-fix) no
        flip flag -- after which every restart reads stamped values in legacy
        mode.

        RED on the unfixed code with the LIVE exception:
        ``StateSerializationError``. Note the past stamps: with FUTURE stamps
        round 2's sample-based repair would rescue this reopen off the
        ``__ttl_system__`` CF the marker created, so the past-stamp regime is
        what proves the marker itself is now the owner.
        """
        partition = _seed_stamped_no_evidence(
            store_partition_factory,
            changelog_producer_mock,
            name="interrupted-marker",
            stamp_ms=PAST_STAMP_MS,
        )
        _replay_marker_only(partition)
        # No complete_recovery(): the finalize seam is exactly what was missed.
        partition.close()

        reopened = store_partition_factory(
            name="interrupted-marker",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
        finally:
            reopened.close()

    def test_full_real_changelog_tail_survives_reopen(
        self, store_partition_factory, changelog_producer_mock
    ):
        """End-to-end over the REAL record sequence read back off the live topic:
        (a) header-absent v3.24.0 live records, (b) header-true completion
        records, (c) the replicated done-marker, (d) header-true live records
        from later containers -- then ``complete_recovery()``, close, reopen,
        read every key.

        ``complete_recovery`` takes the done-marker early return here ("store
        fully migrated, no completion needed"), which is the line replica 3
        logged one second before it crashed on its OTHER partition.

        GREEN pre-fix, and recorded as such: step (b) contains header-true
        default-CF records, and the pre-existing flip-discovery branch flips and
        persists on the first of them, so this sequence never needed the marker
        to own anything. It is here as the end-to-end guard that the real
        sequence reads back after a reopen; the live warm replicas were the
        variant where (a)-(b) had already been applied in an earlier cycle, which
        the two tests above cover.
        """
        partition = store_partition_factory(
            name="full-tail",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        partition._now_ms = lambda: FUTURE_NOW_MS
        _replay_real_changelog_tail(partition, stamp_ms=FUTURE_STAMP_MS)
        partition.complete_recovery()
        partition.close()

        reopened = store_partition_factory(
            name="full-tail",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            for source in (V3240_LIVE, COMPLETION, LATER_LIVE):
                for key, payload in source.items():
                    assert _read(reopened, key) == payload
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
        finally:
            reopened.close()


class TestFlipFlagFollowsTheStampedWrites:
    def test_a_committed_stamped_write_persists_the_flip_flag(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The write/read asymmetry, isolated: a partition that is flipped in
        MEMORY with nothing on disk must not be able to commit a stamped value
        without committing ``__ttl_enabled__`` alongside it.

        How that state arises in production: the flush-time flip
        (``_maybe_flip_or_reject``) sets the runtime flag at PREPARE while the
        flag bytes ride the transaction cache and only land at COMMIT. An abort
        between the two -- rebalance, changelog produce/flush error, failed
        checkpoint -- leaves the object flipped with nothing persisted, and no
        later transaction re-stages the flag. Every write after that stamps
        inline and commits, so the store accumulates stamped values under an
        absent flag; the next partition object for that path opens LEGACY and
        dies on the first read. Live 10:25:35Z: a correctly stamped, header-true
        changelog record produced at .359, ``StateSerializationError`` at .384,
        one partition of one container.

        RED on the unfixed code twice over: the flag is absent after the flush,
        and the reopened read raises ``StateSerializationError``.
        """
        partition = store_partition_factory(
            name="aborted-flip",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        # Exactly what an aborted flip flush leaves behind: runtime flag True,
        # index CF present, and NOTHING in the metadata CF.
        partition.uses_ttl_stamps = True
        partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        assert _flip_flag(partition) is None

        # The live sequence: miss on a new key, stamped write, commit.
        assert _read(partition, "k") is None
        with partition.begin() as tx:
            tx.set(
                key="k",
                value="v",
                prefix=PREFIX,
                timestamp=FUTURE_NOW_MS,
                ttl=timedelta(hours=1),
            )
        # The flag now agrees with what the changelog record said about this
        # write (header-true == stamped == TTL mode).
        assert _flip_flag(partition) is not None
        assert _format_marker(partition) is not None
        assert _read(partition, "k") == "v"
        partition.close()

        reopened = store_partition_factory(
            name="aborted-flip",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is True
            assert _read(reopened, "k") == "v"
        finally:
            reopened.close()

    def test_legacy_partition_writes_no_ttl_metadata(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The other direction, and the one that must not regress: an unflipped
        legacy partition's flush stays byte-identical -- no ``__ttl_enabled__``,
        no format marker, no stamp.

        GREEN pre-fix. It is the guard on the re-assert added to ``write()``:
        that block is gated on ``uses_ttl_stamps``, so the 99% legacy workload
        must be untouched by it.
        """
        partition = store_partition_factory(
            name="stays-legacy",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            with partition.begin() as tx:
                tx.set(key="k", value="v", prefix=PREFIX, timestamp=FUTURE_NOW_MS)
            assert partition.uses_ttl_stamps is False
            assert _flip_flag(partition) is None
            assert _format_marker(partition) is None
            assert _default_cf(partition)[_raw_key("k")] == json_dumps("v")
            assert _read(partition, "k") == "v"
        finally:
            partition.close()


# The open-time decision INFOs of cases 2-5. A warm store that opens on a
# persisted flag must log NONE of them: it needs no decision. (Case 1, rollback,
# is absent because these tests set no lever.)
_DECISION_LINES = (
    "found UNFLAGGED",  # case 2, done-marker
    "force-flip lever: ON",  # case 3, operator lever detected
    "Forced TTL mode",  # case 3, the flip it performs
    "Detected v3.24.0 TTL store",  # case 4, warm signal
    "Repaired an interrupted legacy-TTL migration",  # case 5, sample-gated
)


class TestAPersistedFlagIsNeverUnTrusted:
    """A WARM v3.24.0 store is FLAGGED -- v3.24.0 persists ``__ttl_enabled__`` +
    ``__ttl_format_version__`` on its first ``ttl=`` flush -- so the live control
    store that upgraded cleanly to ``v3.25.1a1`` opened on its own flag with zero
    open-time decision lines, not via any repair.

    That reframes the regression class. It is NOT "unflagged warm v3.24.0 store".
    It is (a) the COLD restore of a header-absent changelog whose values are
    stamped, where the flag is local-only and therefore gone -- owned by
    ``_adopt_v3240_stamps`` and covered in ``test_v3240_auto_adopt.py``; and (b)
    whether anything can push an already-FLAGGED store back onto a legacy path.
    These two tests pin (b), which is the half nothing else asserts.

    The answer for the record: ``uses_ttl_stamps`` is cleared in exactly ONE
    place, ``_rollback_provisional_adopt``, reachable only with
    ``__ttl_adopt_pending__`` on disk AND the rollback lever set -- and that site
    is present identically in ``v3.25.1a1``. Every repair case gates on ``not
    self.uses_ttl_stamps``, so none of them, and neither the
    ``_has_warm_ttl_artifacts`` veto nor a missing done-marker nor a census, can
    demote a store that carries its flag.
    """

    def _seed_flagged_v3240(self, store_partition_factory, changelog_producer, name):
        """The warm v3.24.0 disk shape: stamped default CF, a local
        ``__ttl_index__``, and the flip flag + format marker v3.24.0 itself
        persisted. PAST stamps, so no value sample could rescue it.
        """
        partition = _seed_stamped_no_evidence(
            store_partition_factory,
            changelog_producer,
            name=name,
            stamp_ms=PAST_STAMP_MS,
        )
        partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        # Exactly what v3.24.0's first ttl= flush leaves behind.
        partition._stamp_flip_metadata()
        assert _flip_flag(partition) is not None
        return partition

    def test_flagged_warm_store_opens_flipped_with_no_decision_at_all(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """The live control, pinned: the flag alone carries the store, and no
        resolution case fires. GREEN on ``v3.25.1a1`` and here alike."""
        partition = self._seed_flagged_v3240(
            store_partition_factory, changelog_producer_mock, name="flagged-warm"
        )
        partition.close()

        with caplog.at_level(logging.INFO):
            reopened = store_partition_factory(
                name="flagged-warm",
                options=LEGACY_TTL_OPTIONS,
                changelog_producer=changelog_producer_mock,
            )
        try:
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
            assert reopened.uses_ttl_stamps is True
            # Loaded from disk, not inferred: no case ran.
            assert reopened._persisted_flipped_at_open is True
            assert reopened._ttl_flag_repaired_at_open is False
            assert reopened._ttl_stamped_reads_unflagged is False

            messages = _messages(caplog)
            for line in _DECISION_LINES:
                assert not any(line in message for message in messages), line
        finally:
            reopened.close()

    def test_the_veto_artifacts_cannot_demote_a_flagged_store(
        self, store_partition_factory, changelog_producer_mock
    ):
        """The same store carrying the artifacts that orphan an UNflagged one:
        the replicated done-marker in ``__ttl_system__`` (which vetoes
        ``_has_warm_ttl_artifacts``) plus a non-empty ``__ttl_backfill_pending__``
        census.

        Both are present, the warm probe is vetoed, and the store still opens
        flipped and readable -- because the flag is read before any of it and is
        never re-litigated.
        """
        partition = self._seed_flagged_v3240(
            store_partition_factory, changelog_producer_mock, name="flagged-vetoed"
        )
        _write_replicated_done_marker(partition)
        partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME).put(
            _raw_key("l0"), b""
        )
        assert partition._has_warm_ttl_artifacts() is False
        partition.close()

        reopened = store_partition_factory(
            name="flagged-vetoed",
            options=LEGACY_TTL_OPTIONS,
            changelog_producer=changelog_producer_mock,
        )
        try:
            assert reopened.uses_ttl_stamps is True
            assert _flip_flag(reopened) is not None
            assert reopened._persisted_flipped_at_open is True
            for key, payload in SURVIVORS.items():
                assert _read(reopened, key) == payload
        finally:
            reopened.close()
