"""
Red-first tests for three CONFIRMED bugs in the v3.24.0-stamp adoption state
machine.  Each test MUST fail (RED) on the current HEAD (``95a62399``); a fix
by ArchDev will turn them green.

Finding references match the review-round-4 numbering from
``dev-planning/state-ttl-v3240-auto-adopt/``.
"""

from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_ADOPT_BACKUP_CF_NAME,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import (
    TTL_ADOPT_PENDING_KEY,
    TTL_MIGRATION_DONE_KEY,
)
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000


# ---------------------------------------------------------------------------
# Helpers (mirrored from test_v3240_auto_adopt.py)
# ---------------------------------------------------------------------------


def _make_producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _rocksdb_partition(tmp_path, name="db", options=None, changelog_producer=None):
    path = (tmp_path / name).as_posix()
    opts = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        path, options=opts, changelog_producer=changelog_producer
    )


def _replay_default(partition, msgs, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value, ttl_stamped)`` default-CF messages."""
    partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """One v3.24.0-style default-CF changelog message."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    stamped = encode_ttl_value(expiry_ms, json_dumps(user_value))
    return (raw_key, stamped, False)


def _provision_partition(tmp_path, name, n_keys=3, expiry_ms=None, producer=None):
    """Create a cold-provisionally-adopted partition (backup CF present,
    ``_adopt_provisional=True``, pending marker on disk).

    Returns ``(partition, originals)`` where ``originals`` maps
    ``raw_key -> verbatim pre-adoption value`` (from the backup CF).
    """
    if producer is None:
        producer = _make_producer()
    if expiry_ms is None:
        expiry_ms = NOW_MS + 7 * DAY_MS
    msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry_ms) for i in range(n_keys)]
    partition = _rocksdb_partition(tmp_path, name=name, changelog_producer=producer)
    _replay_default(partition, msgs, now_ms=NOW_MS)
    partition.complete_recovery()

    # Verify preconditions.
    assert partition.uses_ttl_stamps is True, "precondition: flipped"
    assert partition._adopt_provisional is True, "precondition: provisional"
    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    assert (
        metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
    ), "precondition: pending marker on disk"

    # Snapshot the originals from the backup CF.
    backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
    originals = {
        bytes(cast(bytes, k)): bytes(cast(bytes, v)) for k, v in backup_cf.items()
    }
    assert len(originals) == n_keys, "precondition: backup populated"
    return partition, originals


# ===========================================================================
# Test 1 — Finding #1: abort during corroboration destroys rollback backup
# Bug site: partition.py:1582 corroborate_adoption() runs DURING prepare(),
#           before the checkpoint commit barrier (super().prepare()).
# ===========================================================================


class TestAbortDuringCorroborationPreservesBackup:
    """Finding #1: ``corroborate_adoption()`` runs at partition.py:1596-1603
    INSIDE the prepare() hooks (transaction.py:680), BEFORE ``super().prepare()``
    (transaction.py:685) which is the actual changelog-commit barrier.

    The sequence drops the backup CF (line 1602) and deletes the pending marker
    (line 1598-1601) before the transaction's changelog records are produced.
    If ``super().prepare()`` subsequently fails (e.g. a changelog producer error),
    the transaction is FAILED — its user-data writes are lost — but the backup
    CF is ALREADY GONE, making rollback impossible.

    EXPECTED RED: after the aborted prepare, the backup CF no longer exists
    (or is empty), so the assert that it still holds the originals fails.
    """

    def test_abort_during_corroboration_preserves_backup(self, tmp_path):
        """Validates that an aborted corroboration preserves the backup CF.

        Spec reference: §5.4 corroboration, §5.6 rollback safety.
        Expected: after a failed prepare (post-corroboration, pre-commit),
        the backup CF (__ttl_adopt_backup__) still exists with all originals.
        Actual (bug): corroborate_adoption() already dropped it.
        """
        producer = _make_producer()
        partition, originals = _provision_partition(
            tmp_path, "abort_corrob", producer=producer
        )

        # Begin a transaction with a live ttl= write (triggers corroboration).
        tx = partition.begin()
        tx.set(
            key="klive",
            value="vlive",
            prefix=b"pfx",
            timestamp=NOW_MS,
            ttl=timedelta(days=1),
        )

        # Make the producer fail on the SECOND produce() call. The first
        # produce() call comes from _produce_migration_done_marker() inside
        # corroborate_adoption(). The second comes from super().prepare() →
        # _prepare() which produces changelog records for the user writes.
        call_count = {"n": 0}

        def produce_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise RuntimeError("simulated changelog production failure")

        producer.produce.side_effect = produce_side_effect

        # prepare() should raise — the changelog production failed.
        with pytest.raises(
            RuntimeError, match="simulated changelog production failure"
        ):
            tx.prepare(processed_offsets={"topic": 1})

        # THE RED ASSERTION: the backup CF must still exist with all originals.
        # BUG: corroborate_adoption() already dropped it (partition.py:1602).
        cfs = partition.list_column_families()
        assert TTL_ADOPT_BACKUP_CF_NAME in cfs, (
            "BUG #1: corroborate_adoption() dropped __ttl_adopt_backup__ during "
            "prepare() BEFORE the changelog commit barrier; after the abort the "
            "backup CF is gone and rollback is impossible"
        )
        backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
        remaining = {
            bytes(cast(bytes, k)): bytes(cast(bytes, v)) for k, v in backup_cf.items()
        }
        assert remaining == originals, (
            "BUG #1: backup values should be intact after an aborted corroboration; "
            f"expected {len(originals)} entries, got {len(remaining)}"
        )

        partition.close()


# ===========================================================================
# Test 2 — Finding #2: rollback clobbers post-adoption writes (data loss)
# Bug site: partition.py:1636-1638 _rollback_provisional_adopt() restores
#           EVERY backup-CF item unconditionally into the default CF.
# ===========================================================================


class TestRollbackPreservesPostAdoptionWrites:
    """Finding #2: ``_rollback_provisional_adopt()`` at partition.py:1611
    restores every backup-CF item unconditionally (``batch.put(raw_key, value,
    default_handle)``, lines 1636-1638) — overwriting any post-adoption commits
    with stale pre-adoption values and leaving new post-adoption keys with their
    8-byte stamp prefix misread as legacy data.

    EXPECTED RED: (a) key ``k`` reads back stale pre-adoption value ``v0``
    instead of the post-adoption committed value ``v_updated``; (b) new key
    ``n`` reads back correctly only if the stamp prefix is not misinterpreted.
    """

    def test_rollback_preserves_post_adoption_writes(self, tmp_path):
        """Validates that rollback does not clobber post-adoption durable writes.

        Spec reference: §5.6 rollback safety.
        Expected: reading k0 after rollback returns 'v_updated' (the latest
        committed value), not the stale pre-adoption 'v0'.
        Actual (bug): rollback unconditionally restores the pre-adoption backup
        value 'v0' over the updated 'v_updated', causing silent data loss.
        """
        import os

        producer = _make_producer()
        partition, originals = _provision_partition(
            tmp_path, "rollback_clobber", n_keys=2, producer=producer
        )

        # Post-adoption write #1: overwrite k0 (originally v0) with v_updated.
        # This is a plain set() (sentinel stamp, no ttl=) on the flipped store.
        tx1 = partition.begin()
        tx1.set(key="k0", value="v_updated", prefix=b"pfx", timestamp=NOW_MS)
        tx1.prepare(processed_offsets={"topic": 1})
        tx1.flush(changelog_offset=100)

        # Post-adoption write #2: create a brand-new key "n0" that only exists
        # in the default CF (stamped, never in the backup).
        tx2 = partition.begin()
        tx2.set(key="n0", value="new_value", prefix=b"pfx", timestamp=NOW_MS)
        tx2.prepare(processed_offsets={"topic": 2})
        tx2.flush(changelog_offset=101)

        # Verify the writes are durable.
        tx_check = partition.begin()
        assert tx_check.get(key="k0", prefix=b"pfx", timestamp=NOW_MS) == "v_updated"
        assert tx_check.get(key="n0", prefix=b"pfx", timestamp=NOW_MS) == "new_value"
        partition.close()

        # Rollback: reopen with the env var.
        os.environ["QUIXSTREAMS_STATE_TTL_ROLLBACK"] = "1"
        try:
            producer2 = _make_producer()
            partition2 = _rocksdb_partition(
                tmp_path, name="rollback_clobber", changelog_producer=producer2
            )

            assert (
                partition2.uses_ttl_stamps is False
            ), "precondition: store reverted to legacy after rollback"

            # THE RED ASSERTIONS:
            # (a) k0 should be the LATEST committed value, not the stale backup.
            raw_k0 = b"pfx|" + json_dumps("k0")
            from quixstreams.state.metadata import Marker

            val_k0 = partition2.get(raw_k0, cf_name="default")
            assert val_k0 is not Marker.UNDEFINED, "k0 should exist after rollback"
            # The rollback should NOT have clobbered the post-adoption write.
            # BUG: rollback restores the stale pre-adoption value over v_updated.
            # After rollback to legacy, the raw value on disk IS the user value
            # (no stamp prefix). The original pre-adoption raw value was
            # encode_ttl_value(expiry, json_dumps("v0")) — an 8-byte-stamped blob.
            # The post-adoption write of "v_updated" was also stamped (partition
            # was flipped). If rollback unconditionally restores the backup, it
            # overwrites the v_updated stamped blob with the v0 stamped blob.
            # Either way the store is now legacy so reads won't strip stamps.
            # We check via a transaction read that deserializes correctly:
            # after rollback, the store is legacy (no stamp stripping), so the
            # raw bytes on disk for k0 should be something that deserializes
            # to "v_updated", not "v0".
            #
            # Since the store is legacy after rollback, values are read raw
            # (no stamp stripping). A pre-adoption backup value is
            # encode_ttl_value(expiry, json_dumps("v0")) which is 8-byte stamp +
            # json bytes. If the rollback put this back, deserializing it as
            # legacy would fail or return garbage.
            #
            # The simplest assertion: the raw bytes for k0 should NOT equal the
            # original pre-adoption backup value.
            assert bytes(val_k0) != originals[raw_k0], (
                "BUG #2: rollback clobbered the post-adoption write of k0 with "
                "the stale pre-adoption backup value — silent data loss"
            )

            # (b) n0 should still be readable (not absent, not garbage).
            raw_n0 = b"pfx|" + json_dumps("n0")
            val_n0 = partition2.get(raw_n0, cf_name="default")
            # n0 was never in the backup, so rollback should not have deleted it.
            # The value on disk is a stamped blob; reading it as legacy gives the
            # raw stamped bytes (8B prefix + payload). This is the "stamp-prefixed
            # garbage" aspect of the bug — but at minimum the key must still exist.
            assert val_n0 is not Marker.UNDEFINED, (
                "BUG #2: n0 (post-adoption new key) must survive rollback; "
                "it was never in the backup and should not be deleted"
            )

            partition2.close()
        finally:
            os.environ.pop("QUIXSTREAMS_STATE_TTL_ROLLBACK", None)


# ===========================================================================
# Test 3 — Finding #4: crash mid-corroboration pins the sweep off
# Bug site: partition.py:1582 corroborate_adoption() — a crash after the
#           done-marker write but before the pending-marker delete leaves the
#           pending marker on disk, so the next open re-arms _adopt_provisional
#           and the sweep stays suppressed despite the done-marker being present.
# ===========================================================================


class TestCrashMidCorroborationDoesNotPinSweepOff:
    """Finding #4: if corroborate_adoption() at partition.py:1582 crashes AFTER
    producing and persisting the done-marker (line 1596 + 1102-1108) but BEFORE
    deleting the pending marker (lines 1597-1601) and dropping the backup CF
    (line 1602), the next open sees:

    - ``__ttl_adopt_pending__`` still on disk -> ``_adopt_provisional = True``
    - the done-marker in ``__ttl_system__`` CF

    Because ``_adopt_provisional`` is loaded from the pending marker (partition.py:283),
    the sweep remains suppressed (partition.py:2835). The done-marker is not
    consulted at runtime to lift the sweep. Expired adopted records survive
    forever.

    EXPECTED RED: after the simulated crash and reopen, the sweep does NOT
    reclaim the expired adopted record (it is still present) because the sweep
    guard is pinned on.
    """

    def test_crash_mid_corroboration_does_not_pin_sweep_off(self, tmp_path):
        """Validates that a crash during corroboration (after done-marker, before
        pending-marker delete) does not permanently suppress the sweep.

        Spec reference: section 5.4 corroboration, section 5.3 sweep suppression.
        Bug site: partition.py:1582 corroborate_adoption() and partition.py:283
        _load_adopt_pending_flag().
        Expected: after reopen, the expired adopted record is reclaimed by the
        sweep (the done-marker proves corroboration succeeded, so the sweep
        should be enabled).
        Actual (bug): the pending marker survives -> _adopt_provisional=True ->
        sweep suppressed -> expired records survive forever.
        """
        producer = _make_producer()
        # Use a MIX of past and future stamps: the all-past quarantine guard
        # requires at least one future stamp for adoption to proceed. After
        # adoption, the past-stamped record (k_past) should be swept once
        # corroboration lifts the sweep guard.
        expiry_past = NOW_MS - DAY_MS
        expiry_future = NOW_MS + 7 * DAY_MS
        msgs = [
            _v3240_msg("k_past", "v_past", expiry_past),
            _v3240_msg("k_future", "v_future", expiry_future),
        ]
        partition = _rocksdb_partition(
            tmp_path, name="crash_corrob", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        # Verify preconditions: provisionally adopted.
        assert partition.uses_ttl_stamps is True, "precondition: flipped"
        assert partition._adopt_provisional is True, "precondition: provisional"

        # Simulate a crash during corroborate_adoption():
        # corroborate_adoption() sequence:
        #   1. self._produce_migration_done_marker()
        #        1a. self._stamp_flip_metadata()       # local _write: flip flag
        #        1b. produce + flush the marker        # changelog-first
        #        1c. self._write(batch)                # local _write: done-marker
        #   2. batch.delete(TTL_ADOPT_PENDING_KEY, ...)
        #      self._write(batch)                      # local _write: pending del
        #   3. self._adopt_provisional = False
        # (the irreversible __ttl_adopt_backup__ drop is DEFERRED to
        #  finalize_corroboration_teardown, which the transaction calls only past
        #  the commit barrier, so it is not part of this sequence.)
        #
        # Crash between step 1 and step 2: the done-marker is on disk but the
        # pending marker is NOT deleted.
        real_write = partition._write

        def crash_on_pending_delete(batch):
            # Target the write by CONTENT, not by call index. Step 2 is the first
            # _write that runs once the done-marker is already durable, so this
            # condition selects it however many writes step 1 makes.
            #
            # A hard-coded index does not survive: sc-74843 round 3 made step 1
            # persist the flip flag in its OWN durable write (1a) before
            # producing or writing the marker -- the invariant "done-marker
            # present => __ttl_enabled__ present", without which the store
            # reopens in legacy mode over stamped values and crash-loops. That
            # turned step 1 into TWO writes, so the previous "crash on call #2"
            # hook landed on the done-marker's own write (1c) and left no marker
            # on disk, failing the precondition below instead of exercising the
            # crash this test is about.
            if partition._has_local_migration_done_marker():
                raise RuntimeError("simulated crash mid-corroboration")
            return real_write(batch)

        # Drive a transaction with a ttl= write to trigger corroboration.
        tx = partition.begin()
        tx.set(
            key="klive",
            value="vlive",
            prefix=b"pfx",
            timestamp=NOW_MS,
            ttl=timedelta(days=1),
        )

        # Install the crash hook just before prepare.
        partition._write = crash_on_pending_delete

        with pytest.raises(RuntimeError, match="simulated crash mid-corroboration"):
            tx.prepare(processed_offsets={"topic": 1})

        # Restore real _write and close (simulating a process crash + restart).
        partition._write = real_write
        partition.close()

        # Reopen the partition from the same path (warm restart).
        producer2 = _make_producer()
        partition2 = _rocksdb_partition(
            tmp_path, name="crash_corrob", changelog_producer=producer2
        )

        # Verify the done-marker IS present (the first _write succeeded).
        system_cf = partition2.get_or_create_column_family(TTL_SYSTEM_CF_NAME)
        assert (
            system_cf.get(TTL_MIGRATION_DONE_KEY, default=None) is not None
        ), "precondition: done-marker should be on disk (first _write succeeded)"

        # Verify the pending marker is still on disk (the second _write crashed).
        metadata_cf = partition2.get_or_create_column_family(METADATA_CF_NAME)
        pending_still_there = (
            metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
        )

        # The store should be flipped and the sweep should be ENABLED (the
        # done-marker proves corroboration succeeded).
        assert partition2.uses_ttl_stamps is True, "store should be flipped"

        # Now drive a normal transaction to trigger the sweep.
        # Advance high-water past the expired record.
        partition2._high_water_ms = NOW_MS + 1

        tx2 = partition2.begin()
        tx2.set(key="probe", value="probe_val", prefix=b"pfx", timestamp=NOW_MS)
        tx2.prepare(processed_offsets={"topic": 3})
        tx2.flush(changelog_offset=200)

        # THE RED ASSERTION: the expired adopted record should be reclaimed.
        raw_k_past = b"pfx|" + json_dumps("k_past")
        from quixstreams.state.metadata import Marker

        val = partition2.get(raw_k_past, cf_name="default")
        assert val is Marker.UNDEFINED, (
            "BUG #4: the expired adopted record k_past survives after reopen "
            "because the pending marker pins _adopt_provisional=True and the "
            f"sweep is suppressed; got val={val!r} instead of UNDEFINED. "
            f"pending_marker_survives={pending_still_there}, "
            f"_adopt_provisional={partition2._adopt_provisional}"
        )

        partition2.close()
