"""
An interrupted legacy-TTL migration must be completed even after an
OFFSET-CAUGHT-UP restart.

Crash window: a MIXED cold restore replayed its LAST changelog message (so the
persisted changelog offset == highwater-1) and flipped the partition, but crashed
before ``complete_recovery`` drained the ``__ttl_backfill_pending__`` census. On
restart the normal ``needs_recovery_check`` test (``highwater-1 > offset``) is
False, so recovery — and therefore completion — never runs, stranding the
leftover legacy records as never-expiring forever (the live ``ttl=`` gate
short-circuits on an already-flipped store).

The fix has two parts:
  1. ``StorePartition.has_incomplete_ttl_migration()`` (True iff persisted-flipped
     + pending non-empty + no done-marker); ``RecoveryPartition.needs_recovery_check``
     forces the check when it is True.
  2. ``complete_recovery`` treats a partition ALREADY persisted-flipped at open as
     saw-stamped-equivalent, so its census is COMPLETED (not discarded even though
     ``_recovery_saw_stamped`` stays False with no replay this session). Config-
     absent completion with no replayed survivor derives the expiry from the max
     on-disk ``__ttl_index__`` stamp.
"""

from datetime import timedelta

from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000


def _replay(partition, msgs, now_ms=None):
    if now_ms is not None:
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
    return offset  # == number of messages == highwater (last offset + 1)


def _mixed_changelog(n_stamped, n_legacy, stamp_expiry):
    msgs = []
    legacy_values = {}
    for i in range(n_stamped):
        key = f"pfx|s{i}".encode()
        msgs.append(
            (key, encode_ttl_value(stamp_expiry, f"stamped-{i}".encode()), True)
        )
    for i in range(n_legacy):
        key = f"pfx|l{i}".encode()
        raw = f"legacy-payload-{i}".encode()
        legacy_values[key] = raw
        msgs.append((key, raw, False))
    return msgs, legacy_values


def _default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: value for key, value in cf.items()}


def _index_cf(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


class TestOffsetCaughtUpCompletion:
    def test_incomplete_migration_completes_on_restart_config_present(
        self,
        store_partition_factory,
        changelog_producer_mock,
        recovery_partition_factory,
    ):
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        stamp_expiry = now_ms + 30 * DAY_MS
        n_stamped, n_legacy = 3, 5
        msgs, legacy_values = _mixed_changelog(n_stamped, n_legacy, stamp_expiry)

        # --- Run 1: replay a MIXED changelog to the end, then "crash" BEFORE
        # complete_recovery drains the census. ---
        opts = RocksDBOptions(legacy_records_ttl=legacy_ttl)
        run1 = store_partition_factory(
            name="dst", options=opts, changelog_producer=changelog_producer_mock
        )
        highwater = _replay(run1, msgs, now_ms=now_ms)
        assert run1.uses_ttl_stamps is True
        assert _pending_keys(run1) == set(legacy_values.keys())
        # Persisted offset is highwater-1 (last replayed message), the crash window.
        assert run1.get_changelog_offset() == highwater - 1
        run1.close()  # crash: completion never ran

        # --- Run 2: reopen the SAME on-disk store (offset already caught up). ---
        run2 = store_partition_factory(
            name="dst", options=opts, changelog_producer=changelog_producer_mock
        )
        run2._now_ms = lambda: now_ms  # noqa: E731

        # Part 1: the store reports an incomplete migration, and the recovery
        # gate forces a check even though the offset is caught up.
        assert run2._persisted_flipped_at_open is True
        assert run2.has_incomplete_ttl_migration() is True
        rp = recovery_partition_factory(
            store_partition=run2, lowwater=0, highwater=highwater
        )
        # The bare "state behind" signal is False (offset == highwater-1)...
        assert rp._changelog_highwater - 1 == rp.offset
        # ...yet the migration check forces recovery to run.
        assert rp.needs_recovery_check is True

        # Part 2: completion runs with NO replay this session and drains the census.
        changelog_producer_mock.produce.reset_mock()
        run2.complete_recovery()

        expected_expiry = now_ms + 7 * DAY_MS
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(run2).items()}
        index = _index_cf(run2)
        for key, raw in legacy_values.items():
            assert decoded[key] == (expected_expiry, raw)
            assert index[key] == expected_expiry
        # Stamped survivors are byte-unchanged.
        for i in range(n_stamped):
            key = f"pfx|s{i}".encode()
            assert decoded[key] == (stamp_expiry, f"stamped-{i}".encode())
        assert _pending_keys(run2) == set()
        assert all(exp != SENTINEL_NEVER for exp, _ in decoded.values())
        run2.close()

    def test_incomplete_migration_config_absent_uses_max_index_stamp(
        self, store_partition_factory, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(2, 4, stamp_expiry)

        # Run 1 without legacy_records_ttl configured.
        run1 = store_partition_factory(
            name="dst", changelog_producer=changelog_producer_mock
        )
        _replay(run1, msgs, now_ms=now_ms)
        assert run1.uses_ttl_stamps is True
        assert _pending_keys(run1) == set(legacy_values.keys())
        run1.close()

        # Run 2: reopen. No replay this session → _recovery_max_survivor_expiry_ms
        # is unset → the config-absent completion derives the expiry from the max
        # ON-DISK __ttl_index__ stamp (the persisted survivor cohort).
        run2 = store_partition_factory(
            name="dst", changelog_producer=changelog_producer_mock
        )
        run2._now_ms = lambda: now_ms  # noqa: E731
        assert run2.has_incomplete_ttl_migration() is True
        assert run2._max_index_stamp_ms() == stamp_expiry

        run2.complete_recovery()
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(run2).items()}
        index = _index_cf(run2)
        for key, raw in legacy_values.items():
            # Leftovers inherit the surviving cohort's window (max index stamp).
            assert decoded[key] == (stamp_expiry, raw)
            assert index[key] == stamp_expiry
        assert _pending_keys(run2) == set()
        run2.close()

    def test_truncated_changelog_completes_migration_strand_free(
        self,
        store_partition_factory,
        changelog_producer_mock,
        recovery_partition_factory,
    ):
        """Validates PR #1130 Finding 2 deeper guarantee: a fully-truncated
        changelog (``lowwater == highwater``, zero consumable offsets) must NOT
        strand an interrupted legacy-TTL migration. The recovery gate must fire
        solely from ``has_incomplete_ttl_migration()``, and ``complete_recovery``
        must finalize the leftover records, write the done-marker, and clear the
        pending census — even though no replay occurs this session."""
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        stamp_expiry = now_ms + 30 * DAY_MS
        n_stamped, n_legacy = 3, 5
        msgs, legacy_values = _mixed_changelog(n_stamped, n_legacy, stamp_expiry)

        # --- Run 1: replay a MIXED changelog to the end, then "crash" BEFORE
        # complete_recovery drains the census. ---
        opts = RocksDBOptions(legacy_records_ttl=legacy_ttl)
        run1 = store_partition_factory(
            name="trunc", options=opts, changelog_producer=changelog_producer_mock
        )
        highwater = _replay(run1, msgs, now_ms=now_ms)
        assert run1.uses_ttl_stamps is True
        assert _pending_keys(run1) == set(legacy_values.keys())
        assert run1.get_changelog_offset() == highwater - 1
        run1.close()  # crash: completion never ran

        # --- Run 2: reopen with a FULLY-TRUNCATED changelog
        # (lowwater == highwater — retention purged everything). ---
        run2 = store_partition_factory(
            name="trunc", options=opts, changelog_producer=changelog_producer_mock
        )
        run2._now_ms = lambda: now_ms  # noqa: E731

        assert run2._persisted_flipped_at_open is True
        assert run2.has_incomplete_ttl_migration() is True

        # Build a RecoveryPartition with the truncated-changelog watermarks.
        rp = recovery_partition_factory(
            store_partition=run2, lowwater=highwater, highwater=highwater
        )
        # Both normal recovery signals are False:
        assert rp._changelog_lowwater == rp._changelog_highwater  # no consumable
        assert not (rp._changelog_highwater - 1 > rp.offset)  # not behind
        # Yet recovery is forced by the incomplete-migration check alone.
        assert rp.needs_recovery_check is True

        # --- Drive completion: no replay this session, just finalize. ---
        changelog_producer_mock.produce.reset_mock()
        run2.complete_recovery()

        # Legacy leftovers are stamped at wallclock + TTL.
        expected_expiry = now_ms + 7 * DAY_MS
        decoded = {k: decode_ttl_value(v) for k, v in _default_cf(run2).items()}
        index = _index_cf(run2)
        for key, raw in legacy_values.items():
            assert (
                decoded[key] == (expected_expiry, raw)
            ), f"legacy key {key!r} not stamped correctly after truncated-changelog completion"
            assert index[key] == expected_expiry

        # Stamped survivors are byte-unchanged.
        for i in range(n_stamped):
            key = f"pfx|s{i}".encode()
            assert decoded[key] == (stamp_expiry, f"stamped-{i}".encode())

        # Pending census is fully drained.
        assert (
            _pending_keys(run2) == set()
        ), "pending census not cleared — records stranded after truncated-changelog completion"

        # Done-marker is written — future restarts will NOT re-flag.
        assert (
            run2._has_local_migration_done_marker() is True
        ), "done-marker not written — migration would re-trigger on every restart"
        assert run2.has_incomplete_ttl_migration() is False

        # No SENTINEL_NEVER (never-expiring) records escaped.
        assert all(exp != SENTINEL_NEVER for exp, _ in decoded.values())
        run2.close()

    def test_done_marker_makes_migration_not_incomplete(
        self, store_partition_factory, changelog_producer_mock
    ):
        # A completed migration (done-marker written) must NOT be flagged
        # incomplete, so a normal warm restart does not force a redundant check.
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, _ = _mixed_changelog(2, 3, stamp_expiry)
        opts = RocksDBOptions(legacy_records_ttl=timedelta(days=7))
        run1 = store_partition_factory(
            name="dst", options=opts, changelog_producer=changelog_producer_mock
        )
        _replay(run1, msgs, now_ms=now_ms)
        run1.complete_recovery()  # completes + writes the done-marker
        assert _pending_keys(run1) == set()
        run1.close()

        run2 = store_partition_factory(
            name="dst", options=opts, changelog_producer=changelog_producer_mock
        )
        assert run2._has_local_migration_done_marker() is True
        assert run2.has_incomplete_ttl_migration() is False
        run2.close()


class TestUnflippedOrphanCensusRegression:
    def test_unflipped_store_still_discards_orphan_census(
        self, store_partition_factory, changelog_producer_mock
    ):
        # Regression: a pure-legacy (never-flipped) store that censused
        # header-absent keys during replay must DISCARD that orphan census at
        # completion — the completion fix must not turn this into a spurious
        # completion.
        now_ms = 1_780_000_000_000
        msgs = [(f"pfx|l{i}".encode(), f"legacy-{i}".encode(), False) for i in range(5)]
        part = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(part, msgs, now_ms=now_ms)

        assert part.uses_ttl_stamps is False
        assert part._persisted_flipped_at_open is False
        assert part._recovery_saw_stamped is False
        # An un-flipped store never reports an incomplete migration.
        assert part.has_incomplete_ttl_migration() is False
        assert _pending_keys(part) == set(f"pfx|l{i}".encode() for i in range(5))

        part.complete_recovery()
        # Orphan census discarded; records stay raw legacy (owned by the live
        # first-ttl=-write backfill), nothing stamped.
        assert _pending_keys(part) == set()
        on_disk = _default_cf(part)
        for i in range(5):
            assert on_disk[f"pfx|l{i}".encode()] == f"legacy-{i}".encode()
        part.close()
