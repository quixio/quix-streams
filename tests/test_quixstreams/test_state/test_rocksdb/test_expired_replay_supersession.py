"""
An EXPIRED stamped record replayed during recovery must SUPERSEDE any older copy
of the same key.

A compacted changelog can carry several pre-compaction copies of one key. Before
the fix, when a header-true stamped record was judged expired at replay the branch
did a bare ``pass`` (skip both the main and the index write). That let an OLDER
copy of the key replayed earlier — a verbatim header-absent legacy value, or an
older *unexpired* stamped copy — survive in the main CF. In the MIXED shape the
key's pending census entry was already deleted by the stamped record's
supersession, so ``complete_recovery`` could never repair it, and the expired
record RESURRECTED as a never-expiring, unswept legacy value.

The fix: the expired branch now stages ``batch.delete(key, cf_handle)``
(latest-record-wins). The older copy's ``__ttl_index__`` pointer (if any) is left
for the sweep's ghost/orphan GC (its stamp is unknown at replay time).

See ``test_memory/test_expired_replay_supersession.py`` for the memory mirror.
"""

from datetime import timedelta

from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

DAY_MS = 86_400_000


def _replay(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` default-CF messages.

    ``now_ms`` models a WARM restore (finding #3): it is seeded as the partition's
    persisted event-time high-water (the recovery-drop frontier) AND pins the
    recovery wallclock, so a stamped record with ``stamp <= now_ms`` is dropped
    exactly as the live read filter would drop it. This exercises the
    delete-supersession path (a past-frontier expired copy supersedes an older
    copy by deletion) that the fix preserves for warm restores."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
        # Seed the event-time frontier = now_ms so the recovery drop mirrors the
        # live read filter (drop iff stamp <= high_water); a past-dated stamp is
        # then dropped just as it was under the old wallclock, now event-time.
        partition.advance_high_water(now_ms)
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


def _default_keys(partition):
    cf = partition.get_or_create_column_family("default")
    return set(cf.keys())


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


class TestExpiredReplaySupersedesOlderCopy:
    def test_legacy_then_expired_stamped_drops_key(
        self, store_partition_factory, changelog_producer_mock
    ):
        """Case (a): a header-absent legacy copy of K lands first, then a
        header-true EXPIRED stamped copy of K. After recovery + completion K must
        be absent from BOTH the main CF and the pending census — not resurrected
        as a never-expiring legacy value."""
        now_ms = 1_780_000_000_000
        past_stamp = now_ms - DAY_MS  # already expired at recovery wallclock
        key = b"pfx|k0"
        msgs = [
            (key, b"legacy-raw-value", False),
            (key, encode_ttl_value(past_stamp, b"stamped-raw"), True),
        ]

        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        # Flipped on the stamped record, and the earlier legacy census entry was
        # superseded (deleted) by the stamped record.
        assert recovered.uses_ttl_stamps is True
        assert recovered._recovery_saw_stamped is True
        assert key not in _pending_keys(recovered)
        # The expired stamped copy deletes the older legacy value.
        assert key not in _default_keys(recovered)

        recovered.complete_recovery()
        # Still gone after completion — no repair path resurrects it.
        assert key not in _default_keys(recovered)
        assert key not in _pending_keys(recovered)
        recovered.close()

    def test_unexpired_stamped_then_expired_stamped_drops_key(
        self, store_partition_factory, changelog_producer_mock
    ):
        """Case (b): an older UNEXPIRED stamped copy of K lands first (and is
        kept + indexed), then a newer EXPIRED stamped copy of K. Latest-record-
        wins: K must be absent after replay (the stale index pointer is left for
        the sweep's ghost GC)."""
        now_ms = 1_780_000_000_000
        future_stamp = now_ms + 30 * DAY_MS  # unexpired → kept on first replay
        past_stamp = now_ms - DAY_MS  # expired → supersedes on second replay
        key = b"pfx|k0"
        msgs = [
            (key, encode_ttl_value(future_stamp, b"old-payload"), True),
            (key, encode_ttl_value(past_stamp, b"new-payload"), True),
        ]

        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        assert recovered.uses_ttl_stamps is True
        # The newer expired copy supersedes the older unexpired one.
        assert key not in _default_keys(recovered)
        assert key not in _pending_keys(recovered)

        recovered.complete_recovery()
        assert key not in _default_keys(recovered)
        recovered.close()


class TestExpiredReplayDropAggregateInfo:
    """The event-time-frontier-expired replay drops must be visible as ONE
    aggregate INFO per partition per recovery (count + path + clock), not
    silent."""

    def test_aggregate_info_on_expired_drops(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """>=2 stamped records already expired against the recovery event-time
        frontier are dropped during replay; complete_recovery emits exactly one
        aggregate INFO naming the count and the frontier (high_water).

        RED (HEAD): no INFO mentions dropped/expired records.
        GREEN: exactly one INFO with count=2 + path + clock.
        """
        import logging

        now_ms = 1_780_000_000_000
        past = now_ms - DAY_MS
        msgs = [
            (b"pfx|k0", encode_ttl_value(past, b"v0"), True),
            (b"pfx|k1", encode_ttl_value(past, b"v1"), True),
        ]
        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)
        # Both dropped by the latest-record-wins recovery-drop filter.
        assert _default_keys(recovered) == set()

        with caplog.at_level(logging.INFO):
            recovered.complete_recovery()

        drop_logs = [
            r
            for r in caplog.records
            if "already-expired stamped record" in r.message
            and r.levelno == logging.INFO
        ]
        assert len(drop_logs) == 1, (
            f"expected exactly one aggregate drop INFO, got "
            f"{[r.message for r in drop_logs]}"
        )
        msg = drop_logs[0].getMessage()
        assert "dropped 2 already-expired stamped record(s)" in msg
        # The drop clock is now the event-time frontier (seeded high_water=now_ms).
        assert f"high_water={now_ms}" in msg
        assert "path=" in msg
        recovered.close()

    def test_no_info_when_zero_drops(
        self, store_partition_factory, changelog_producer_mock, caplog
    ):
        """A recovery that drops nothing logs no drop INFO (gated on count > 0)."""
        import logging

        now_ms = 1_780_000_000_000
        future = now_ms + 30 * DAY_MS
        msgs = [
            (b"pfx|k0", encode_ttl_value(future, b"v0"), True),
            (b"pfx|k1", encode_ttl_value(future, b"v1"), True),
        ]
        recovered = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)
        with caplog.at_level(logging.INFO):
            recovered.complete_recovery()
        assert not any(
            "already-expired stamped record" in r.message for r in caplog.records
        )
        recovered.close()
