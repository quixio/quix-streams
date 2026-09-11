"""
Memory-backend tests: an EXPIRED stamped record replayed during recovery must
SUPERSEDE any older copy of the same key.

If the expired branch simply left an older copy (verbatim legacy, or an older
unexpired stamped copy) alive in the main dict, it would resurrect the expired
record as a never-expiring leftover that the completion pass could no longer
repair. The correct behavior pops the key (latest-record-wins).
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

DAY_MS = 86_400_000


def _replay(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` default-CF messages.

    ``now_ms`` models a WARM restore (finding #3): it is seeded as the partition's
    event-time high-water (the recovery-drop frontier) AND pins the recovery
    wallclock, so a stamped record with ``stamp <= now_ms`` is dropped exactly as
    the live read filter would — exercising the delete-supersession path the fix
    preserves for warm restores."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
        # Seed the event-time frontier = now_ms so the recovery drop mirrors the
        # live read filter (drop iff stamp <= high_water).
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
    return set(partition._state.get("default", {}).keys())


def _pending_keys(partition):
    return set(partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {}).keys())


class TestExpiredReplaySupersedesOlderCopy:
    def test_legacy_then_expired_stamped_drops_key(self, changelog_producer_mock):
        now_ms = 1_780_000_000_000
        past_stamp = now_ms - DAY_MS
        key = b"pfx|k0"
        msgs = [
            (key, b"legacy-raw-value", False),
            (key, encode_ttl_value(past_stamp, b"stamped-raw"), True),
        ]
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            legacy_records_ttl=timedelta(days=7),
        )
        _replay(partition, msgs, now_ms=now_ms)

        assert partition.uses_ttl_stamps is True
        assert partition._recovery_saw_stamped is True
        assert key not in _pending_keys(partition)
        assert key not in _default_keys(partition)

        partition.complete_recovery()
        assert key not in _default_keys(partition)
        assert key not in _pending_keys(partition)

    def test_unexpired_stamped_then_expired_stamped_drops_key(
        self, changelog_producer_mock
    ):
        now_ms = 1_780_000_000_000
        future_stamp = now_ms + 30 * DAY_MS
        past_stamp = now_ms - DAY_MS
        key = b"pfx|k0"
        msgs = [
            (key, encode_ttl_value(future_stamp, b"old-payload"), True),
            (key, encode_ttl_value(past_stamp, b"new-payload"), True),
        ]
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            legacy_records_ttl=timedelta(days=7),
        )
        _replay(partition, msgs, now_ms=now_ms)

        assert partition.uses_ttl_stamps is True
        assert key not in _default_keys(partition)
        assert key not in _pending_keys(partition)

        partition.complete_recovery()
        assert key not in _default_keys(partition)


class TestExpiredReplayDropAggregateInfo:
    """Event-time-frontier-expired replay drops emit ONE aggregate INFO per
    partition per recovery (count + clock, no path)."""

    def test_aggregate_info_on_expired_drops(self, changelog_producer_mock, caplog):
        """Exactly one INFO with count=2 plus the recovery event-time frontier
        (high_water) is emitted when expired stamped records are dropped."""
        import logging

        now_ms = 1_780_000_000_000
        past = now_ms - DAY_MS
        msgs = [
            (b"pfx|k0", encode_ttl_value(past, b"v0"), True),
            (b"pfx|k1", encode_ttl_value(past, b"v1"), True),
        ]
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            legacy_records_ttl=timedelta(days=7),
        )
        _replay(partition, msgs, now_ms=now_ms)
        assert _default_keys(partition) == set()

        with caplog.at_level(logging.INFO):
            partition.complete_recovery()

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

    def test_no_info_when_zero_drops(self, changelog_producer_mock, caplog):
        import logging

        now_ms = 1_780_000_000_000
        future = now_ms + 30 * DAY_MS
        msgs = [
            (b"pfx|k0", encode_ttl_value(future, b"v0"), True),
            (b"pfx|k1", encode_ttl_value(future, b"v1"), True),
        ]
        partition = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            legacy_records_ttl=timedelta(days=7),
        )
        _replay(partition, msgs, now_ms=now_ms)
        with caplog.at_level(logging.INFO):
            partition.complete_recovery()
        assert not any(
            "already-expired stamped record" in r.message for r in caplog.records
        )
