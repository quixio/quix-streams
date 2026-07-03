"""
Fix A (shortcut 73191 review), memory-backend mirror of
``test_rocksdb/test_expired_replay_supersession.py``.

An EXPIRED stamped record replayed during recovery must SUPERSEDE any older copy
of the same key. Before Fix A the expired branch did a bare ``pass``, leaving an
older copy (verbatim legacy, or an older unexpired stamped copy) alive in the
main dict — resurrecting the expired record as a never-expiring leftover the
completion pass could no longer repair. Fix A pops the key
(latest-record-wins).
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

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
