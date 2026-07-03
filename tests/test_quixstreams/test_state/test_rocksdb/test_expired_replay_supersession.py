"""
Fix A (shortcut 73191 review) — an EXPIRED stamped record replayed during
recovery must SUPERSEDE any older copy of the same key.

A compacted changelog can carry several pre-compaction copies of one key. Before
Fix A, when a header-true stamped record was judged expired at replay the branch
did a bare ``pass`` (skip both the main and the index write). That let an OLDER
copy of the key replayed earlier — a verbatim header-absent legacy value, or an
older *unexpired* stamped copy — survive in the main CF. In the MIXED shape the
key's pending census entry was already deleted by the stamped record's
supersession, so ``complete_recovery`` could never repair it, and the expired
record RESURRECTED as a never-expiring, unswept legacy value.

Fix A: the expired branch now stages ``batch.delete(key, cf_handle)``
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
    """Replay ``(key, value, ttl_stamped)`` default-CF messages, optionally
    fixing the recovery wallclock so the expiry decision is deterministic."""
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
        # Fix A: the expired stamped copy deletes the older legacy value.
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
        # Fix A: the newer expired copy supersedes the older unexpired one.
        assert key not in _default_keys(recovered)
        assert key not in _pending_keys(recovered)

        recovered.complete_recovery()
        assert key not in _default_keys(recovered)
        recovered.close()
