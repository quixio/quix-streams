"""
Progress-based backfill changelog flush loop. ``_flush_backfill_changelog``
replaces the fixed 25 s per-chunk deadline with a slice-loop that fails only when
a full slice makes ZERO delivery progress (plus a runaway cap), so a
large-but-progressing chunk no longer trips a spurious ``ChangelogFlushError``.
"""

from unittest.mock import MagicMock

import pytest

import quixstreams.state.rocksdb.partition as partition_module
from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.recovery import ChangelogProducer


def _producer(flush_returns):
    mock = MagicMock(spec_set=ChangelogProducer)
    if isinstance(flush_returns, list):
        mock.flush.side_effect = flush_returns
    else:
        mock.flush.return_value = flush_returns
    return mock


class TestBackfillFlushProgress:
    def test_slow_but_progressing_succeeds(self, store_partition):
        # 10 → 6 → 2 → 0: each slice strictly decreases, so the loop keeps going
        # and returns when it hits 0 — no raise despite >1 slice.
        producer = _producer([10, 6, 2, 0])
        store_partition._flush_backfill_changelog(producer)
        assert producer.flush.call_count == 4

    def test_immediate_delivery_single_slice(self, store_partition):
        producer = _producer([0])
        store_partition._flush_backfill_changelog(producer)
        assert producer.flush.call_count == 1

    def test_stuck_no_progress_raises(self, store_partition):
        # 10 → 10: the second full slice made no progress → raise.
        producer = _producer([10, 10])
        with pytest.raises(ChangelogFlushError, match="no delivery progress"):
            store_partition._flush_backfill_changelog(producer)
        assert producer.flush.call_count == 2

    def test_indeterminate_return_does_not_block(self, store_partition):
        # A non-int return (unconfigured test double) → return without raising.
        producer = _producer(object())
        store_partition._flush_backfill_changelog(producer)
        assert producer.flush.call_count == 1

    def test_none_producer_is_noop(self, store_partition):
        # No changelog configured → no-op.
        store_partition._flush_backfill_changelog(None)

    def test_runaway_cap_raises(self, store_partition, monkeypatch):
        # An ever-shrinking trickle that never reaches 0 terminates at the cap.
        monkeypatch.setattr(partition_module, "_BACKFILL_CHANGELOG_FLUSH_MAX_SLICES", 3)
        producer = _producer([10, 9, 8, 7, 6])
        with pytest.raises(ChangelogFlushError, match="after 3 ×"):
            store_partition._flush_backfill_changelog(producer)
        assert producer.flush.call_count == 3
