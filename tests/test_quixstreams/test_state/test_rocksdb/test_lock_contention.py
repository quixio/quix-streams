"""
Tests for the RocksDB lock-contention / rebalance-handover fixes.

See docs/rocksdb-lock-contention-analysis.md:
    * fast shutdown  — cancel background flush/compaction before close()
    * stage 3        — stop-flag-aware open-retry loop
"""

import time
from threading import Event
from unittest.mock import MagicMock, call

import pytest

from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition


class TestRocksDBFastShutdown:
    def test_close_cancels_background_work_before_closing(
        self, store_partition_factory
    ):
        """
        close() must stop RocksDB background flush/compaction *before* calling
        db.close(), so that close() does not block waiting for them to wind
        down. This is what keeps the revoke sequence short enough to avoid
        poll-interval eviction during a rebalance handover.
        """
        partition = store_partition_factory("db")
        # Wrap the real Rdict so calls are forwarded but their order is recorded
        db_spy = MagicMock(wraps=partition._db)
        partition._db = db_spy

        partition.close()

        assert db_spy.mock_calls == [
            call.cancel_all_background(True),
            call.close(),
        ]


class TestRocksDBOpenRetryStopEvent:
    def test_open_retry_aborts_promptly_on_stop_event(self, tmp_path):
        """
        When a stop_event is set, the open-retry loop must abort immediately
        instead of sleeping through open_retry_backoff * open_max_retries, so a
        lock-waiting instance stays promptly killable.
        """
        from quixstreams.state.rocksdb.exceptions import RocksDBOpenAborted

        path = (tmp_path / "db").as_posix()
        # First partition holds the OS lock on the DB
        holder = RocksDBStorePartition(path, options=RocksDBOptions(open_max_retries=0))

        stop_event = Event()
        stop_event.set()  # application is already stopping

        start = time.monotonic()
        with pytest.raises(RocksDBOpenAborted):
            RocksDBStorePartition(
                path,
                options=RocksDBOptions(open_max_retries=100, open_retry_backoff=5.0),
                stop_event=stop_event,
            )
        elapsed = time.monotonic() - start

        # Must abort on the first backoff, not sleep the full retry budget
        assert elapsed < 5.0

        holder.close()

    def test_open_retry_without_stop_event_still_retries(self, tmp_path, executor):
        """
        The stop_event is optional — without it the retry loop behaves exactly
        as before (retries until the lock is released).
        """
        path = (tmp_path / "db").as_posix()
        holder = RocksDBStorePartition(path, options=RocksDBOptions(open_max_retries=0))

        def _release():
            time.sleep(2)
            holder.close()

        executor.submit(_release)

        # No stop_event -> must retry and eventually succeed once lock is freed
        partition = RocksDBStorePartition(
            path,
            options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1.0),
        )
        partition.close()
