"""
Tests for the RocksDB lock-contention / rebalance-handover fixes.

See docs/rocksdb-lock-contention-analysis.md:
    * fast shutdown  — cancel background flush/compaction before close()
    * stage 3        — stop-flag-aware open-retry loop
"""

import logging
import time
from threading import Event
from unittest.mock import MagicMock, call

import pytest

from quixstreams.state.rocksdb import (
    OpenDeadline,
    RocksDBOptions,
    RocksDBStorePartition,
)
from quixstreams.state.rocksdb.exceptions import RocksDBOpenAborted


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

    def test_close_falls_back_to_plain_close_without_cancel_all_background(
        self, store_partition_factory
    ):
        """
        cancel_all_background is not present in every rocksdict release within
        the supported `>=0.3,<0.4` range. When it's absent, close() must fall
        back to the plain (slower) close instead of raising.
        """
        partition = store_partition_factory("db")
        real_db = partition._db
        # A db stub without a cancel_all_background attribute at all
        partition._db = MagicMock(spec=["close"])
        try:
            partition.close()
        finally:
            real_db.close()

        partition._db.close.assert_called_once_with()

    def test_close_swallows_shutdown_in_progress_error(
        self, store_partition_factory, caplog
    ):
        """
        After cancel_all_background(), RocksDB may report "Shutdown in
        progress" from close(). The DB still closes cleanly and the lock is
        released, so close() must treat it as benign and log a debug line.
        """
        partition = store_partition_factory("db")
        real_db = partition._db
        db_stub = MagicMock()
        db_stub.close.side_effect = Exception(
            "IO error: Shutdown in progress: background work is cancelled"
        )
        partition._db = db_stub
        try:
            with caplog.at_level(logging.DEBUG):
                partition.close()  # must not raise
        finally:
            real_db.close()

        db_stub.cancel_all_background.assert_called_once_with(True)
        db_stub.close.assert_called_once_with()
        # Finding 7: the benign swallow is logged (only reached after a
        # successful cancel_all_background()).
        assert "Benign" in caplog.text
        assert "lock released" in caplog.text.lower()

    def test_close_still_closes_when_cancel_raises(
        self, store_partition_factory, caplog
    ):
        """
        Finding 6: a cancel_all_background() failure must never skip db.close() -
        that would leak the OS lock (the exact livelock this PR fixes). close()
        logs a warning and attempts close anyway; if close() then succeeds, no
        exception escapes.
        """
        partition = store_partition_factory("db")
        real_db = partition._db
        db_stub = MagicMock()
        db_stub.cancel_all_background.side_effect = Exception("cancel failed")
        partition._db = db_stub
        try:
            with caplog.at_level(logging.WARNING):
                partition.close()  # must not raise
        finally:
            real_db.close()

        db_stub.cancel_all_background.assert_called_once_with(True)
        db_stub.close.assert_called_once_with()
        assert "Failed to cancel background work" in caplog.text

    def test_close_propagates_shutdown_in_progress_on_plain_close(
        self, store_partition_factory
    ):
        """
        Finding 7: on the plain-close fallback (no cancel_all_background attr),
        a "shutdown in progress" error is NOT benign - background work was never
        cancelled - so it must now propagate instead of being swallowed.
        """
        partition = store_partition_factory("db")
        real_db = partition._db
        db_stub = MagicMock(spec=["close"])
        db_stub.close.side_effect = Exception(
            "IO error: Shutdown in progress: background work is cancelled"
        )
        partition._db = db_stub
        try:
            with pytest.raises(Exception, match="Shutdown in progress"):
                partition.close()
        finally:
            real_db.close()

    def test_close_reraises_other_close_errors(self, store_partition_factory):
        """
        Only the benign "Shutdown in progress" status is swallowed - any other
        close() failure must propagate.
        """
        partition = store_partition_factory("db")
        real_db = partition._db
        db_stub = MagicMock()
        db_stub.close.side_effect = Exception("Corruption: bad block contents")
        partition._db = db_stub
        try:
            with pytest.raises(Exception, match="Corruption"):
                partition.close()
        finally:
            real_db.close()


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


class TestStopEventWiring:
    """
    The stop_event must be threaded from the StateStoreManager down to every
    RocksDB-family store partition, so the open-retry loop can honour the
    application's stop signal in production.
    """

    def test_rocksdb_store_forwards_stop_event_to_partition(self, tmp_path):
        from quixstreams.state.rocksdb.store import RocksDBStore

        stop_event = Event()
        store = RocksDBStore(
            name="default",
            stream_id="s1",
            base_dir=str(tmp_path),
            stop_event=stop_event,
        )
        partition = store.create_new_partition(0)
        try:
            assert partition._stop_event is stop_event
        finally:
            partition.close()

    def test_windowed_store_forwards_stop_event_to_partition(self, tmp_path):
        from quixstreams.state.rocksdb.windowed.store import WindowedRocksDBStore

        stop_event = Event()
        store = WindowedRocksDBStore(
            name="default",
            stream_id="s1",
            base_dir=str(tmp_path),
            stop_event=stop_event,
        )
        partition = store.create_new_partition(0)
        try:
            assert partition._stop_event is stop_event
        finally:
            partition.close()

    def test_timestamped_store_forwards_stop_event_to_partition(self, tmp_path):
        from quixstreams.state.rocksdb.timestamped import TimestampedStore

        stop_event = Event()
        store = TimestampedStore(
            name="default",
            stream_id="s1",
            base_dir=str(tmp_path),
            grace_ms=0,
            keep_duplicates=False,
            stop_event=stop_event,
        )
        partition = store.create_new_partition(0)
        try:
            assert partition._stop_event is stop_event
        finally:
            partition.close()

    def test_state_manager_forwards_stop_event_end_to_end(self, tmp_path):
        from quixstreams.state import StateStoreManager

        stop_event = Event()
        manager = StateStoreManager(
            state_dir=str(tmp_path / "state"), stop_event=stop_event
        )
        manager.register_store(stream_id="s1", store_name="default")
        store = manager.get_store(stream_id="s1", store_name="default")
        partition = store.assign_partition(0)
        try:
            assert partition._stop_event is stop_event
        finally:
            store.revoke_partition(0)


class TestRocksDBOpenRetryWarning:
    def test_open_retry_warning_includes_path_and_attempt(self, tmp_path, caplog):
        """
        Finding 5: the open-retry warning must name the DB path and the attempt
        counter as `attempt {n}/{max}` (both were previously dropped).
        """
        path = (tmp_path / "db").as_posix()
        holder = RocksDBStorePartition(path, options=RocksDBOptions(open_max_retries=0))

        try:
            with caplog.at_level(logging.WARNING):
                with pytest.raises(Exception):
                    # Second open contends for the held lock and gives up after
                    # two attempts, warning once ("attempt 1/2").
                    RocksDBStorePartition(
                        path,
                        options=RocksDBOptions(
                            open_max_retries=2, open_retry_backoff=0.1
                        ),
                    )
        finally:
            holder.close()

        assert path in caplog.text
        assert "attempt 1/2" in caplog.text


class TestRocksDBOpenDeadline:
    """
    The shared per-assign open deadline bounds the *total* RocksDB-open time
    across all partitions opened during a single _on_assign.
    """

    def test_open_aborts_within_budget_raising_lock_error(self, tmp_path):
        """
        Finding 3: when the shared open budget is exhausted, the open-retry loop
        stops and re-raises the underlying lock error (NOT RocksDBOpenAborted,
        which is reserved for graceful stop), well before the full retry budget.
        """
        path = (tmp_path / "db").as_posix()
        # First partition holds the OS lock on the DB
        holder = RocksDBStorePartition(path, options=RocksDBOptions(open_max_retries=0))

        deadline = OpenDeadline()
        deadline.arm(0.2)  # tiny per-assign budget

        start = time.monotonic()
        with pytest.raises(Exception) as exc_info:
            RocksDBStorePartition(
                path,
                options=RocksDBOptions(open_max_retries=100, open_retry_backoff=5.0),
                open_deadline=deadline,
            )
        elapsed = time.monotonic() - start

        # A deadline overrun keeps the lock-error failure semantics, it must NOT
        # be the graceful-stop RocksDBOpenAborted.
        assert not isinstance(exc_info.value, RocksDBOpenAborted)
        # Aborted by the budget, not by sleeping the full retry budget.
        assert elapsed < 100 * 5.0
        assert elapsed < 5.0

        holder.close()

    def test_disarmed_deadline_does_not_bound_open(self, tmp_path, executor):
        """
        A disarmed deadline never expires, so the open-retry loop behaves exactly
        as before (retries until the lock is released).
        """
        path = (tmp_path / "db").as_posix()
        holder = RocksDBStorePartition(path, options=RocksDBOptions(open_max_retries=0))

        deadline = OpenDeadline()  # never armed -> expired() is always False

        def _release():
            time.sleep(2)
            holder.close()

        executor.submit(_release)

        partition = RocksDBStorePartition(
            path,
            options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1.0),
            open_deadline=deadline,
        )
        partition.close()


class TestOpenDeadlineWiring:
    """
    The open_deadline must be threaded from the StateStoreManager down to every
    RocksDB-family store partition, exactly like the existing stop_event.
    """

    def test_rocksdb_store_forwards_open_deadline_to_partition(self, tmp_path):
        from quixstreams.state.rocksdb.store import RocksDBStore

        deadline = OpenDeadline()
        store = RocksDBStore(
            name="default",
            stream_id="s1",
            base_dir=str(tmp_path),
            open_deadline=deadline,
        )
        partition = store.create_new_partition(0)
        try:
            assert partition._open_deadline is deadline
        finally:
            partition.close()

    def test_windowed_store_forwards_open_deadline_to_partition(self, tmp_path):
        from quixstreams.state.rocksdb.windowed.store import WindowedRocksDBStore

        deadline = OpenDeadline()
        store = WindowedRocksDBStore(
            name="default",
            stream_id="s1",
            base_dir=str(tmp_path),
            open_deadline=deadline,
        )
        partition = store.create_new_partition(0)
        try:
            assert partition._open_deadline is deadline
        finally:
            partition.close()

    def test_timestamped_store_forwards_open_deadline_to_partition(self, tmp_path):
        from quixstreams.state.rocksdb.timestamped import TimestampedStore

        deadline = OpenDeadline()
        store = TimestampedStore(
            name="default",
            stream_id="s1",
            base_dir=str(tmp_path),
            grace_ms=0,
            keep_duplicates=False,
            open_deadline=deadline,
        )
        partition = store.create_new_partition(0)
        try:
            assert partition._open_deadline is deadline
        finally:
            partition.close()

    def test_state_manager_forwards_open_deadline_end_to_end(self, tmp_path):
        from quixstreams.state import StateStoreManager

        deadline = OpenDeadline()
        manager = StateStoreManager(
            state_dir=str(tmp_path / "state"), open_deadline=deadline
        )
        manager.register_store(stream_id="s1", store_name="default")
        store = manager.get_store(stream_id="s1", store_name="default")
        partition = store.assign_partition(0)
        try:
            assert partition._open_deadline is deadline
        finally:
            store.revoke_partition(0)
