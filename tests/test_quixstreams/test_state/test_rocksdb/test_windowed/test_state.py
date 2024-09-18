from contextlib import contextmanager

import pytest


@pytest.fixture
def store(windowed_rocksdb_store_factory):
    store = windowed_rocksdb_store_factory()
    store.assign_partition(0)
    return store


@pytest.fixture
def transaction_state(store):
    @contextmanager
    def _transaction_state():
        with store.start_partition_transaction(0) as tx:
            yield tx.as_state(prefix=b"__key__")

    return _transaction_state


class TestWindowedRocksDBPartitionTransactionState:
    def test_update_window(self, transaction_state):
        with transaction_state() as state:
            state.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
            assert state.get_window(start_ms=0, end_ms=10) == 1

        with transaction_state() as state:
            assert state.get_window(start_ms=0, end_ms=10) == 1

    def test_expire_windows(self, transaction_state):
        with transaction_state() as state:
            state.update_window(start_ms=0, end_ms=10, value=1, timestamp_ms=2)
            state.update_window(start_ms=10, end_ms=20, value=2, timestamp_ms=10)

        with transaction_state() as state:
            state.update_window(start_ms=20, end_ms=30, value=3, timestamp_ms=20)
            expired = state.expire_windows(duration_ms=10)
            # "expire_windows" must update the expiration index so that the same
            # windows are not expired twice
            assert not state.expire_windows(duration_ms=10)

        assert len(expired) == 2
        assert expired == [
            ((0, 10), 1),
            ((10, 20), 2),
        ]

        with transaction_state() as state:
            assert state.get_window(start_ms=0, end_ms=10) is None
            assert state.get_window(start_ms=10, end_ms=20) is None
            assert state.get_window(start_ms=20, end_ms=30) == 3

    def test_get_latest_timestamp(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        partition = store.assign_partition(0)
        timestamp = 123
        prefix = b"__key__"
        with partition.begin() as tx:
            state = tx.as_state(prefix)
            state.update_window(0, 10, value=1, timestamp_ms=timestamp)
        store.revoke_partition(0)

        partition = store.assign_partition(0)
        with partition.begin() as tx:
            assert tx.get_latest_timestamp() == timestamp
