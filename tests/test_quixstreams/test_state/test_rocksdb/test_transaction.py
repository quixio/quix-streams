from contextlib import contextmanager

import pytest

from quixstreams.state.rocksdb.store import RocksDBStore

MAX_UINT64 = 2**64 - 1  # 18446744073709551615


@pytest.fixture
def transaction(store: RocksDBStore):
    @contextmanager
    def _transaction():
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            yield tx

    return _transaction


def test_increment_global_counter(transaction):
    with transaction() as tx:
        assert tx._increment_global_counter() == 0
        assert tx._increment_global_counter() == 1

    with transaction() as tx:
        assert tx._increment_global_counter() == 2
        assert tx._increment_global_counter() == 3


def test_increment_global_counter_reset_to_zero(transaction):
    with transaction() as tx:
        # Set the counter to the maximum value
        tx._global_counter.counter = MAX_UINT64 - 1

        assert tx._increment_global_counter() == MAX_UINT64
        assert tx._increment_global_counter() == 0
