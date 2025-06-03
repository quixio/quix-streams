from contextlib import contextmanager

import pytest

from quixstreams.state.rocksdb.metadata import (
    GLOBAL_COUNTER_CF_NAME,
    GLOBAL_COUNTER_KEY,
)
from quixstreams.state.rocksdb.store import RocksDBStore
from quixstreams.state.rocksdb.transaction import MAX_UINT64


@pytest.fixture
def transaction(store: RocksDBStore):
    @contextmanager
    def _transaction():
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            yield tx

    return _transaction


def test_increment_counter(transaction):
    with transaction() as tx:
        assert tx._increment_counter() == 0
        assert tx._counter == 0
        assert tx._increment_counter() == 1
        assert tx._counter == 1

    with transaction() as tx:
        assert tx._increment_counter() == 2
        assert tx._counter == 2
        assert tx._increment_counter() == 3
        assert tx._counter == 3


def test_increment_counter_reset_to_zero(transaction):
    with transaction() as tx:
        # Set the counter to the maximum value
        tx._counter = MAX_UINT64 - 1
        assert tx._increment_counter() == MAX_UINT64
        assert tx._counter == MAX_UINT64

        # Next increment is going to reset the counter to 0
        assert tx._increment_counter() == 0
        assert tx._counter == 0


def test_persist_counter_called_only_once(transaction):
    with transaction() as tx:
        assert tx._increment_counter() == 0
        assert tx._increment_counter() == 1
        assert tx._increment_counter() == 2

        # The counter is not yet persisted
        assert (
            tx.get(
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                default=None,
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
            is None
        )

    with transaction() as tx:
        assert (
            tx.get(
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                default=None,
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
            == 2
        )
