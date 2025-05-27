from contextlib import contextmanager

import pytest

from quixstreams.state.rocksdb.store import RocksDBStore


@pytest.fixture
def transaction(store: RocksDBStore):
    @contextmanager
    def _transaction():
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            yield tx

    return _transaction


def test_get_next_count(transaction):
    with transaction() as tx:
        assert tx._get_next_count() == 0
        assert tx._get_next_count() == 1

    with transaction() as tx:
        assert tx._get_next_count() == 2
        assert tx._get_next_count() == 3
