from contextlib import contextmanager
from typing import Any

import pytest

from quixstreams.state.rocksdb.timestamped import (
    TimestampedPartitionTransaction,
    TimestampedStore,
)

PARTITION = 0
VALUE = "value"
PREFIX = b"key"


@pytest.fixture
def store_type():
    return TimestampedStore


@pytest.fixture
def transaction(store: TimestampedStore):
    @contextmanager
    def _transaction():
        store.assign_partition(PARTITION)
        with store.start_partition_transaction(PARTITION) as tx:
            yield tx

    return _transaction


@pytest.mark.parametrize(
    ["set_timestamp", "get_timestamp", "expected"],
    [
        (10, 10, "value"),
        (10, 11, "value"),
        (10, 9, None),
    ],
)
def test_set_get_last(
    transaction: TimestampedPartitionTransaction,
    set_timestamp: int,
    get_timestamp: int,
    expected: Any,
):
    with transaction() as tx:
        tx.set(timestamp=set_timestamp, value=VALUE, prefix=PREFIX)
        assert tx.get_last(timestamp=get_timestamp, prefix=PREFIX) == expected
