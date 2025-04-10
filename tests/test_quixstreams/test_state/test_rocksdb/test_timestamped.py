from contextlib import contextmanager
from typing import Any

import pytest

from quixstreams.state.rocksdb.timestamped import (
    TimestampedPartitionTransaction,
    TimestampedStore,
)


@pytest.fixture
def store_type():
    return TimestampedStore


@pytest.fixture
def transaction(store: TimestampedStore):
    @contextmanager
    def _transaction():
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
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
def test_get_last_from_cache(
    transaction: TimestampedPartitionTransaction,
    set_timestamp: int,
    get_timestamp: int,
    expected: Any,
):
    with transaction() as tx:
        tx.set(timestamp=set_timestamp, value="value", prefix=b"key")
        assert tx.get_last(timestamp=get_timestamp, prefix=b"key") == expected


@pytest.mark.parametrize(
    ["set_timestamp", "get_timestamp", "expected"],
    [
        (10, 10, "value"),
        (10, 11, "value"),
        (10, 9, None),
    ],
)
def test_get_last_from_store(
    transaction: TimestampedPartitionTransaction,
    set_timestamp: int,
    get_timestamp: int,
    expected: Any,
):
    with transaction() as tx:
        tx.set(timestamp=set_timestamp, value="value", prefix=b"key")

    with transaction() as tx:
        assert tx.get_last(timestamp=get_timestamp, prefix=b"key") == expected


@pytest.mark.parametrize(
    ["set_timestamp_stored", "set_timestamp_cached", "get_timestamp", "expected"],
    [
        pytest.param(3, 2, 5, "stored", id="stored-greater-than-cached"),
        pytest.param(2, 3, 5, "cached", id="cached-greater-than-stored"),
    ],
)
def test_get_last_stored_key_greater_than_cached_key(
    transaction: TimestampedPartitionTransaction,
    set_timestamp_stored: int,
    set_timestamp_cached: int,
    get_timestamp: int,
    expected: Any,
):
    with transaction() as tx:
        tx.set(timestamp=set_timestamp_stored, value="stored", prefix=b"key")

    with transaction() as tx:
        tx.set(timestamp=set_timestamp_cached, value="cached", prefix=b"key")
        assert tx.get_last(timestamp=get_timestamp, prefix=b"key") == expected
