from contextlib import contextmanager
from typing import Any
from unittest import mock

import pytest

from quixstreams.state.rocksdb.partition import Marker
from quixstreams.state.rocksdb.timestamped import (
    TimestampedPartitionTransaction,
    TimestampedStore,
)


@pytest.fixture
def store_type():
    # This fixture is used by the `store_factory` fixture.
    # Full dependency chain is:
    # `store_type` -> `store_factory` -> `store` -> `transaction`
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


def test_get_last_ignore_deleted(
    transaction: TimestampedPartitionTransaction,
):
    with transaction() as tx:
        tx.set(timestamp=9, value="value9-stored", prefix=b"key")

    with transaction() as tx:
        tx._expire(timestamp=10, prefix=b"key")
        tx.set(timestamp=8, value="value8-cached", prefix=b"key")
        assert tx.get_last(timestamp=10, prefix=b"key") == "value8-cached"


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
def test_get_last_returns_value_for_greater_timestamp(
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


def test_get_last_prefix_not_bytes(transaction: TimestampedPartitionTransaction):
    with transaction() as tx:
        tx.set(timestamp=10, value="value", prefix="key")
        assert tx.get_last(timestamp=10, prefix="key") == "value"
        assert tx.get_last(timestamp=10, prefix=b'"key"') == "value"


def test_get_last_from_cache_with_retention(
    transaction: TimestampedPartitionTransaction,
):
    with transaction() as tx:
        tx.set(timestamp=5, value="value", prefix=b"key")
        assert tx.get_last(timestamp=10, prefix=b"key") == "value"
        assert tx.get_last(timestamp=10, prefix=b"key", retention=5) == "value"
        assert tx.get_last(timestamp=10, prefix=b"key", retention=4) == None


def test_get_last_from_store_with_retention(
    transaction: TimestampedPartitionTransaction,
):
    with transaction() as tx:
        tx.set(timestamp=5, value="value", prefix=b"key")

    with transaction() as tx:
        assert tx.get_last(timestamp=10, prefix=b"key") == "value"
        assert tx.get_last(timestamp=10, prefix=b"key", retention=5) == "value"
        assert tx.get_last(timestamp=10, prefix=b"key", retention=4) == None


@mock.patch("quixstreams.state.rocksdb.timestamped.EXPIRATION_COUNTER", 1000)
def test_get_last_from_cache_with_expire_call(
    transaction: TimestampedPartitionTransaction,
):
    with transaction() as tx:
        tx.set(timestamp=5, value="value1", prefix=b"key")
        assert tx._update_cache.get_updates_for_prefix(prefix=b"key") == {
            b"key|\x00\x00\x00\x00\x00\x00\x00\x05": b'"value1"',
        }

        # Expiration counter is exhausted for this `get_last` call
        # `_expire` method is called with `lower_bound_timestamp` = 10 - 4 = 6
        # Everything below timestamp 6 gets expired.
        assert tx.get_last(timestamp=10, prefix=b"key", retention=4) == None
        assert tx._update_cache.get_updates_for_prefix(prefix=b"key") == {}


@mock.patch("quixstreams.state.rocksdb.timestamped.EXPIRATION_COUNTER", 1000)
def test_get_last_from_store_with_expire_call(
    transaction: TimestampedPartitionTransaction,
):
    key = b"key|\x00\x00\x00\x00\x00\x00\x00\x05"

    with transaction() as tx:
        tx.set(timestamp=5, value="value1", prefix=b"key")

    with transaction() as tx:
        assert tx._partition.get(key) == b'"value1"'
        assert not tx._update_cache.get_deletes()

        # Expiration counter is exhausted for this `get_last` call
        # `_expire` method is called with `lower_bound_timestamp` = 10 - 4 = 6
        # Everything belowe timestamp 6 gets expired.
        assert tx.get_last(timestamp=10, prefix=b"key", retention=4) == None
        assert key in tx._update_cache.get_deletes()

    with transaction() as tx:
        assert tx._partition.get(key) is Marker.UNDEFINED


def test_expire_cached(transaction: TimestampedPartitionTransaction):
    with transaction() as tx:
        tx.set(timestamp=1, value="value1", prefix=b"key")
        tx.set(timestamp=10, value="value10", prefix=b"key")
        tx.set(timestamp=11, value="value11", prefix=b"key")

        tx._expire(timestamp=11, prefix=b"key")

        assert tx.get_last(timestamp=10, prefix=b"key") == None
        assert tx.get_last(timestamp=11, prefix=b"key") == "value11"


def test_expire_stored(transaction: TimestampedPartitionTransaction):
    with transaction() as tx:
        tx.set(timestamp=1, value="value1", prefix=b"key")
        tx.set(timestamp=10, value="value10", prefix=b"key")
        tx.set(timestamp=11, value="value11", prefix=b"key")

    with transaction() as tx:
        tx._expire(timestamp=11, prefix=b"key")

        assert tx.get_last(timestamp=10, prefix=b"key") == None
        assert tx.get_last(timestamp=11, prefix=b"key") == "value11"


def test_expire_idempotent(transaction: TimestampedPartitionTransaction):
    with transaction() as tx:
        tx.set(timestamp=1, value="value1", prefix=b"key")

    with transaction() as tx:
        tx.set(timestamp=10, value="value10", prefix=b"key")

        tx._expire(timestamp=11, prefix=b"key")
        tx._expire(timestamp=11, prefix=b"key")

        assert tx.get_last(timestamp=10, prefix=b"key") == None
