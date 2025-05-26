from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Union

import pytest

from quixstreams.dataframe.utils import ensure_milliseconds
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
    def _transaction(grace_ms: Union[int, timedelta] = timedelta(days=7)):
        store._grace_ms = ensure_milliseconds(grace_ms)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            yield tx

    return _transaction


class TestTimestampedPartitionTransaction:
    @pytest.mark.parametrize(
        ["set_timestamp", "get_timestamp", "expected"],
        [
            pytest.param(10, 10, "value", id="set_timestamp_equal_to_get_timestamp"),
            pytest.param(10, 11, "value", id="set_timestamp_less_than_get_timestamp"),
            pytest.param(10, 9, None, id="set_timestamp_greater_than_get_timestamp"),
        ],
    )
    def test_get_latest_from_cache(
        self,
        transaction: TimestampedPartitionTransaction,
        set_timestamp: int,
        get_timestamp: int,
        expected: Any,
    ):
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=set_timestamp, value="value", prefix=b"key")
            assert tx.get_latest(timestamp=get_timestamp, prefix=b"key") == expected

    @pytest.mark.parametrize(
        ["set_timestamp", "get_timestamp", "expected"],
        [
            pytest.param(10, 10, "value", id="set_timestamp_equal_to_get_timestamp"),
            pytest.param(10, 11, "value", id="set_timestamp_less_than_get_timestamp"),
            pytest.param(10, 9, None, id="set_timestamp_greater_than_get_timestamp"),
        ],
    )
    def test_get_latest_from_store(
        self,
        transaction: TimestampedPartitionTransaction,
        set_timestamp: int,
        get_timestamp: int,
        expected: Any,
    ):
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=set_timestamp, value="value", prefix=b"key")

        with transaction() as tx:
            assert tx.get_latest(timestamp=get_timestamp, prefix=b"key") == expected

    @pytest.mark.parametrize(
        ["set_timestamp_stored", "set_timestamp_cached", "get_timestamp", "expected"],
        [
            pytest.param(3, 2, 5, "stored", id="stored-greater-than-cached"),
            pytest.param(2, 3, 5, "cached", id="cached-greater-than-stored"),
        ],
    )
    def test_get_latest_returns_value_for_greater_timestamp(
        self,
        transaction: TimestampedPartitionTransaction,
        set_timestamp_stored: int,
        set_timestamp_cached: int,
        get_timestamp: int,
        expected: Any,
    ):
        with transaction() as tx:
            tx.set_for_timestamp(
                timestamp=set_timestamp_stored, value="stored", prefix=b"key"
            )

        with transaction() as tx:
            tx.set_for_timestamp(
                timestamp=set_timestamp_cached, value="cached", prefix=b"key"
            )
            assert tx.get_latest(timestamp=get_timestamp, prefix=b"key") == expected

    def test_get_latest_prefix_not_bytes(
        self, transaction: TimestampedPartitionTransaction
    ):
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=10, value="value", prefix="key")
            assert tx.get_latest(timestamp=10, prefix="key") == "value"
            assert tx.get_latest(timestamp=10, prefix=b'"key"') == "value"

    def test_get_latest_for_out_of_order_timestamp(
        self,
        transaction: TimestampedPartitionTransaction,
    ):
        with transaction(grace_ms=5) as tx:
            tx.set_for_timestamp(timestamp=10, value="value10", prefix=b"key")
            assert tx.get_latest(timestamp=10, prefix=b"key") == "value10"
            tx.set_for_timestamp(timestamp=5, value="value5", prefix=b"key")
            tx.set_for_timestamp(timestamp=4, value="value4", prefix=b"key")

        with transaction() as tx:
            assert tx.get_latest(timestamp=5, prefix=b"key") == "value5"

            # Retention watermark is 10 - 5 = 5 so everything lower is ignored
            assert tx.get_latest(timestamp=4, prefix=b"key") is None

    def test_set_for_timestamp_with_prefix_not_bytes(
        self,
        transaction: TimestampedPartitionTransaction,
    ):
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=10, value="value", prefix="key")
            assert tx.get_latest(timestamp=10, prefix="key") == "value"
            assert tx.get_latest(timestamp=10, prefix=b'"key"') == "value"

    def test_set_for_timestamp_with_grace_cached(
        self,
        transaction: TimestampedPartitionTransaction,
    ):
        with transaction(grace_ms=2) as tx:
            tx.set_for_timestamp(timestamp=2, value="v2", prefix=b"key")
            tx.set_for_timestamp(timestamp=5, value="v5", prefix=b"key")
            assert tx.get_latest(timestamp=2, prefix=b"key") is None
            assert tx.get_latest(timestamp=5, prefix=b"key") == "v5"

    def test_set_for_timestamp_with_grace_stored(
        self,
        transaction: TimestampedPartitionTransaction,
    ):
        with transaction(grace_ms=2) as tx:
            tx.set_for_timestamp(timestamp=2, value="v2", prefix=b"key")
            tx.set_for_timestamp(timestamp=5, value="v5", prefix=b"key")

        with transaction(grace_ms=2) as tx:
            assert tx.get_latest(timestamp=2, prefix=b"key") is None
            assert tx.get_latest(timestamp=5, prefix=b"key") == "v5"

    def test_expire_multiple_keys(self, transaction: TimestampedPartitionTransaction):
        with transaction(grace_ms=10) as tx:
            tx.set_for_timestamp(timestamp=1, value="11", prefix=b"key1")
            tx.set_for_timestamp(timestamp=1, value="21", prefix=b"key2")
            tx.set_for_timestamp(timestamp=12, value="112", prefix=b"key1")
            tx.set_for_timestamp(timestamp=12, value="212", prefix=b"key2")

        with transaction(grace_ms=10) as tx:
            assert tx.get(key=1, prefix=b"key1") is None
            assert tx.get(key=1, prefix=b"key2") is None
            assert tx.get(key=12, prefix=b"key1") == "112"
            assert tx.get(key=12, prefix=b"key2") == "212"

            # Expiration advances only on `set_for_timestamp` calls
            assert tx.get_latest(timestamp=30, prefix=b"key1") == "112"
            assert tx.get_latest(timestamp=30, prefix=b"key2") == "212"

    def test_set_for_timestamp_overwrites_value_with_same_timestamp(
        self,
        transaction: TimestampedPartitionTransaction,
    ):
        with transaction() as tx:
            tx.set_for_timestamp(timestamp=1, value="11", prefix=b"key")
            tx.set_for_timestamp(timestamp=1, value="21", prefix=b"key")
            assert tx.get_latest(timestamp=1, prefix=b"key") == "21"

        with transaction() as tx:
            assert tx.get_latest(timestamp=1, prefix=b"key") == "21"
