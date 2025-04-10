from typing import Any, Optional

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)

from .partition import RocksDBStorePartition
from .store import RocksDBStore

__all__ = (
    "TimestampedStore",
    "TimestampedStorePartition",
    "TimestampedPartitionTransaction",
)


class TimestampedPartitionTransaction(PartitionTransaction):
    """
    A partition-specific transaction handler for the `TimestampedStore`.

    Provides timestamp-aware methods for querying key-value pairs
    based on a timestamp, alongside standard transaction operations.
    It interacts with both an in-memory update cache and the persistent RocksDB store.
    """

    # Override the type hint from the parent class (`PartitionTransaction`).
    # This informs type checkers like mypy that in this specific subclass,
    # `_partition` is a `TimestampedStorePartition` (defined below),
    # which has methods like `.iter_items()` that the base type might lack.
    # The string quotes are necessary for the forward reference.
    _partition: "TimestampedStorePartition"

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def get_last(
        self,
        timestamp: int,
        prefix: bytes,
        cf_name: str = "default",
    ) -> Optional[Any]:
        """Get the latest value for a prefix up to a given timestamp.

        Searches both the transaction's update cache and the underlying RocksDB store
        to find the value associated with the given `prefix` that has the highest
        timestamp less than or equal to the provided `timestamp`.

        The search prioritizes values from the update cache if their timestamps are
        more recent than those found in the store.

        :param timestamp: The upper bound timestamp (inclusive) in milliseconds.
        :param prefix: The key prefix to search for.
        :param cf_name: The column family name.
        :return: The deserialized value if found, otherwise None.
        """
        # Add +1 because the storage `.iter_items()` is exclusive on the upper bound
        key = self._serialize_key(timestamp + 1, prefix=prefix)
        value: Optional[bytes] = None

        cached = self._update_cache.iter_items(
            prefix=prefix,
            backwards=True,
            cf_name=cf_name,
        )
        for cached_key, cached_value in cached:
            if prefix < cached_key < key:
                value = cached_value
                break

        stored = self._partition.iter_items(
            lower_bound=prefix,
            upper_bound=key,
            backwards=True,
            cf_name=cf_name,
        )
        for stored_key, stored_value in stored:
            if value is None or cached_key < stored_key:
                value = stored_value
            # We only care about the first item found when iterating backwards
            # from the upper bound, hence the break.
            break

        return self._deserialize_value(value) if value is not None else None

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set(self, timestamp: int, value: Any, prefix: bytes, cf_name: str = "default"):
        """Set a value associated with a prefix and timestamp.

        This method acts as a proxy, passing the provided `timestamp` and `prefix`
        to the parent `set` method. The parent method internally serializes these
        into a combined key before storing the value in the update cache.

        :param timestamp: Timestamp associated with the value in milliseconds.
        :param value: The value to store.
        :param prefix: The key prefix.
        :param cf_name: Column family name.
        """
        super().set(timestamp, value, prefix, cf_name)


class TimestampedStorePartition(RocksDBStorePartition):
    """
    Represents a single partition within a `TimestampedStore`.

    This class is responsible for managing the state of one partition and creating
    `TimestampedPartitionTransaction` instances to handle atomic operations for that partition.
    """

    partition_transaction_class = TimestampedPartitionTransaction


class TimestampedStore(RocksDBStore):
    """
    A RocksDB-backed state store implementation that manages key-value pairs
    associated with timestamps.

    Uses `TimestampedStorePartition` to manage individual partitions.
    """

    store_partition_class = TimestampedStorePartition
