from typing import Any, Optional, Union, cast

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.cache import TimestampsCache
from quixstreams.state.rocksdb.metadata import (
    LATEST_TIMESTAMP_KEY,
    LATEST_TIMESTAMPS_CF_NAME,
)
from quixstreams.state.serialization import (
    DumpsFunc,
    LoadsFunc,
    int_to_int64_bytes,
    serialize,
)

from .partition import RocksDBStorePartition
from .store import RocksDBStore

__all__ = (
    "TimestampedStore",
    "TimestampedStorePartition",
    "TimestampedPartitionTransaction",
)

DAYS_7 = 7 * 24 * 60 * 60 * 1000


class TimestampedPartitionTransaction(PartitionTransaction):
    """
    A partition-specific transaction handler for the `TimestampedStore`.

    Provides timestamp-aware methods for querying key-value pairs
    based on a timestamp, alongside standard transaction operations.
    It interacts with both an in-memory update cache and the persistent RocksDB store.
    """

    def __init__(
        self,
        partition: "TimestampedStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> None:
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition: TimestampedStorePartition = cast(
            "TimestampedStorePartition", self._partition
        )
        self._latest_timestamps: TimestampsCache = TimestampsCache(
            key=LATEST_TIMESTAMP_KEY,
            cf_name=LATEST_TIMESTAMPS_CF_NAME,
        )

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def get_last(
        self,
        timestamp: int,
        prefix: Any,
        retention_ms: int = DAYS_7,
        cf_name: str = "default",
    ) -> Optional[Any]:
        """Get the latest value for a prefix up to a given timestamp.

        Searches both the transaction's update cache and the underlying RocksDB store
        to find the value associated with the given `prefix` that has the highest
        timestamp less than or equal to the provided `timestamp`.

        The search prioritizes values from the update cache if their timestamps are
        more recent than those found in the store.

        :param timestamp: The upper bound timestamp (inclusive) in milliseconds.
        :param prefix: The key prefix.
        :param retention_ms: The retention period in milliseconds.
        :param cf_name: The column family name.
        :return: The deserialized value if found, otherwise None.
        """

        prefix = self._ensure_bytes(prefix)
        latest_timestamp = self._get_latest_timestamp(prefix, timestamp)

        # Negative retention is not allowed
        lower_bound = self._serialize_key(
            max(latest_timestamp - retention_ms, 0), prefix
        )
        # +1 because upper bound is exclusive
        upper_bound = self._serialize_key(timestamp + 1, prefix)

        value: Optional[bytes] = None

        deletes = self._update_cache.get_deletes(cf_name=cf_name)
        updates = self._update_cache.get_updates(cf_name=cf_name).get(prefix, {})

        cached = sorted(updates.items(), reverse=True)
        for cached_key, cached_value in cached:
            if lower_bound <= cached_key < upper_bound and cached_key not in deletes:
                value = cached_value
                break

        stored = self._partition.iter_items(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            backwards=True,
            cf_name=cf_name,
        )
        for stored_key, stored_value in stored:
            if stored_key in deletes:
                continue

            if value is None or cached_key < stored_key:
                value = stored_value

            # We only care about the first not deleted item when
            # iterating backwards from the upper bound.
            break

        return self._deserialize_value(value) if value is not None else None

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set_for_timestamp(
        self,
        timestamp: int,
        value: Any,
        prefix: Any,
        retention_ms: int = DAYS_7,
        cf_name: str = "default",
    ) -> None:
        """Set a value for the timestamp.

        This method acts as a proxy, passing the provided `timestamp` and `prefix`
        to the parent `set` method. The parent method internally serializes these
        into a combined key before storing the value in the update cache.

        Additionally, it triggers the expiration logic.

        :param timestamp: Timestamp associated with the value in milliseconds.
        :param value: The value to store.
        :param prefix: The key prefix.
        :param cf_name: Column family name.
        """
        prefix = self._ensure_bytes(prefix)
        super().set(timestamp, value, prefix, cf_name=cf_name)
        self._expire(
            timestamp=timestamp,
            prefix=prefix,
            retention_ms=retention_ms,
            cf_name=cf_name,
        )

    def _expire(
        self, timestamp: int, prefix: bytes, retention_ms: int, cf_name: str = "default"
    ) -> None:
        """
        Delete all entries for a given prefix with timestamps less than the
        provided timestamp.

        This applies to both the in-memory update cache and the underlying
        RocksDB store within the current transaction.

        :param timestamp: The upper bound timestamp (exclusive) in milliseconds.
            Entries with timestamps strictly less than this will be deleted.
        :param prefix: The key prefix.
        :param cf_name: Column family name.
        """

        latest_timestamp = self._get_latest_timestamp(prefix, timestamp)
        self._set_timestamp(prefix, latest_timestamp)

        key = self._serialize_key(max(timestamp - retention_ms, 0), prefix)

        cached = self._update_cache.get_updates(cf_name=cf_name).get(prefix, {})
        # Cast to list to avoid RuntimeError: dictionary changed size during iteration
        for cached_key in list(cached):
            if cached_key < key:
                self._update_cache.delete(cached_key, prefix, cf_name=cf_name)

        stored = self._partition.iter_items(
            lower_bound=prefix,
            upper_bound=key,
            cf_name=cf_name,
        )
        for stored_key, _ in stored:
            self._update_cache.delete(stored_key, prefix, cf_name=cf_name)

    def _ensure_bytes(self, prefix: Any) -> bytes:
        if isinstance(prefix, bytes):
            return prefix
        return serialize(prefix, dumps=self._dumps)

    def _serialize_key(self, key: Union[int, bytes], prefix: bytes) -> bytes:
        if isinstance(key, int):
            return prefix + SEPARATOR + int_to_int64_bytes(key)
        elif isinstance(key, bytes):
            return prefix + SEPARATOR + key
        raise ValueError(f"Invalid key type: {type(key)}")

    def _get_latest_timestamp(self, prefix: bytes, timestamp: int) -> Any:
        """
        Get the latest timestamp for a given prefix.

        If the timestamp is not found in the cache, it is fetched from the store.
        """
        cache = self._latest_timestamps
        ts = (
            cache.timestamps.get(prefix)
            or self.get(key=cache.key, prefix=prefix, cf_name=cache.cf_name)
            or 0
        )
        cache.timestamps[prefix] = latest_timestamp = max(ts, timestamp)
        return latest_timestamp

    def _set_timestamp(self, prefix: bytes, timestamp: int) -> None:
        cache = self._latest_timestamps
        cache.timestamps[prefix] = timestamp
        self.set(key=cache.key, value=timestamp, prefix=prefix, cf_name=cache.cf_name)


class TimestampedStorePartition(RocksDBStorePartition):
    """
    Represents a single partition within a `TimestampedStore`.

    This class is responsible for managing the state of one partition and creating
    `TimestampedPartitionTransaction` instances to handle atomic operations for that partition.
    """

    partition_transaction_class = TimestampedPartitionTransaction
    additional_column_families = (LATEST_TIMESTAMPS_CF_NAME,)


class TimestampedStore(RocksDBStore):
    """
    A RocksDB-backed state store implementation that manages key-value pairs
    associated with timestamps.

    Uses `TimestampedStorePartition` to manage individual partitions.
    """

    store_partition_class = TimestampedStorePartition
