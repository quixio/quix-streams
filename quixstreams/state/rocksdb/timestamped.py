from collections import deque
from typing import Any, Optional, Union, cast

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.cache import TimestampsCache
from quixstreams.state.serialization import (
    DumpsFunc,
    LoadsFunc,
    encode_integer_pair,
    serialize,
)

from .partition import RocksDBStorePartition
from .store import RocksDBStore

__all__ = (
    "TimestampedStore",
    "TimestampedStorePartition",
    "TimestampedPartitionTransaction",
)

DAY_MS = 24 * 60 * 60 * 1000

MIN_ELIGIBLE_TIMESTAMPS_CF_NAME = "__min-eligible-timestamps__"
MIN_ELIGIBLE_TIMESTAMPS_KEY = b"__min_eligible_timestamps__"


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
        """
        Initializes a new `TimestampedPartitionTransaction`.

        :param partition: The `TimestampedStorePartition` this transaction belongs to.
        :param dumps: The serialization function for keys/values.
        :param loads: The deserialization function for keys/values.
        :param changelog_producer: Optional `ChangelogProducer` for recording changes.
        """
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition: TimestampedStorePartition = cast(
            "TimestampedStorePartition", self._partition
        )
        self._min_eligible_timestamps: TimestampsCache = TimestampsCache(
            key=MIN_ELIGIBLE_TIMESTAMPS_KEY,
            cf_name=MIN_ELIGIBLE_TIMESTAMPS_CF_NAME,
        )

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def get_latest(self, timestamp: int, prefix: Any) -> Optional[Any]:
        """Get the latest value for a prefix up to a given timestamp.

        Searches both the transaction's update cache and the underlying RocksDB store
        to find the value associated with the given `prefix` that has the highest
        timestamp less than or equal to the provided `timestamp`.

        The search considers both the update cache and the store. It returns the value
        associated with the key that has the numerically largest timestamp less than
        or equal to the provided `timestamp`. If multiple entries exist for the same
        prefix across the cache and store within the valid time range, the one with
        the highest timestamp is chosen.

        :param timestamp: The upper bound timestamp (inclusive) in milliseconds.
        :param prefix: The key prefix.
        :return: The deserialized value if found, otherwise None.
        """
        prefix = self._ensure_bytes(prefix)
        min_eligible_timestamp = self._get_min_eligible_timestamp(prefix)

        if timestamp < min_eligible_timestamp:
            return None

        lower_bound = self._serialize_key(min_eligible_timestamp, prefix)
        # +1 because upper bound is exclusive
        upper_bound = self._serialize_key(timestamp + 1, prefix)

        deletes = self._update_cache.get_deletes()
        updates = self._update_cache.get_updates().get(prefix, {})

        cached = sorted(updates.items(), reverse=True)
        value: Optional[bytes] = None
        cached_key: Optional[bytes] = None
        # First check the boundaries to skip the iteration if the cached values
        # are outside the search boundaries
        if cached and cached[0][0] >= lower_bound and cached[-1][0] <= upper_bound:
            for cached_key, cached_value in cached:
                if (
                    lower_bound <= cached_key < upper_bound
                    and cached_key not in deletes
                ):
                    value = cached_value
                    break

        stored = self._partition.iter_items(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            backwards=True,
        )
        for stored_key, stored_value in stored:
            if stored_key in deletes:
                continue

            if value is None or (cached_key and cached_key < stored_key):
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
        retention_ms: int = 7 * DAY_MS,
    ) -> None:
        """Set a value for the timestamp.

        This method acts as a proxy, passing the provided `timestamp` and `prefix`
        to the parent `set` method. The parent method internally serializes these
        into a combined key before storing the value in the update cache.

        Additionally, it updates the minimum eligible timestamp for the given prefix
        based on the `retention_ms`, which is used later during the flush process to
        expire old data.

        :param timestamp: Timestamp associated with the value in milliseconds.
        :param value: The value to store.
        :param prefix: The key prefix.
        :param retention_ms: retention for the key in milliseconds
        """
        prefix = self._ensure_bytes(prefix)
        self.set(timestamp, value, prefix)
        min_eligible_timestamp = max(
            self._get_min_eligible_timestamp(prefix),
            timestamp - retention_ms,
        )
        self._set_min_eligible_timestamp(prefix, min_eligible_timestamp)

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offsets: Optional[dict[str, int]] = None) -> None:
        """
        This method first calls `_expire()` to remove outdated entries based on
        their timestamps and retention periods, then calls the parent class's
        `prepare()` to prepare the transaction for flush.

        :param processed_offsets: the dict with <topic: offset> of the latest processed message
        """
        self._expire()
        super().prepare(processed_offsets=processed_offsets)

    def _expire(self) -> None:
        """
        Delete all entries with timestamps less than the minimum
        eligible timestamp for the given prefix.

        This applies to both the in-memory update cache and the underlying
        RocksDB store within the current transaction.
        """
        updates = self._update_cache.get_updates()
        # Accumulate the expired keys separately to avoid
        # mutating the update cache during iteration
        keys_to_delete: deque[tuple[bytes, bytes]] = deque()

        for prefix, cached in updates.items():
            min_eligible_timestamp = self._get_min_eligible_timestamp(prefix)

            key = self._serialize_key(min_eligible_timestamp, prefix)
            for cached_key in cached:
                if cached_key < key:
                    keys_to_delete.append((cached_key, prefix))

            stored = self._partition.iter_items(lower_bound=prefix, upper_bound=key)
            for stored_key, _ in stored:
                keys_to_delete.append((stored_key, prefix))

        # Mark the expired keys as deleted in the update cache
        for key, prefix in keys_to_delete:
            self._update_cache.delete(key, prefix)

    def _ensure_bytes(self, prefix: Any) -> bytes:
        if isinstance(prefix, bytes):
            return prefix
        return serialize(prefix, dumps=self._dumps)

    def _serialize_key(self, key: Union[int, bytes], prefix: bytes) -> bytes:
        if isinstance(key, int):
            # TODO: Currently using constant 0, but will be
            # replaced with a global counter in the future
            return prefix + SEPARATOR + encode_integer_pair(key, 0)
        elif isinstance(key, bytes):
            return prefix + SEPARATOR + key
        raise TypeError(f"Invalid key type: {type(key)}")

    def _get_min_eligible_timestamp(self, prefix: bytes) -> int:
        """
        Retrieves the minimum eligible timestamp for a given prefix.

        It first checks an in-memory cache (`self._min_eligible_timestamps`).
        If not found, it queries the underlying RocksDB store using `self.get()`.
        Defaults to 0 if no timestamp is found.

        :param prefix: The key prefix (bytes).
        :return: The minimum eligible timestamp (int).
        """
        cache = self._min_eligible_timestamps
        cached = cache.timestamps.get(prefix)
        if cached is not None:
            return cached
        stored = self.get(key=cache.key, prefix=prefix) or 0
        # Write the timestamp back to cache since it is known now
        cache.timestamps[prefix] = stored
        return stored

    def _set_min_eligible_timestamp(self, prefix: bytes, timestamp: int) -> None:
        """
        Sets the minimum eligible timestamp for a given prefix.

        Updates an in-memory cache (`self._min_eligible_timestamps`) and then
        persists this new minimum to the underlying RocksDB store using `self.set()`.
        The value is stored in a designated column family.

        :param prefix: The key prefix (bytes).
        :param timestamp: The minimum eligible timestamp (int) to set.
        """
        cache = self._min_eligible_timestamps
        cache.timestamps[prefix] = timestamp
        self.set(key=cache.key, value=timestamp, prefix=prefix, cf_name=cache.cf_name)


class TimestampedStorePartition(RocksDBStorePartition):
    """
    Represents a single partition within a `TimestampedStore`.

    This class is responsible for managing the state of one partition and creating
    `TimestampedPartitionTransaction` instances to handle atomic operations for that partition.
    """

    def begin(self) -> TimestampedPartitionTransaction:
        # Override the method to specify the correct return type
        return TimestampedPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )


class TimestampedStore(RocksDBStore):
    """
    A RocksDB-backed state store implementation that manages key-value pairs
    associated with timestamps.

    Uses `TimestampedStorePartition` to manage individual partitions.
    """

    store_partition_class = TimestampedStorePartition
