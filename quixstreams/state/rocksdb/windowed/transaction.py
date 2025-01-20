import itertools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, cast

from rocksdict import ReadOptions

from quixstreams.state.base.transaction import PartitionTransaction
from quixstreams.state.metadata import DEFAULT_PREFIX, SEPARATOR
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc, serialize

from .metadata import (
    GLOBAL_COUNTER_CF_NAME,
    GLOBAL_COUNTER_KEY,
    LATEST_DELETED_VALUE_CF_NAME,
    LATEST_DELETED_VALUE_TIMESTAMP_KEY,
    LATEST_DELETED_WINDOW_CF_NAME,
    LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
    LATEST_TIMESTAMP_KEY,
    LATEST_TIMESTAMPS_CF_NAME,
    VALUES_CF_NAME,
)
from .serialization import append_integer, encode_integer_pair, parse_window_key
from .state import WindowedTransactionState

if TYPE_CHECKING:
    from .partition import WindowedRocksDBStorePartition


@dataclass
class TimestampsCache:
    key: bytes
    cf_name: str
    timestamps: dict[bytes, Optional[int]] = field(default_factory=dict)


@dataclass
class CounterCache:
    key: bytes
    cf_name: str
    counter: Optional[int] = None


class WindowedRocksDBPartitionTransaction(PartitionTransaction):
    def __init__(
        self,
        partition: "WindowedRocksDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition: WindowedRocksDBStorePartition = cast(
            "WindowedRocksDBStorePartition", self._partition
        )
        # Cache the metadata separately to avoid serdes on each access
        # (we are 100% sure that the underlying types are immutable, while windows'
        # values are not)
        self._latest_timestamps: TimestampsCache = TimestampsCache(
            key=LATEST_TIMESTAMP_KEY,
            cf_name=LATEST_TIMESTAMPS_CF_NAME,
        )
        self._last_expired_timestamps: TimestampsCache = TimestampsCache(
            key=LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
            cf_name=LATEST_EXPIRED_WINDOW_CF_NAME,
        )
        self._last_deleted_window_timestamps: TimestampsCache = TimestampsCache(
            key=LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
            cf_name=LATEST_DELETED_WINDOW_CF_NAME,
        )
        self._last_deleted_value_timestamps: TimestampsCache = TimestampsCache(
            key=LATEST_DELETED_VALUE_TIMESTAMP_KEY,
            cf_name=LATEST_DELETED_VALUE_CF_NAME,
        )
        self._global_counter: CounterCache = CounterCache(
            key=GLOBAL_COUNTER_KEY,
            cf_name=GLOBAL_COUNTER_CF_NAME,
        )

    def as_state(self, prefix: Any = DEFAULT_PREFIX) -> WindowedTransactionState:  # type: ignore [override]
        return WindowedTransactionState(
            transaction=self,
            prefix=(
                prefix
                if isinstance(prefix, bytes)
                else serialize(prefix, dumps=self._dumps)
            ),
        )

    def get_latest_timestamp(self, prefix: bytes) -> Optional[int]:
        return self._get_timestamp(
            prefix=prefix, cache=self._latest_timestamps, default=0
        )

    def get_window(
        self,
        start_ms: int,
        end_ms: int,
        prefix: bytes,
        default: Any = None,
    ) -> Any:
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)
        key = encode_integer_pair(start_ms, end_ms)
        return self.get(key=key, default=default, prefix=prefix)

    def update_window(
        self,
        start_ms: int,
        end_ms: int,
        value: Any,
        timestamp_ms: int,
        prefix: bytes,
    ) -> None:
        if timestamp_ms < 0:
            raise ValueError("Timestamp cannot be negative")
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)

        key = encode_integer_pair(start_ms, end_ms)
        self.set(key=key, value=value, prefix=prefix)
        latest_timestamp_ms = self.get_latest_timestamp(prefix=prefix)
        updated_timestamp_ms = (
            max(latest_timestamp_ms, timestamp_ms)
            if latest_timestamp_ms is not None
            else timestamp_ms
        )

        self._set_timestamp(
            cache=self._latest_timestamps,
            prefix=prefix,
            timestamp_ms=updated_timestamp_ms,
        )

    def add_to_collection(
        self,
        timestamp_ms: int,
        value: Any,
        prefix: bytes,
    ) -> None:
        key = encode_integer_pair(timestamp_ms, self._get_next_count())
        self.set(key=key, value=value, prefix=prefix, cf_name=VALUES_CF_NAME)

    def delete_window(self, start_ms: int, end_ms: int, prefix: bytes):
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)
        key = encode_integer_pair(start_ms, end_ms)
        self.delete(key=key, prefix=prefix)

    def expire_windows(
        self,
        max_start_time: int,
        prefix: bytes,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all expired windows from RocksDB up to the specified `max_start_time` timestamp.

        This method marks the latest found window as expired in the expiration index,
        so consecutive calls may yield different results for the same "latest timestamp".

        How it works:
        - First, it checks the expiration cache for the start time of the last expired
          window for the current prefix. If found, this value helps reduce the search
          space and prevents returning previously expired windows.
        - Next, it iterates over window segments and identifies the windows that should
          be marked as expired.
        - Finally, it updates the expiration cache with the start time of the latest
          windows found.

        Collection behavior (when collect=True):
        - For tumbling and hopping windows (created using .collect()), the window
          value is None and is replaced with the list of collected values.
        - For sliding windows, the window value is [max_timestamp, None] where
          None is replaced with the list of collected values.
        - Values are collected from a separate column family and obsolete values
          are deleted if delete=True.

        :param max_start_time: The timestamp up to which windows are considered expired, inclusive.
        :param prefix: The key prefix for filtering windows.
        :param delete: If True, expired windows will be deleted.
        :param collect: If True, values will be collected into windows.
        :param end_inclusive: If True, the end of the window will be inclusive.
            Relevant only together with `collect=True`.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        start_from = -1

        # Find the latest start timestamp of the expired windows for the given key
        last_expired = self._get_timestamp(
            cache=self._last_expired_timestamps, prefix=prefix
        )
        if last_expired is not None:
            start_from = max(start_from, last_expired)

        # Use the latest expired timestamp to limit the iteration over
        # only those windows that have not been expired before
        expired_windows = self.get_windows(
            start_from_ms=start_from,
            start_to_ms=max_start_time,
            prefix=prefix,
        )
        if not expired_windows:
            return []

        # Save the start of the latest expired window to the expiration index
        latest_window = expired_windows[-1]
        last_expired__gt = latest_window[0][0]

        self._set_timestamp(
            cache=self._last_expired_timestamps,
            prefix=prefix,
            timestamp_ms=last_expired__gt,
        )

        # Collect values into windows
        if collect:
            collected_expired_windows = []
            for (start, end), value in expired_windows:
                collection = self._get_values(
                    start=start,
                    # Sliding windows are inclusive on both ends
                    # (including timestamps of messages equal to `end`).
                    # Since RocksDB range queries are exclusive on the
                    # `end` boundary, we add +1 to include it.
                    end=end + 1 if end_inclusive else end,
                    prefix=prefix,
                )
                if value is None:
                    value = collection
                else:
                    # Sliding windows are timestamped:
                    # value is [max_timestamp, value] where max_timestamp
                    # is the timestamp of the latest message in the window
                    value[1] = collection
                collected_expired_windows.append(((start, end), value))
            expired_windows = collected_expired_windows

        # Delete expired windows from the state
        if delete:
            for (start, end), _ in expired_windows:
                self.delete_window(start, end, prefix=prefix)
            if collect:
                self._delete_values(max_timestamp=start, prefix=prefix)

        return expired_windows

    def delete_windows(
        self, max_start_time: int, delete_values: bool, prefix: bytes
    ) -> None:
        """
        Delete windows from RocksDB up to the specified `max_start_time` timestamp.

        This method removes all window entries that have a start time less than or equal to the given
        `max_start_time`. It ensures that expired data is cleaned up efficiently without affecting
        unexpired windows.

        How it works:
        - It retrieves the start time of the last deleted window for the given prefix from the
        deletion index. This minimizes redundant scans over already deleted windows.
        - It iterates over the windows starting from the last deleted timestamp up to the `max_start_time`.
        - Each window within this range is deleted from the database.
        - After deletion, it updates the deletion index with the start time of the latest window
        that was deleted to keep track of progress.
        - Values with timestamps less than max_start_time are considered obsolete and are
        deleted if delete_values=True, as they can no longer belong to any active window.

        :param max_start_time: The timestamp up to which windows should be deleted, inclusive.
        :param delete_values: If True, obsolete values will be deleted.
        :param prefix: The key prefix used to identify and filter relevant windows.
        """
        start_from = -1

        # Find the latest start timestamp of the deleted windows for the given key
        last_deleted = self._get_timestamp(
            cache=self._last_deleted_window_timestamps, prefix=prefix
        )
        if last_deleted is not None:
            start_from = max(start_from, last_deleted)

        windows = self.get_windows(
            start_from_ms=start_from,
            start_to_ms=max_start_time,
            prefix=prefix,
        )

        last_deleted__gt = None
        for (start, end), _ in windows:
            last_deleted__gt = start
            self.delete_window(start, end, prefix=prefix)

        # Save the start of the latest deleted window to the deletion index
        if last_deleted__gt:
            self._set_timestamp(
                cache=self._last_deleted_window_timestamps,
                prefix=prefix,
                timestamp_ms=last_deleted__gt,
            )

        if delete_values:
            self._delete_values(max_timestamp=max_start_time, prefix=prefix)

    def _delete_values(self, max_timestamp: int, prefix: bytes) -> None:
        """
        Delete collected values with timestamps less than max_timestamp.

        This method maintains a deletion index to track progress and avoid
        re-scanning previously deleted values. It:
        1. Retrieves the last deleted timestamp from the cache
        2. Scans values from last deleted timestamp up to max_timestamp
        3. Updates the deletion index with the latest deleted timestamp

        :param max_timestamp: Delete values with timestamps less than this value
        :param prefix: Key prefix for filtering values to delete
        """
        start = (
            self._get_timestamp(
                cache=self._last_deleted_value_timestamps, prefix=prefix
            )
            or -1
        )

        last_deleted_timestamp = None
        for key, _ in self._get_items(
            start=start, end=max_timestamp, prefix=prefix, cf_name=VALUES_CF_NAME
        ):
            _, timestamp_ms, count = parse_window_key(key)
            last_deleted_timestamp = max(last_deleted_timestamp or 0, timestamp_ms)
            key = encode_integer_pair(timestamp_ms, count)
            self.delete(key=key, prefix=prefix, cf_name=VALUES_CF_NAME)

        if last_deleted_timestamp is not None:
            self._set_timestamp(
                cache=self._last_deleted_value_timestamps,
                prefix=prefix,
                timestamp_ms=last_deleted_timestamp,
            )

    def get_windows(
        self,
        start_from_ms: int,
        start_to_ms: int,
        prefix: bytes,
        backwards: bool = False,
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all windows within the specified time range.

        This method retrieves all window entries that have a start time between
        `start_from_ms` (exclusive) and `start_to_ms` (inclusive). The windows can be
        retrieved in either forward or reverse chronological order.

        How it works:
        - It uses `_get_items` to fetch the raw key-value pairs within
          the specified time range.
        - For each window, it parses the key to extract start and end timestamps.
        - Values are deserialized before being returned.
        - Results are returned as tuples of ((start_time, end_time), value).

        :param start_from_ms: The lower bound timestamp (exclusive) for window start times.
        :param start_to_ms: The upper bound timestamp (inclusive) for window start times.
        :param prefix: The key prefix used to identify and filter relevant windows.
        :param backwards: If True, returns windows in reverse chronological order.
        :return: A list of tuples in the format ((start_ms, end_ms), value).
        """
        result = []
        for key, value in self._get_items(
            start=start_from_ms,
            end=start_to_ms + 1,  # add +1 to make the upper bound inclusive
            prefix=prefix,
            backwards=backwards,
        ):
            _, start, end = parse_window_key(key)
            if start_from_ms < start <= start_to_ms:
                value = self._deserialize_value(value)
                result.append(((start, end), value))

        return result

    def _get_values(self, start: int, end: int, prefix: bytes) -> list[Any]:
        items = self._get_items(
            start=start, end=end, prefix=prefix, cf_name=VALUES_CF_NAME
        )
        return [self._deserialize_value(value) for _, value in items]

    def _get_items(
        self,
        start: int,
        end: int,
        prefix: bytes,
        backwards: bool = False,
        cf_name: str = "default",
    ) -> list[tuple[bytes, bytes]]:
        """
        Get all items that start between `start` and `end`
        within the specified prefix.

        This function also checks the update cache for any updates not yet
        committed to RocksDB.

        :param start: Start of the range, inclusive.
        :param end: End of the range, exclusive.
        :param prefix: The key prefix for filtering items.
        :param backwards: If True, returns items in reverse order.
        :param cf_name: The RocksDB column family name.
        :return: A sorted list of key-value pairs.
        """
        start = max(start, 0)
        if start > end:
            return []

        seek_from_key = append_integer(base_bytes=prefix, integer=start)
        seek_to_key = append_integer(base_bytes=prefix, integer=end)

        # Create an iterator over the state store
        # Set iterator bounds to reduce IO by limiting the range of keys fetched
        read_opt = ReadOptions()
        read_opt.set_iterate_lower_bound(seek_from_key)
        read_opt.set_iterate_upper_bound(seek_to_key)
        db_items = self._partition.iter_items(
            read_opt=read_opt, from_key=seek_from_key, cf_name=cf_name
        )

        cache = self._update_cache
        update_cache = cache.get_updates(cf_name=cf_name).get(prefix, {})
        delete_cache = cache.get_deletes(cf_name=cf_name)

        # Get cached updates with matching keys
        updated_items = (
            (key, value)
            for key, value in update_cache.items()
            if seek_from_key < key <= seek_to_key
        )

        # Iterate over stored and cached items and merge them to a single dict
        merged_items = {}
        for key, value in itertools.chain(db_items, updated_items):
            if key not in delete_cache:
                merged_items[key] = value

        # Sort and deserialize items merged from the cache and store
        return sorted(merged_items.items(), key=lambda kv: kv[0], reverse=backwards)

    def _get_timestamp(
        self, cache: TimestampsCache, prefix: bytes, default: Any = None
    ) -> Optional[int]:
        cached_ts = cache.timestamps.get(prefix)
        if cached_ts is not None:
            return cached_ts

        stored_ts = self.get(
            key=cache.key,
            prefix=prefix,
            cf_name=cache.cf_name,
            default=default,
        )
        if stored_ts is not None and not isinstance(stored_ts, int):
            raise ValueError(f"invalid timestamp {stored_ts}")

        cache.timestamps[prefix] = stored_ts
        return stored_ts

    def _set_timestamp(self, cache: TimestampsCache, prefix: bytes, timestamp_ms: int):
        cache.timestamps[prefix] = timestamp_ms
        self.set(
            key=cache.key,
            value=timestamp_ms,
            prefix=prefix,
            cf_name=cache.cf_name,
        )

    def _validate_duration(self, start_ms: int, end_ms: int):
        if end_ms <= start_ms:
            raise ValueError(
                f"Invalid window duration: window end {end_ms} is smaller or equal "
                f"than window start {start_ms}"
            )

    def _serialize_key(self, key: Any, prefix: bytes) -> bytes:
        # Allow bytes keys in WindowedStore
        key_bytes = key if isinstance(key, bytes) else serialize(key, dumps=self._dumps)
        return prefix + SEPARATOR + key_bytes

    def _get_next_count(self) -> int:
        """
        Get the next unique global counter value.

        This method maintains a global counter in RocksDB to ensure unique
        identifiers for values collected within the same timestamp. The counter
        is cached to reduce database reads.

        :return: Next sequential counter value
        """
        cache = self._global_counter
        kwargs = {"key": cache.key, "prefix": b"", "cf_name": cache.cf_name}

        if cache.counter is None:
            cache.counter = self.get(default=-1, **kwargs)

        cache.counter += 1

        self.set(value=cache.counter, **kwargs)
        return cache.counter
