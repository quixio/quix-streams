import itertools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, cast

from rocksdict import ReadOptions

from quixstreams.state.base.transaction import PartitionTransaction
from quixstreams.state.metadata import DEFAULT_PREFIX, PREFIX_SEPARATOR
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc, serialize

from .metadata import (
    LATEST_DELETED_WINDOW_CF_NAME,
    LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
    LATEST_TIMESTAMP_KEY,
    LATEST_TIMESTAMPS_CF_NAME,
)
from .serialization import encode_window_key, encode_window_prefix, parse_window_key
from .state import WindowedTransactionState

if TYPE_CHECKING:
    from .partition import WindowedRocksDBStorePartition


@dataclass
class TimestampsCache:
    key: bytes
    cf_name: str
    timestamps: dict[bytes, int] = field(default_factory=dict)


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
        self._partition = cast("WindowedRocksDBStorePartition", self._partition)
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
        self._last_deleted_timestamps: TimestampsCache = TimestampsCache(
            key=LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
            cf_name=LATEST_DELETED_WINDOW_CF_NAME,
        )

    def as_state(self, prefix: Any = DEFAULT_PREFIX) -> WindowedTransactionState:
        return WindowedTransactionState(
            transaction=self,
            prefix=(
                prefix
                if isinstance(prefix, bytes)
                else serialize(prefix, dumps=self._dumps)
            ),
        )

    def get_latest_timestamp(self, prefix: bytes) -> int:
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
        key = encode_window_key(start_ms, end_ms)
        return self.get(key=key, default=default, prefix=prefix)

    def update_window(
        self,
        start_ms: int,
        end_ms: int,
        value: Any,
        timestamp_ms: int,
        prefix: bytes,
        window_timestamp_ms: Optional[int] = None,
    ) -> None:
        if timestamp_ms < 0:
            raise ValueError("Timestamp cannot be negative")
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)

        key = encode_window_key(start_ms, end_ms)
        if window_timestamp_ms is not None:
            value = [window_timestamp_ms, value]
        self.set(key=key, value=value, prefix=prefix)
        latest_timestamp_ms = self.get_latest_timestamp(prefix=prefix)
        self._set_timestamp(
            cache=self._latest_timestamps,
            prefix=prefix,
            timestamp_ms=max(latest_timestamp_ms, timestamp_ms),
        )

    def delete_window(self, start_ms: int, end_ms: int, prefix: bytes):
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)
        key = encode_window_key(start_ms, end_ms)
        self.delete(key=key, prefix=prefix)

    def expire_windows(
        self, max_start_time: int, prefix: bytes, delete: bool = True
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

        :param max_start_time: The timestamp up to which windows are considered expired, inclusive.
        :param prefix: The key prefix for filtering windows.
        :param delete: If True, expired windows will be deleted.
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
        expired_windows = list(
            self.get_windows(
                start_from_ms=start_from,
                start_to_ms=max_start_time,
                prefix=prefix,
            )
        )
        if expired_windows:
            # Save the start of the latest expired window to the expiration index
            latest_window = expired_windows[-1]
            last_expired__gt = latest_window[0][0]

            self._set_timestamp(
                cache=self._last_expired_timestamps,
                prefix=prefix,
                timestamp_ms=last_expired__gt,
            )
            # Delete expired windows from the state
            if delete:
                for (start, end), _ in expired_windows:
                    self.delete_window(start, end, prefix=prefix)
        return expired_windows

    def delete_windows(self, max_start_time: int, prefix: bytes) -> None:
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

        :param max_start_time: The timestamp up to which windows should be deleted, inclusive.
        :param prefix: The key prefix used to identify and filter relevant windows.
        """
        start_from = -1

        # Find the latest start timestamp of the deleted windows for the given key
        last_deleted = self._get_timestamp(
            cache=self._last_deleted_timestamps, prefix=prefix
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
                cache=self._last_deleted_timestamps,
                prefix=prefix,
                timestamp_ms=last_deleted__gt,
            )

    def get_windows(
        self,
        start_from_ms: int,
        start_to_ms: int,
        prefix: bytes,
        backwards: bool = False,
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all windows that start between "start_from_ms" and "start_to_ms"
        within the specified prefix.

        This function also checks the update cache for any updates not yet
        committed to RocksDB.

        :param start_from_ms: The minimal window start time, exclusive.
        :param start_to_ms: The maximum window start time, inclusive.
        :param prefix: The key prefix for filtering windows.
        :param backwards: If True, yields windows in reverse order.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        seek_from = max(start_from_ms, 0)
        seek_from_key = encode_window_prefix(prefix=prefix, start_ms=seek_from)

        # Add +1 to make the upper bound inclusive
        seek_to = start_to_ms + 1
        seek_to_key = encode_window_prefix(prefix=prefix, start_ms=seek_to)

        # Create an iterator over the state store
        # Set iterator bounds to reduce IO by limiting the range of keys fetched
        read_opt = ReadOptions()
        read_opt.set_iterate_lower_bound(seek_from_key)
        read_opt.set_iterate_upper_bound(seek_to_key)
        db_windows = self._partition.iter_items(
            read_opt=read_opt, from_key=seek_from_key
        )

        # Get cached updates with matching keys
        update_cache = self._update_cache
        updated_windows = (
            (k, v)
            for k, v in update_cache.get_updates(cf_name="default")
            .get(prefix, {})
            .items()
            if seek_from_key < k <= seek_to_key
        )

        # Iterate over stored and cached windows and merge them to a single dict
        deleted_windows = update_cache.get_deletes(cf_name="default")
        merged_windows = {}
        for key, value in itertools.chain(db_windows, updated_windows):
            if key not in deleted_windows:
                merged_windows[key] = value

        # Sort and deserialize windows merged from the cache and store
        result = []
        for window_key, window_value in sorted(
            merged_windows.items(), key=lambda kv: kv[0], reverse=backwards
        ):
            _, start, end = parse_window_key(window_key)
            if start_from_ms < start <= start_to_ms:
                value = self._deserialize_value(window_value)
                result.append(((start, end), value))

        return result

    def _get_timestamp(
        self, cache: TimestampsCache, prefix: bytes, default: Any = None
    ) -> int:
        cached_ts = cache.timestamps.get(prefix)
        if cached_ts is not None:
            return cached_ts

        stored_ts = self.get(
            key=cache.key,
            prefix=prefix,
            cf_name=cache.cf_name,
            default=default,
        )
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
        return prefix + PREFIX_SEPARATOR + key_bytes
