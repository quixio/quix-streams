import itertools
from typing import Any, Optional, TYPE_CHECKING, cast

from rocksdict import ReadOptions

from quixstreams.state.metadata import PREFIX_SEPARATOR, DEFAULT_PREFIX
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import (
    serialize,
    LoadsFunc,
    DumpsFunc,
)
from quixstreams.state.base.transaction import PartitionTransaction
from quixstreams.state.exceptions import InvalidChangelogOffset

from .metadata import LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY, LATEST_EXPIRED_WINDOW_CF_NAME
from .serialization import encode_window_key, encode_window_prefix, parse_window_key
from .state import WindowedTransactionState

if TYPE_CHECKING:
    from .partition import WindowedRocksDBStorePartition


class WindowedRocksDBPartitionTransaction(PartitionTransaction):
    __slots__ = ("_latest_timestamp_ms",)

    def __init__(
        self,
        partition: "WindowedRocksDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        latest_timestamp_ms: int,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition = cast("WindowedRocksDBStorePartition", self._partition)
        self._latest_timestamp_ms = latest_timestamp_ms

    def as_state(self, prefix: Any = DEFAULT_PREFIX) -> WindowedTransactionState:
        return WindowedTransactionState(
            transaction=self,
            prefix=(
                prefix
                if isinstance(prefix, bytes)
                else serialize(prefix, dumps=self._dumps)
            ),
        )

    def get_latest_timestamp(self) -> int:
        return self._latest_timestamp_ms

    def _validate_duration(self, start_ms: int, end_ms: int):
        if end_ms <= start_ms:
            raise ValueError(
                f"Invalid window duration: window end {end_ms} is smaller or equal "
                f"than window start {start_ms}"
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
        self, start_ms: int, end_ms: int, value: Any, timestamp_ms: int, prefix: bytes
    ):
        if timestamp_ms < 0:
            raise ValueError("Timestamp cannot be negative")
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)

        key = encode_window_key(start_ms, end_ms)
        self.set(key=key, value=value, prefix=prefix)
        self._latest_timestamp_ms = max(self._latest_timestamp_ms, timestamp_ms)

    def delete_window(self, start_ms: int, end_ms: int, prefix: bytes):
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)
        key = encode_window_key(start_ms, end_ms)
        self.delete(key=key, prefix=prefix)

    def _flush(self, processed_offset: Optional[int], changelog_offset: Optional[int]):
        if self._update_cache.is_empty():
            return

        if changelog_offset is not None:
            current_changelog_offset = self._partition.get_changelog_offset()
            if (
                current_changelog_offset is not None
                and changelog_offset < current_changelog_offset
            ):
                raise InvalidChangelogOffset(
                    "Cannot set changelog offset lower than already saved one"
                )

        self._partition.write(
            cache=self._update_cache,
            processed_offset=processed_offset,
            changelog_offset=changelog_offset,
            latest_timestamp_ms=self._latest_timestamp_ms,
        )

    def expire_windows(
        self, duration_ms: int, prefix: bytes, grace_ms: int = 0
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all expired windows from RocksDB based on the latest timestamp,
        window duration, and an optional grace period.

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

        :param duration_ms: The duration of each window in milliseconds.
        :param prefix: The key prefix for filtering windows.
        :param grace_ms: An optional grace period in milliseconds to delay expiration.
            Defaults to 0, meaning no grace period is applied.
        :return: A generator that yields sorted tuples in the format `((start, end), value)`.
        """
        latest_timestamp = self._latest_timestamp_ms
        start_to = latest_timestamp - duration_ms - grace_ms
        start_from = -1

        # Find the latest start timestamp of the expired windows for the given key
        last_expired = self.get(
            key=LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
            prefix=prefix,
            cf_name=LATEST_EXPIRED_WINDOW_CF_NAME,
        )
        if last_expired is not None:
            start_from = max(start_from, last_expired)

        # Use the latest expired timestamp to limit the iteration over
        # only those windows that have not been expired before
        expired_windows = list(
            self.get_windows(
                start_from_ms=start_from,
                start_to_ms=start_to,
                prefix=prefix,
            )
        )
        if expired_windows:
            # Save the start of the latest expired window to the expiration index
            latest_window = expired_windows[-1]
            last_expired__gt = latest_window[0][0]
            self.set(
                key=LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
                value=last_expired__gt,
                prefix=prefix,
                cf_name=LATEST_EXPIRED_WINDOW_CF_NAME,
            )
            # Delete expired windows from the state
            for (start, end), _ in expired_windows:
                self.delete_window(start, end, prefix=prefix)
        return expired_windows

    def _serialize_key(self, key: Any, prefix: bytes) -> bytes:
        # Allow bytes keys in WindowedStore
        key_bytes = key if isinstance(key, bytes) else serialize(key, dumps=self._dumps)
        return prefix + PREFIX_SEPARATOR + key_bytes

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
