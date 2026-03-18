import heapq
from typing import TYPE_CHECKING, Any, Iterable, Optional, cast

from quixstreams.state.base.transaction import (
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.metadata import DEFAULT_PREFIX
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.cache import Cache
from quixstreams.state.rocksdb.transaction import RocksDBPartitionTransaction
from quixstreams.state.serialization import (
    DumpsFunc,
    LoadsFunc,
    encode_integer_pair,
    int_to_bytes,
    serialize,
)
from quixstreams.state.types import ExpiredWindowDetail, WindowDetail

from .metadata import (
    LATEST_DELETED_VALUE_CF_NAME,
    LATEST_DELETED_VALUE_TIMESTAMP_KEY,
    LATEST_DELETED_WINDOW_CF_NAME,
    LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
    VALUES_CF_NAME,
)
from .serialization import parse_window_key
from .state import WindowedTransactionState

if TYPE_CHECKING:
    from .partition import WindowedRocksDBStorePartition


class WindowedRocksDBPartitionTransaction(RocksDBPartitionTransaction):
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
        self._last_expired_timestamps: Cache = Cache(
            key=LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
            cf_name=LATEST_EXPIRED_WINDOW_CF_NAME,
        )
        self._last_deleted_window_timestamps: Cache = Cache(
            key=LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
            cf_name=LATEST_DELETED_WINDOW_CF_NAME,
        )
        self._last_deleted_value_timestamps: Cache = Cache(
            key=LATEST_DELETED_VALUE_TIMESTAMP_KEY,
            cf_name=LATEST_DELETED_VALUE_CF_NAME,
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

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def keys(self, cf_name: str = "default") -> Iterable[Any]:
        """
        Return all keys in the store partition for the given column family.
        It merges data from the transaction update cache and DB,
        and returns keys in a sorted way.

        :param cf_name: column family name.
        """
        delete_cache_keys: set[bytes] = self._update_cache.get_deletes()
        update_cache_keys: set[bytes] = set()

        for prefix_update_cache in self._update_cache.get_updates(
            cf_name=cf_name
        ).values():
            # when iterating over the DB, skip keys already returned by the cache
            update_cache_keys.update(prefix_update_cache.keys())

        # Get the keys stored in the DB excluding the keys updated/deleted
        # in the current transaction
        db_skip_keys = delete_cache_keys | update_cache_keys
        stored_keys = (
            key
            for key in self._partition.iter_keys(cf_name=cf_name)
            if key not in db_skip_keys
        )

        # Sort the keys updated in the cache to iterate over both generators
        # in the sorted way
        update_cache_keys_sorted = sorted(update_cache_keys)
        for key in heapq.merge(stored_keys, update_cache_keys_sorted):
            yield key

    def get_latest_expired(self, prefix: bytes) -> int:
        return (
            self._get_timestamp(prefix=prefix, cache=self._last_expired_timestamps) or 0
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

    def add_to_collection(
        self,
        id: Optional[int],
        value: Any,
        prefix: bytes,
    ) -> int:
        counter = self._increment_counter()
        if id is None:
            key = encode_integer_pair(counter, counter)
        else:
            key = encode_integer_pair(id, counter)

        self.set(key=key, value=value, prefix=prefix, cf_name=VALUES_CF_NAME)
        return counter

    def get_from_collection(self, start: int, end: int, prefix: bytes) -> list[Any]:
        items = self._get_items(
            start=start, end=end, prefix=prefix, cf_name=VALUES_CF_NAME
        )
        return [self._deserialize_value(value) for _, value in items]

    def delete_from_collection(
        self, end: int, prefix: bytes, *, start: Optional[int] = None
    ) -> None:
        if start is None:
            start = (
                self._get_timestamp(
                    cache=self._last_deleted_value_timestamps, prefix=prefix
                )
                or -1
            )

        last_deleted_id = None
        for key, _ in self._get_items(
            start=start, end=end, prefix=prefix, cf_name=VALUES_CF_NAME
        ):
            _, id, count = parse_window_key(key)
            last_deleted_id = max(last_deleted_id or 0, id)
            key = encode_integer_pair(id, count)
            self.delete(key=key, prefix=prefix, cf_name=VALUES_CF_NAME)

        if last_deleted_id is not None:
            self._set_timestamp(
                cache=self._last_deleted_value_timestamps,
                prefix=prefix,
                timestamp_ms=last_deleted_id,
            )

    def delete_window(self, start_ms: int, end_ms: int, prefix: bytes):
        self._validate_duration(start_ms=start_ms, end_ms=end_ms)
        key = encode_integer_pair(start_ms, end_ms)
        self.delete(key=key, prefix=prefix)

    def expire_all_windows(
        self,
        max_end_time: int,
        step_ms: int = 1,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> Iterable[ExpiredWindowDetail]:
        """
        Get all expired windows for all prefix from RocksDB up to the specified `max_end_time` timestamp.

        :param max_end_time: The timestamp up to which windows are considered expired, inclusive.
        :param step_ms: step between the windows is known.
            For example, tumbling windows of size 100ms have 100ms step between them.
            This value is used to optimize the DB lookups.
            Default - 1ms.
        :param delete: If True, expired windows will be deleted.
        :param collect: If True, values will be collected into windows.
        :param end_inclusive: If True, the end of the window will be inclusive.
            Relevant only together with `collect=True`.
        """

        max_end_time = max(max_end_time, 0)
        last_expired = self.get_latest_expired(prefix=b"")

        to_delete: set[tuple[bytes, int, int]] = set()
        collected = []
        if last_expired:
            # TODO: Probably optimize that. It works only for tumbling/hopping windows
            # with fixed boundaries
            windows = windows_to_expire(last_expired, max_end_time, step_ms)
            if not windows:
                return
            last_expired = windows[-1]  # windows are ordered
            suffixes: set[bytes] = set(int_to_bytes(window) for window in windows)
            for key in self.keys():
                if key[-8:] in suffixes:
                    prefix, start, end = parse_window_key(key)
                    to_delete.add((prefix, start, end))
                    aggregated = self.get(
                        encode_integer_pair(start, end), prefix=prefix
                    )
                    if collect:
                        collected = self.get_from_collection(
                            start=start,
                            # Sliding windows are inclusive on both ends
                            # (including timestamps of messages equal to `end`).
                            # Since RocksDB range queries are exclusive on the
                            # `end` boundary, we add +1 to include it.
                            end=end + 1 if end_inclusive else end,
                            prefix=prefix,
                        )
                    yield (start, end), aggregated, collected, prefix

        else:
            # If we don't have a saved last_expired value it means one of two cases
            # 1. It's a new window, iterating over all the keys is fast.
            # 2. The expiration strategy changed from key to partition. We need to expire all
            #    the old per-key windows.
            last_expired = max(windows_to_expire(last_expired, max_end_time, step_ms))
            for key in self.keys():
                prefix, start, end = parse_window_key(key)
                if end <= last_expired:
                    to_delete.add((prefix, start, end))
                    aggregated = self.get(
                        encode_integer_pair(start, end), prefix=prefix
                    )
                    if collect:
                        collected = self.get_from_collection(
                            start=start,
                            # Sliding windows are inclusive on both ends
                            # (including timestamps of messages equal to `end`).
                            # Since RocksDB range queries are exclusive on the
                            # `end` boundary, we add +1 to include it.
                            end=end + 1 if end_inclusive else end,
                            prefix=prefix,
                        )

                    yield (start, end), aggregated, collected, prefix

        if delete:
            for prefix, start, end in to_delete:
                self.delete_window(start, end, prefix)
                if collect:
                    self.delete_from_collection(end=start, prefix=prefix)

        self._set_timestamp(
            prefix=b"", cache=self._last_expired_timestamps, timestamp_ms=last_expired
        )

    def delete_all_windows(self, max_end_time: int, collect: bool) -> None:
        """
        Delete windows from RocksDB up to the specified `max_start_time` timestamp.

        :param max_end_time: The timestamp up to which windows should be deleted, inclusive.
        :param collect: If True, the values from collections will be deleted too.
        """
        max_end_time = max(max_end_time, 0)
        for key in self.keys():
            prefix, start, end = parse_window_key(key)
            if end <= max_end_time:
                self.delete_window(start, end, prefix)
                if collect:
                    self.delete_from_collection(end=start, prefix=prefix)

    def get_windows(
        self,
        start_from_ms: int,
        start_to_ms: int,
        prefix: bytes,
        backwards: bool = False,
    ) -> list[WindowDetail]:
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
                result.append(((start, end), self._deserialize_value(value), prefix))

        return result

    def _get_timestamp(self, cache: Cache, prefix: bytes) -> Optional[int]:
        if prefix in cache.values:
            # Return the cached value if it has been set at least once
            return cache.values[prefix]

        stored_ts = self.get(
            key=cache.key,
            prefix=prefix,
            cf_name=cache.cf_name,
        )
        if stored_ts is not None and not isinstance(stored_ts, int):
            raise ValueError(f"invalid timestamp {stored_ts}")

        cache.values[prefix] = stored_ts
        return stored_ts

    def _set_timestamp(self, cache: Cache, prefix: bytes, timestamp_ms: int):
        cache.values[prefix] = timestamp_ms
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


def windows_to_expire(
    last_expired: int,
    timestamp_ms: int,
    step_ms: int,
) -> list[int]:
    if not last_expired:
        return [timestamp_ms - (timestamp_ms % step_ms)]

    next_to_expire = last_expired + step_ms
    if next_to_expire > timestamp_ms:
        return []

    window: list[int] = []
    while next_to_expire <= timestamp_ms:
        window.append(next_to_expire)
        next_to_expire += step_ms

    return window
