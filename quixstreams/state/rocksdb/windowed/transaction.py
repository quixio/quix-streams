from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, cast

from quixstreams.state.base.transaction import (
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.metadata import DEFAULT_PREFIX, SEPARATOR, SEPARATOR_LENGTH
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb.cache import Cache
from quixstreams.state.rocksdb.transaction import (
    MAX_UINT64,
    RocksDBPartitionTransaction,
)
from quixstreams.state.serialization import (
    DumpsFunc,
    LoadsFunc,
    append_integer,
    deserialize,
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
    LATEST_TIMESTAMP_KEY,
    LATEST_TIMESTAMPS_CF_NAME,
    VALUES_CF_NAME,
)
from .serialization import parse_window_key
from .state import WindowedTransactionState

if TYPE_CHECKING:
    from .partition import WindowedRocksDBStorePartition

# A window key is `<prefix>|<start>|<end>`, where `start` and `end` are 8-byte
# big-endian integers joined by the separator.
_TIMESTAMPS_SEGMENT = encode_integer_pair(MAX_UINT64, MAX_UINT64)
_TIMESTAMPS_SEGMENT_LEN = len(_TIMESTAMPS_SEGMENT)
_WINDOW_KEY_SUFFIX_LEN = SEPARATOR_LENGTH + _TIMESTAMPS_SEGMENT_LEN
# `prefix + _SEPARATOR_SUCCESSOR` is the smallest byte string strictly greater
# than every key starting with `prefix + SEPARATOR`. That range is a superset of
# `prefix`'s window keys: when one message key is a SEPARATOR-extension of
# another (`b"user"` and `b"user|123"`), the extension's window keys sort inside
# it, so consumers must also check the exact key length
# (`len(prefix) + _WINDOW_KEY_SUFFIX_LEN`) before attributing a key to `prefix`.
_SEPARATOR_SUCCESSOR = bytes([SEPARATOR[0] + 1])


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
        self._latest_timestamps: Cache = Cache(
            key=LATEST_TIMESTAMP_KEY,
            cf_name=LATEST_TIMESTAMPS_CF_NAME,
        )
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

    def key_from_prefix(self, prefix: bytes, message_key: Any) -> Any:
        """
        Reverse `as_state()`: map a store prefix back to the message key.

        :param prefix: a store prefix, e.g. as yielded by `expire_all_windows()`
        :param message_key: the key of the record being processed. Only its type
            is read: `as_state()` stores `bytes` keys verbatim and serializes
            every other key.
        :return: the message key the prefix was built from
        """
        if isinstance(message_key, bytes):
            return prefix
        return deserialize(prefix, loads=self._loads)

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def keys(self, cf_name: str = "default") -> Iterable[Any]:
        db_skip_keys: set[bytes] = set()

        cache = self._update_cache.get_updates(cf_name=cf_name)
        for prefix_update_cache in cache.values():
            # when iterating over the DB, skip keys already returned by the cache
            db_skip_keys.update(prefix_update_cache.keys())
            yield from prefix_update_cache.keys()

        # skip keys that were deleted from the cache
        db_skip_keys.update(self._update_cache.get_deletes())

        for key in self._partition.iter_keys(cf_name=cf_name):
            if key in db_skip_keys:
                continue
            yield key

    def get_latest_timestamp(self, prefix: bytes) -> int:
        return self._get_timestamp(prefix=prefix, cache=self._latest_timestamps) or 0

    def get_latest_expired(self, prefix: bytes) -> int:
        return (
            self._get_timestamp(prefix=prefix, cache=self._last_expired_timestamps) or 0
        )

    def get_partition_timestamp(self) -> int:
        """
        Get the maximum event timestamp observed across the whole partition.

        Stored in the "latest timestamps" cache under the empty prefix, which a
        message key serializing to empty bytes also shares. Returns 0 when nothing
        has been observed yet.
        """
        return self.get_latest_timestamp(prefix=b"")

    def advance_partition_timestamp(self, timestamp_ms: int) -> int:
        """
        Monotonically raise the partition-wide watermark and return its new value.

        :param timestamp_ms: the event timestamp of the message being processed.
        :return: the watermark after the update, i.e.
            `max(timestamp_ms, previous watermark)`.
        """
        current = self.get_latest_timestamp(prefix=b"")
        if timestamp_ms <= current:
            return current

        self._set_timestamp(
            cache=self._latest_timestamps,
            prefix=b"",
            timestamp_ms=timestamp_ms,
        )
        return timestamp_ms

    def get_expiry_checkpoint(self, prefix: bytes = b"") -> Optional[int]:
        """
        Get the expiry cursor stored for `prefix`, or `None` when unset.

        Two distinct meanings share this cache, keyed by prefix:
        - for a message-key prefix it is the **start** of the last expired window,
          exactly like the cursor `expire_windows` maintains;
        - for the empty prefix it is the partition-wide expiry checkpoint used by
          session windows: the earliest watermark value at which some session in
          the partition may close. A message key serializing to empty bytes shares
          that slot.

        :param prefix: The key prefix. Default - the partition-wide slot.
        """
        return self._get_timestamp(prefix=prefix, cache=self._last_expired_timestamps)

    def set_expiry_checkpoint(self, timestamp_ms: int, prefix: bytes = b"") -> None:
        """
        Persist the expiry cursor for `prefix`. See `get_expiry_checkpoint`.

        :param timestamp_ms: The cursor value to store.
        :param prefix: The key prefix. Default - the partition-wide slot.
        """
        self._set_timestamp(
            cache=self._last_expired_timestamps,
            prefix=prefix,
            timestamp_ms=timestamp_ms,
        )

    def iter_windows(
        self,
        prefix: bytes,
        start_from_ms: int = 0,
        start_to_ms: Optional[int] = None,
        backwards: bool = False,
    ) -> Iterator[WindowDetail]:
        """
        Lazily iterate over the windows of `prefix` ordered by window start.

        Unlike `get_windows()`, this method:
        - has an **inclusive** lower bound, so a window starting at 0 is returned
          for `start_from_ms=0`;
        - accepts `start_to_ms=None` for an unbounded upper bound;
        - is a generator that reaches its first element in `O(log n)` instead of
          materialising the whole range into a list.

        RocksDB orders window keys by `(prefix, start, end)`, so for one prefix the
        iteration order is window-start order.

        The uncommitted updates of this transaction are merged in, and callers may
        delete windows while consuming the iterator.

        :param prefix: The key prefix used to identify and filter relevant windows.
        :param start_from_ms: The minimal window start time, inclusive.
        :param start_to_ms: The maximum window start time, inclusive.
            `None` means unbounded.
        :param backwards: If True, yields windows from the greatest start down.
        :return: An iterator of `((start, end), value, prefix)` tuples.
        """
        start_from_ms = max(start_from_ms, 0)
        if start_to_ms is not None and start_to_ms < start_from_ms:
            return

        lower_bound = append_integer(base_bytes=prefix, integer=start_from_ms)
        if start_to_ms is None:
            upper_bound = prefix + _SEPARATOR_SUCCESSOR
        else:
            upper_bound = append_integer(
                base_bytes=prefix, integer=min(start_to_ms + 1, MAX_UINT64)
            )

        db_items = self._partition.iter_items(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            backwards=backwards,
        )

        # Snapshot the in-range cached keys before yielding anything: callers
        # delete windows while consuming this iterator.
        updates = self._update_cache.get_updates(cf_name="default")
        update_cache = updates.get(prefix, {})
        cached_items = sorted(
            (
                (key, value)
                for key, value in update_cache.items()
                if lower_bound <= key < upper_bound
            ),
            key=lambda item: item[0],
            reverse=backwards,
        )
        delete_cache = self._update_cache.get_deletes(cf_name="default")

        # A SEPARATOR-extended message key's window keys fall inside the same
        # byte range but are longer, so only the exact length attributes a key
        # to `prefix` (see `_SEPARATOR_SUCCESSOR`).
        window_key_len = len(prefix) + _WINDOW_KEY_SUFFIX_LEN

        for key, value in _merge_sorted(iter(cached_items), db_items, backwards):
            if key in delete_cache or len(key) != window_key_len:
                continue
            _, start, end = parse_window_key(key)
            yield ((start, end), self._deserialize_value(value), prefix)

    def iter_prefixes(self, cf_name: str = "default") -> Iterator[bytes]:
        """
        Yield each distinct message-key prefix present in the store, in key order.

        Window keys are streamed once and deduplicated into prefixes, without
        deserializing any value. Prefixes that exist only in this transaction's
        uncommitted update cache are included.

        :param cf_name: rocksdb column family name. Default - "default"
        :return: An iterator of prefixes.
        """
        db_prefixes = self._iter_db_prefixes(cf_name=cf_name)
        # Snapshot the cached prefixes: expiring windows mutates the update cache.
        cached_prefixes = iter(
            sorted(self._update_cache.get_updates(cf_name=cf_name).keys())
        )

        db_next = next(db_prefixes, None)
        cached_next = next(cached_prefixes, None)
        while True:
            if db_next is None:
                if cached_next is None:
                    return
                yield cached_next
                cached_next = next(cached_prefixes, None)
            elif cached_next is None or db_next < cached_next:
                yield db_next
                db_next = next(db_prefixes, None)
            elif cached_next < db_next:
                yield cached_next
                cached_next = next(cached_prefixes, None)
            else:  # the same prefix is present in both the store and the cache
                yield db_next
                db_next = next(db_prefixes, None)
                cached_next = next(cached_prefixes, None)

    def _iter_db_prefixes(self, cf_name: str) -> Iterator[bytes]:
        """
        Yield the distinct message-key prefixes stored in RocksDB, each exactly
        once, in the order their first window key appears in the DB.

        The pass is linear over the window keys: no seek can step past one
        prefix's whole key range without risking another's, because a
        SEPARATOR-extended message key's window keys live *inside* the shorter
        key's byte range (see `_SEPARATOR_SUCCESSOR`).

        First-appearance order deviates from byte order only when an extended
        key's first extension byte is `0x00` or a window start exceeds 2**56 ms,
        in which case `iter_prefixes` may yield the same prefix twice.
        """
        seen: set[bytes] = set()
        for key, _ in self._partition.iter_items(lower_bound=b"", cf_name=cf_name):
            if len(key) <= _TIMESTAMPS_SEGMENT_LEN:
                # Too short to hold `<prefix>|<start>|<end>`.
                continue

            prefix, _, _ = parse_window_key(key)
            if prefix not in seen:
                seen.add(prefix)
                yield prefix

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

    def expire_windows(
        self,
        max_start_time: int,
        prefix: bytes,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> Iterable[ExpiredWindowDetail]:
        """
        Get all expired windows with a set prefix from RocksDB up to the specified `max_start_time` timestamp.

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
        windows = self.get_windows(
            start_from_ms=start_from,
            start_to_ms=max_start_time,
            prefix=prefix,
        )
        if not windows:
            return

        # Save the start of the latest expired window to the expiration index
        latest_window = windows[-1]
        last_expired__gt = latest_window[0][0]

        self._set_timestamp(
            cache=self._last_expired_timestamps,
            prefix=prefix,
            timestamp_ms=last_expired__gt,
        )

        # Collect values into windows
        if collect:
            for (start, end), aggregated, key in windows:
                collected = self.get_from_collection(
                    start=start,
                    # Sliding windows are inclusive on both ends
                    # (including timestamps of messages equal to `end`).
                    # Since RocksDB range queries are exclusive on the
                    # `end` boundary, we add +1 to include it.
                    end=end + 1 if end_inclusive else end,
                    prefix=prefix,
                )
                yield ((start, end), aggregated, collected, key)

        else:
            for window, aggregated, key in windows:
                yield (window, aggregated, [], key)

        # Delete expired windows from the state
        if delete:
            for (start, end), _, _ in windows:
                self.delete_window(start, end, prefix=prefix)
            if collect:
                self.delete_from_collection(end=start, prefix=prefix)

    def expire_all_windows(
        self,
        max_end_time: int,
        step_ms: int,
        delete: bool = True,
        collect: bool = False,
    ) -> Iterable[ExpiredWindowDetail]:
        """
        Get all expired windows for all prefix from RocksDB up to the specified `max_end_time` timestamp.

        :param max_end_time: The timestamp up to which windows are considered expired, inclusive.
        :param delete: If True, expired windows will be deleted.
        :param collect: If True, values will be collected into windows.
        """
        last_expired = self.get_latest_expired(prefix=b"")

        to_delete: set[tuple[bytes, int, int]] = set()
        collected = []

        if last_expired:
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
                            end=end,
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
                            end=end,
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
        for (start, end), _, _ in windows:
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
            self.delete_from_collection(end=max_start_time, prefix=prefix)

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


def _merge_sorted(
    left: Iterator[tuple[bytes, bytes]],
    right: Iterator[tuple[bytes, bytes]],
    backwards: bool,
) -> Iterator[tuple[bytes, bytes]]:
    """
    Merge two key-sorted iterators of `(key, value)` pairs into a single sorted
    stream, advancing whichever side currently holds the smaller key (the greater
    one when `backwards` is True).

    When both sides hold the same key the `left` value wins, so callers pass the
    uncommitted update cache on the left to let it shadow the stored value.
    """
    left_item = next(left, None)
    right_item = next(right, None)
    while True:
        if left_item is None:
            if right_item is None:
                return
            yield right_item
            right_item = next(right, None)
        elif right_item is None:
            yield left_item
            left_item = next(left, None)
        elif left_item[0] == right_item[0]:
            yield left_item
            left_item = next(left, None)
            right_item = next(right, None)
        elif (left_item[0] < right_item[0]) != backwards:
            yield left_item
            left_item = next(left, None)
        else:
            yield right_item
            right_item = next(right, None)


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
