from typing import Any, Generator, Optional, List, Tuple, TYPE_CHECKING, cast

from rocksdict import ReadOptions

from quixstreams.state.metadata import DELETED, PREFIX_SEPARATOR, DEFAULT_PREFIX
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
        if not self._update_cache:
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
            data=self._update_cache,
            processed_offset=processed_offset,
            changelog_offset=changelog_offset,
            latest_timestamp_ms=self._latest_timestamp_ms,
        )

    def expire_windows(
        self, duration_ms: int, prefix: bytes, grace_ms: int = 0
    ) -> List[Tuple[Tuple[int, int], Any]]:
        """
        Get a list of expired windows from RocksDB considering latest timestamp,
        window size and grace period.
        It marks the latest found window as expired in the expiration index, so
        calling this method multiple times will yield different results for the same
        "latest timestamp".

        How it works:
        - First, it looks for the start time of the last expired window for the current
          prefix using expiration cache. If it's found, it will be used to reduce
          the search space and to avoid returning already expired windows.
        - Then it goes over window segments and fetches the windows
          that should be expired.
        - At last, it updates the expiration cache with the start time of the latest
          found windows

        :return: sorted list of tuples in format `((start, end), value)`
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
        self, start_from_ms: int, start_to_ms: int, prefix: bytes
    ) -> Generator[Tuple[Tuple[int, int], Any], None, None]:
        """
        Get all windows starting between "start_from" and "start_to"
        within the given prefix.

        This function also checks the update cache in case some updates have not
        been committed to RocksDB yet.

        :param start_from_ms: minimal window start time, exclusive
        :param start_to_ms: maximum window start time, inclusive
        :param prefix: a key prefix
        :return: generator yielding sorted tuples in format `((start, end), value)`
        """

        # Iterate over rocksdb within the given prefix and (start_form, start_to)
        # timestamps
        seek_from = max(start_from_ms, 0)
        seek_from_key = encode_window_prefix(prefix=prefix, start_ms=seek_from)

        # Add +1 to make the "start_to" inclusive
        seek_to = start_to_ms + 1
        seek_to_key = encode_window_prefix(prefix=prefix, start_ms=seek_to)

        # Set iterator bounds to reduce the potential IO
        read_opt = ReadOptions()
        read_opt.set_iterate_lower_bound(seek_from_key)
        read_opt.set_iterate_upper_bound(seek_to_key)

        db_windows = self._partition.iter_items(
            read_opt=read_opt, from_key=seek_from_key
        )

        # If the cache is empty then take this shortcut to yield only db windows
        if not (cache := self._update_cache.get("default", {}).get(prefix, {})):
            for db_key, db_value in db_windows:
                _, start, end = parse_window_key(db_key)
                if start_from_ms < start <= start_to_ms:
                    yield (start, end), self._deserialize_value(db_value)
            return

        cached_windows = sorted(cache.items(), reverse=True)
        cached_key, cached_value = cached_windows.pop()

        for db_key, db_value in db_windows:
            yield_db_window = True
            if cached_key:
                # Yield all cached windows with lower or equal timestamp
                while cached_key <= db_key:
                    if cached_value is not DELETED:
                        _, start, end = parse_window_key(cached_key)
                        if start_from_ms < start <= start_to_ms:
                            yield (start, end), self._deserialize_value(cached_value)

                    # Cached window with equal timestamp takes precedence
                    # so do not yield db window
                    yield_db_window = cached_key != db_key

                    try:
                        cached_key, cached_value = cached_windows.pop()
                    except IndexError:
                        cached_key, cached_value = None, None
                        break

            if yield_db_window:
                _, start, end = parse_window_key(db_key)
                if start_from_ms < start <= start_to_ms:
                    yield (start, end), self._deserialize_value(db_value)

        # Yield remaining cached windows
        while cached_key:
            if cached_value is not DELETED:
                _, start, end = parse_window_key(cached_key)
                if start_from_ms < start <= start_to_ms:
                    yield (start, end), self._deserialize_value(cached_value)

            try:
                cached_key, cached_value = cached_windows.pop()
            except IndexError:
                return
