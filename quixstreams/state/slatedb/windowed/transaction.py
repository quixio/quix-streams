from typing import TYPE_CHECKING, Any, Iterable, Optional, cast

from quixstreams.state.base.transaction import (
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.metadata import DEFAULT_PREFIX
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import (
    DumpsFunc,
    LoadsFunc,
    encode_integer_pair,
    int_to_bytes,
    serialize,
)
from quixstreams.state.types import WindowDetail

from ...rocksdb.windowed.metadata import (
    LATEST_DELETED_VALUE_CF_NAME,
    LATEST_DELETED_VALUE_TIMESTAMP_KEY,
    LATEST_DELETED_WINDOW_CF_NAME,
    LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
    LATEST_TIMESTAMP_KEY,
    LATEST_TIMESTAMPS_CF_NAME,
)
from ...rocksdb.windowed.serialization import parse_window_key
from ...rocksdb.windowed.state import WindowedTransactionState
from ...rocksdb.windowed.transaction import WindowedRocksDBPartitionTransaction

if TYPE_CHECKING:
    from .partition import WindowedSlateDBStorePartition


class WindowedSlateDBPartitionTransaction(WindowedRocksDBPartitionTransaction):
    def __init__(
        self,
        partition: "WindowedSlateDBStorePartition",
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
        self._partition: WindowedSlateDBStorePartition = cast(
            "WindowedSlateDBStorePartition", self._partition
        )
        self._latest_timestamps = {
            "key": LATEST_TIMESTAMP_KEY,
            "cf": LATEST_TIMESTAMPS_CF_NAME,
        }
        self._last_expired_timestamps = {
            "key": LATEST_EXPIRED_WINDOW_TIMESTAMP_KEY,
            "cf": LATEST_EXPIRED_WINDOW_CF_NAME,
        }
        self._last_deleted_window_timestamps = {
            "key": LATEST_DELETED_WINDOW_TIMESTAMP_KEY,
            "cf": LATEST_DELETED_WINDOW_CF_NAME,
        }
        self._last_deleted_value_timestamps = {
            "key": LATEST_DELETED_VALUE_TIMESTAMP_KEY,
            "cf": LATEST_DELETED_VALUE_CF_NAME,
        }

    def as_state(self, prefix: Any = DEFAULT_PREFIX) -> WindowedTransactionState:  # type: ignore [override]
        return WindowedTransactionState(
            transaction=self,
            prefix=(
                prefix
                if isinstance(prefix, bytes)
                else serialize(prefix, dumps=self._dumps)
            ),
        )

    def expire_windows(
        self,
        max_start_time: int,
        prefix: bytes,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> list[WindowDetail]:
        # Minimal path: list windows up to max_start_time and optionally delete
        windows = self.get_windows(
            start_from_ms=-1,
            start_to_ms=max_start_time + 1,
            prefix=prefix,
            backwards=False,
        )
        if delete:
            for (start, end), _ in windows:
                self.delete_window(start_ms=start, end_ms=end, prefix=prefix)
        return windows

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def keys(self, cf_name: str = "default") -> Iterable[Any]:
        db_skip_keys: set[bytes] = set()
        cache = self._update_cache.get_updates(cf_name=cf_name)
        for prefix_update_cache in cache.values():
            db_skip_keys.update(prefix_update_cache.keys())
            yield from prefix_update_cache.keys()
        db_skip_keys.update(self._update_cache.get_deletes())
        for key in self._partition.iter_keys(cf_name=cf_name):
            if key in db_skip_keys:
                continue
            yield key

    def get_window(
        self,
        start_ms: int,
        end_ms: int,
        prefix: bytes,
        default: Any = None,
    ) -> Any:
        key = encode_integer_pair(start_ms, end_ms)
        return self.get(key=key, default=default, prefix=prefix)

    def get_windows(
        self,
        start_from_ms: int,
        start_to_ms: int,
        prefix: bytes,
        backwards: bool = False,
    ) -> list[WindowDetail]:
        # Align semantics with RocksDB: lower bound exclusive, upper bound inclusive.
        items = self._get_items(
            start=start_from_ms,
            end=start_to_ms + 1,  # make upper bound inclusive
            prefix=prefix,
            backwards=backwards,
        )
        result: list[WindowDetail] = []
        for key, value in items:
            msg_key, start, end = parse_window_key(key)
            if msg_key != prefix:
                continue
            if start_from_ms < start <= start_to_ms:
                result.append(((start, end), self._deserialize_value(value)))
        return result

    def delete_window(self, start_ms: int, end_ms: int, prefix: bytes):
        key = encode_integer_pair(start_ms, end_ms)
        self.delete(key=key, prefix=prefix)

    def update_window(
        self,
        start_ms: int,
        end_ms: int,
        value: Any,
        timestamp_ms: int,
        prefix: bytes,
    ) -> None:
        key = encode_integer_pair(start_ms, end_ms)
        # Ensure bytes serialization using the transaction serializer to match base behavior
        self.set(key=key, value=value, prefix=prefix)
        latest = self.get_bytes(
            key=self._latest_timestamps["key"],
            prefix=prefix,
            default=None,
            cf_name=self._latest_timestamps["cf"],
        )  # type: ignore
        curr = int_to_bytes(timestamp_ms)
        if latest is None or latest < curr:
            self.set_bytes(
                key=self._latest_timestamps["key"],
                value=curr,
                prefix=prefix,
                cf_name=self._latest_timestamps["cf"],
            )  # type: ignore

    # Minimal subset: delete_window and get_windows could be added later as needed
