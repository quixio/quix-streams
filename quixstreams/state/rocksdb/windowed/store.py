import struct
from datetime import datetime
from typing import Optional, Any, List, Tuple

from .cache import WindowExpirationCache
from .. import RocksDBOptionsType
from ..metadata import (
    KEY_SEPARATOR,
    KEY_SEPARATOR_STR,
    LATEST_TIMESTAMP_KEY,
)
from ..partition import RocksDBStorePartition, RocksDBPartitionTransaction
from ..store import RocksDBStore
from ... import DumpsFunc, LoadsFunc


def _float_to_double_bytes(value: float) -> bytes:
    return struct.pack(">d", value)


def _float_from_double_bytes(value: bytes) -> int:
    return struct.unpack(">d", value)[0]


_DOUBLE_BYTES_ZERO = _float_to_double_bytes(0.0)


def _parse_window_key(
    key: bytes,
    loads: LoadsFunc,
) -> (bytes, float, float):
    parts = key.split(KEY_SEPARATOR, 1)
    if len(parts) != 2:
        raise ValueError(f"Could not parse window key: {key}")

    message_key, timestamps_bytes = parts
    start_str, end_str = loads(timestamps_bytes).split(KEY_SEPARATOR_STR)
    start, end = float(start_str), float(end_str)
    return message_key, start, end


def _get_window_key(start: float, end: float) -> str:
    return str(start) + KEY_SEPARATOR_STR + str(end)


class WindowedRocksDBStore(RocksDBStore):
    def __init__(
        self,
        name: str,
        topic: str,
        base_dir: str,
        grace_period: float = 0.0,
        options: Optional[RocksDBOptionsType] = None,
        open_max_retries: int = 10,
        open_retry_backoff: float = 3.0,
    ):
        super().__init__(
            name=name,
            topic=topic,
            base_dir=base_dir,
            options=options,
            open_max_retries=open_max_retries,
            open_retry_backoff=open_retry_backoff,
        )
        self._grace_period = grace_period

    def create_new_partition(self, path: str) -> "WindowedRocksDBStorePartition":
        db_partition = WindowedRocksDBStorePartition(
            path=path,
            options=self._options,
            open_max_retries=self._open_max_retries,
            open_retry_backoff=self._open_retry_backoff,
            grace_period=self._grace_period,
        )
        loads = self._options.loads
        for db_key in db_partition.keys():
            message_key, window_start, window_end = _parse_window_key(
                key=db_key, loads=loads
            )
            db_partition.expiration_cache.add(
                message_key=message_key, start=window_start, end=window_end
            )
        return db_partition

    def start_partition_transaction(
        self, partition: int, key: Any
    ) -> "WindowedRocksDBPartitionTransaction":
        return super().start_partition_transaction(partition=partition, key=key)


class WindowedRocksDBStorePartition(RocksDBStorePartition):
    def __init__(
        self,
        path: str,
        grace_period: float = 0.0,
        options: Optional[RocksDBOptionsType] = None,
        open_max_retries: int = 10,
        open_retry_backoff: float = 3.0,
    ):
        super().__init__(
            path=path,
            options=options,
            open_max_retries=open_max_retries,
            open_retry_backoff=open_retry_backoff,
        )
        self.grace_period = grace_period
        self.expiration_cache = WindowExpirationCache(grace_period=grace_period)
        self._latest_timestamp = self._get_latest_timestamp_from_db()

    def begin(self, message_key: Any) -> "WindowedRocksDBPartitionTransaction":
        return WindowedRocksDBPartitionTransaction(
            partition=self,
            message_key=message_key,
            dumps=self._options.dumps,
            loads=self._options.loads,
            latest_timestamp=self._latest_timestamp,
        )

    def _get_latest_timestamp_from_db(self) -> float:
        value = self.get(LATEST_TIMESTAMP_KEY, default=_DOUBLE_BYTES_ZERO)
        return _float_from_double_bytes(value)

    def set_latest_timestamp(self, timestamp: float):
        self._latest_timestamp = timestamp


# TODO: Typings
class WindowedRocksDBPartitionTransaction(RocksDBPartitionTransaction):
    _partition: WindowedRocksDBStorePartition

    def __init__(
        self,
        partition: RocksDBStorePartition,
        message_key: Any,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        latest_timestamp: float,
    ):
        super().__init__(
            partition=partition, message_key=message_key, dumps=dumps, loads=loads
        )
        self._latest_timestamp = latest_timestamp

    def get_window(self, start: float, end: float, default: Any = None) -> Any:
        key = _get_window_key(start, end)
        return self.get(key=key, default=default)

    def update_window(self, start: float, end: float, value: Any, timestamp: float):
        key = _get_window_key(start, end)
        self.set(key=key, value=value)
        self._latest_timestamp = max(self._latest_timestamp, timestamp)
        self._partition.expiration_cache.add(
            message_key=self._message_key, start=start, end=end
        )

    def delete_window(self, start: float, end: float):
        start_iso = datetime.fromtimestamp(start).isoformat()
        end_iso = datetime.fromtimestamp(end).isoformat()
        key = start_iso + KEY_SEPARATOR_STR + end_iso
        return self.delete(key=key)

    def get_expired_windows(self) -> List[Tuple[float, float]]:
        return list(
            self._partition.expiration_cache.get_expired(
                message_key=self._message_key, now=self._latest_timestamp
            )
        )

    def maybe_flush(self, offset: Optional[int] = None):
        expiration_cache = self._partition.expiration_cache
        for start, end in expiration_cache.get_expired(
            message_key=self._message_key, now=self._latest_timestamp
        ):
            self.delete_window(start=start, end=end)
        self._batch.put(
            LATEST_TIMESTAMP_KEY, _float_to_double_bytes(self._latest_timestamp)
        )
        super().maybe_flush(offset=offset)
        expiration_cache.expire(
            message_key=self._message_key, now=self._latest_timestamp
        )
        self._partition.set_latest_timestamp(self._latest_timestamp)
