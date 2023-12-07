from typing import Optional, Any, Tuple

import rocksdict

from .cache import WindowExpirationCache
from ..metadata import (
    METADATA_CF_NAME,
    PREFIX_SEPARATOR,
    LATEST_TIMESTAMP_KEY,
    EXPIRED_WINDOWS_CF_NAME,
)
from ..partition import RocksDBStorePartition, RocksDBPartitionTransaction
from ..serialization import serialize, float_to_double_bytes, float_from_double_bytes
from ..store import RocksDBStore
from ..types import RocksDBOptionsType, DumpsFunc, LoadsFunc

_DOUBLE_BYTES_ZERO = float_to_double_bytes(0.0)
_TIMESTAMP_BYTE_LENGTH = len(_DOUBLE_BYTES_ZERO)


def _parse_window_key(key: bytes) -> Tuple[bytes, float, float]:
    """
    Parse the window key from Rocksdb into (message_key, start, end).

    Expected window key format:
    <message_key>|<start>|<end>

    :param key: a key from Rocksdb
    :return: a tuple with message key, start timestamp, end timestamp
    """
    # Get a timestamps segment from the bytes key
    # Should be last 17 bytes (2*8 bytes for timestamps and 1 byte for separator)
    timestamps_segment_len = _TIMESTAMP_BYTE_LENGTH * 2 + 1
    message_key, timestamps_bytes = (
        key[: -timestamps_segment_len - 1],
        key[-timestamps_segment_len:],
    )
    start_bytes, end_bytes = (
        timestamps_bytes[:_TIMESTAMP_BYTE_LENGTH],
        timestamps_bytes[_TIMESTAMP_BYTE_LENGTH + 1 :],
    )

    start, end = float_from_double_bytes(start_bytes), float_from_double_bytes(
        end_bytes
    )
    return message_key, start, end


def _encode_window_key(start: float, end: float) -> bytes:
    """
    Encode window start and end timestamps into bytes of the following format:
    ```<start>|<end>```

    Encoding window keys this way make them sortable in RocksDB within the same prefix.

    :param start: window start
    :param end: window end
    :return: window timestamps as bytes
    """
    return float_to_double_bytes(start) + PREFIX_SEPARATOR + float_to_double_bytes(end)


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
        return db_partition

    def start_partition_transaction(
        self, partition: int
    ) -> "WindowedRocksDBPartitionTransaction":
        return super().start_partition_transaction(partition=partition)


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

    def _open_rocksdict(self) -> rocksdict.Rdict:
        options = self._options.to_options()
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        rdict = rocksdict.Rdict(
            path=self._path,
            options=options,
            access_type=rocksdict.AccessType.read_write(),
            column_families={
                METADATA_CF_NAME: options,
                EXPIRED_WINDOWS_CF_NAME: options,
            },
        )
        return rdict

    def begin(self) -> "WindowedRocksDBPartitionTransaction":
        return WindowedRocksDBPartitionTransaction(
            partition=self,
            dumps=self._options.dumps,
            loads=self._options.loads,
            latest_timestamp=self._latest_timestamp,
        )

    def _get_latest_timestamp_from_db(self) -> float:
        value = self.get(
            LATEST_TIMESTAMP_KEY, default=_DOUBLE_BYTES_ZERO, cf_name=METADATA_CF_NAME
        )
        return float_from_double_bytes(value)

    def set_latest_timestamp(self, timestamp: float):
        self._latest_timestamp = timestamp


# TODO: Typings
class WindowedRocksDBPartitionTransaction(RocksDBPartitionTransaction):
    __slots__ = ("_latest_timestamp",)
    _partition: WindowedRocksDBStorePartition

    def __init__(
        self,
        partition: RocksDBStorePartition,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        latest_timestamp: float,
    ):
        super().__init__(partition=partition, dumps=dumps, loads=loads)
        self._latest_timestamp = latest_timestamp
        self._state = WindowedTransactionState(transaction=self)

    @property
    def state(self) -> "WindowedTransactionState":
        return self._state

    def get_latest_timestamp(self) -> float:
        return self._latest_timestamp

    def get_window(self, start: float, end: float, default: Any = None) -> Any:
        key = _encode_window_key(start, end)
        return self.get(key=key, default=default)

    def update_window(self, start: float, end: float, value: Any, timestamp: float):
        key = _encode_window_key(start, end)
        self.set(key=key, value=value)
        self._latest_timestamp = max(self._latest_timestamp, timestamp)

    def maybe_flush(self, offset: Optional[int] = None):
        cf_handle = self._partition.get_column_family_handle(METADATA_CF_NAME)
        self._batch.put(
            LATEST_TIMESTAMP_KEY,
            float_to_double_bytes(self._latest_timestamp),
            cf_handle,
        )
        super().maybe_flush(offset=offset)
        self._partition.set_latest_timestamp(self._latest_timestamp)

    def _serialize_key(self, key: Any) -> bytes:
        # Allow bytes keys in WindowedStore
        key_bytes = key if isinstance(key, bytes) else serialize(key, dumps=self._dumps)
        return self._prefix + PREFIX_SEPARATOR + key_bytes


class WindowedTransactionState:
    __slots__ = ("_transaction",)

    def __init__(self, transaction: WindowedRocksDBPartitionTransaction):
        """
        A windowed state to be provided into `StreamingDataFrame` window functions.

        :param transaction: instance of `WindowedRocksDBPartitionTransaction`
        """
        self._transaction = transaction

    def get_window(
        self, start: float, end: float, default: Any = None
    ) -> Optional[Any]:
        """
        Get the value of the window defined by `start` and `end` timestamps
        if the window is present in the state, else default

        :param start: start of the window
        :param end: end of the window
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        return self._transaction.get_window(start=start, end=end, default=default)

    def update_window(self, start: float, end: float, value: Any, timestamp: float):
        """
        Set a value for the window.

        This method will also update the latest observed timestamp in state partition
        using the provided `timestamp`.


        :param start: start of the window
        :param end: end of the window
        :param value: value of the window
        :param timestamp: current message timestamp
        """
        return self._transaction.update_window(
            start=start, end=end, timestamp=timestamp, value=value
        )

    def get_latest_timestamp(self) -> float:
        """
        Get the latest observed timestamp for the current state partition.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp
        """

        return self._transaction.get_latest_timestamp()
