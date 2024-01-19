from typing import Protocol, Any, Optional, Iterator, Callable, Dict

from typing_extensions import Self

DumpsFunc = Callable[[Any], bytes]
LoadsFunc = Callable[[bytes], Any]


class Store(Protocol):
    """
    Abstract state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    @property
    def topic(self) -> str:
        """
        Topic name
        """

    @property
    def name(self) -> str:
        """
        Store name
        """

    @property
    def partitions(self) -> Dict[int, "StorePartition"]:
        """
        Mapping of assigned store partitions
        :return: dict of "{partition: <StorePartition>}"
        """
        ...

    def assign_partition(self, partition: int) -> "StorePartition":
        """
        Assign new store partition

        :param partition: partition number
        :return: instance of `StorePartition`
        """
        ...

    def revoke_partition(self, partition: int):
        """
        Revoke assigned store partition

        :param partition: partition number
        """

        ...

    def start_partition_transaction(
        self, partition: int
    ) -> Optional["PartitionTransaction"]:
        """
        Start a new partition transaction.

        `PartitionTransaction` is the primary interface for working with data in Stores.
        :param partition: partition number
        :return: instance of `PartitionTransaction`
        """

    def close(self):
        """
        Close store and revoke all store partitions
        """

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...


class StorePartition(Protocol):
    """
    A base class to access state in the underlying storage.
    It represents a single instance of some storage (e.g. a single database for
    the persistent storage).

    """

    @property
    def path(self) -> str:
        """
        Absolute path to RocksDB database folder
        """
        ...

    def begin(self) -> "PartitionTransaction":
        """
        State new `PartitionTransaction`
        """

    def get_processed_offset(self) -> Optional[int]:
        ...


class State(Protocol):
    """
    Primary interface for working with key-value state data from `StreamingDataFrame`
    """

    def get(self, key: Any, default: Any = None) -> Optional[Any]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """

    def set(self, key: Any, value: Any):
        """
        Set value for the key.
        :param key: key
        :param value: value
        """

    def delete(self, key: Any):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        """

    def exists(self, key: Any) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :return: True if key exists, False otherwise
        """


class PartitionTransaction(State):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single storage partition.
    """

    @property
    def state(self) -> State:
        """
        An instance of State to be provided to `StreamingDataFrame` functions
        :return:
        """

    @property
    def failed(self) -> bool:
        """
        Return `True` if transaction failed to update data at some point.

        Failed transactions cannot be re-used.
        :return: bool
        """

    @property
    def completed(self) -> bool:
        """
        Return `True` if transaction is completed.

        Completed transactions cannot be re-used.
        :return: bool
        """
        ...

    def with_prefix(self, prefix: Any = b"") -> Iterator[Self]:
        """
        A context manager set the prefix for all keys in the scope.

        Normally, it's called by `StreamingDataFrame` internals to ensure that every
        message key is stored separately.
        :param prefix: key prefix
        :return: context maager
        """

    def maybe_flush(self, offset: Optional[int] = None):
        """
        Flush the recent updates and last processed offset to the storage.
        :param offset: offset of the last processed message, optional.
        """

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...


class WindowedState(Protocol):
    """
    A windowed state to be provided into `StreamingDataFrame` window functions.
    """

    def get_window(
        self, start_ms: int, end_ms: int, default: Any = None
    ) -> Optional[Any]:
        """
        Get the value of the window defined by `start` and `end` timestamps
        if the window is present in the state, else default

        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """

    def update_window(self, start_ms: int, end_ms: int, value: Any, timestamp_ms: int):
        """
        Set a value for the window.

        This method will also update the latest observed timestamp in state partition
        using the provided `timestamp`.

        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param value: value of the window
        :param timestamp_ms: current message timestamp in milliseconds
        """

    def get_latest_timestamp(self) -> int:
        """
        Get the latest observed timestamp for the current state partition.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """

    def expire_windows(self, duration_ms: int, grace_ms: int = 0):
        """
        Get a list of expired windows from RocksDB considering the current
        latest timestamp, window duration and grace period.

        It also marks the latest found window as expired in the expiration index, so
        calling this method multiple times will yield different results for the same
        "latest timestamp".

        :param duration_ms: duration of the windows in milliseconds
        :param grace_ms: grace period in milliseconds. Default - "0"
        """


class WindowedPartitionTransaction(WindowedState):
    @property
    def state(self) -> WindowedState:
        ...

    @property
    def failed(self) -> bool:
        """
        Return `True` if transaction failed to update data at some point.

        Failed transactions cannot be re-used.
        :return: bool
        """

    @property
    def completed(self) -> bool:
        """
        Return `True` if transaction is completed.

        Completed transactions cannot be re-used.
        :return: bool
        """
        ...

    def with_prefix(self, prefix: Any = b"") -> Iterator[Self]:
        """
        A context manager set the prefix for all keys in the scope.

        Normally, it's called by `StreamingDataFrame` internals to ensure that every
        message key is stored separately.
        :param prefix: key prefix
        :return: context maager
        """

    def maybe_flush(self, offset: Optional[int] = None):
        """
        Flush the recent updates and last processed offset to the storage.
        :param offset: offset of the last processed message, optional.
        """

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...
