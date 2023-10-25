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
