import enum
from typing import Protocol, Any, Optional, Callable, Dict, ClassVar, Tuple, List

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.models.types import MessageHeadersMapping

DumpsFunc = Callable[[Any], bytes]
LoadsFunc = Callable[[bytes], Any]


class Store(Protocol):
    """
    Abstract state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    options_type: ClassVar[object]

    @property
    def topic(self) -> str:
        """
        Topic name
        """
        ...

    @property
    def name(self) -> str:
        """
        Store name
        """
        ...

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

    def start_partition_transaction(self, partition: int) -> "PartitionTransaction":
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
        ...

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...


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

    def recover_from_changelog_message(
        self, changelog_message: ConfluentKafkaMessageProto, committed_offset: int
    ):
        """
        Updates state from a given changelog message.

        :param changelog_message: A raw Confluent message read from a changelog topic.
        :param committed_offset: latest committed offset for the partition
        """
        ...

    def get_processed_offset(self) -> Optional[int]:
        """
        Get last processed offset for the given partition
        :return: offset or `None` if there's no processed offset yet
        """
        ...

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        ...

    def set_changelog_offset(self, changelog_offset: int):
        """
        Set the changelog offset based on a message (usually an "offset-only" message).

        Used during recovery.

        :param changelog_offset: A changelog offset
        """
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
        ...

    def set(self, key: Any, value: Any):
        """
        Set value for the key.
        :param key: key
        :param value: value
        """
        ...

    def delete(self, key: Any):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        """
        ...

    def exists(self, key: Any) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :return: True if key exists, False otherwise
        """
        ...


class PartitionTransaction(Protocol):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single storage partition.
    """

    def as_state(self, prefix: Any) -> State:
        """
        Create an instance implementing the `State` protocol to be provided
        to `StreamingDataFrame` functions.
        All operations called on this State object will be prefixed with
        the supplied `prefix`.

        :return: an instance implementing the `State` protocol
        """
        ...

    def get(self, key: Any, prefix: bytes, default: Any = None) -> Optional[Any]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param prefix: a key prefix
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        ...

    def set(self, key: Any, prefix: bytes, value: Any):
        """
        Set value for the key.
        :param key: key
        :param prefix: a key prefix
        :param value: value
        """
        ...

    def delete(self, key: Any, prefix: bytes):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        :param prefix: a key prefix
        """
        ...

    def exists(self, key: Any, prefix: bytes) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :param prefix: a key prefix
        :return: True if key exists, False otherwise
        """
        ...

    @property
    def failed(self) -> bool:
        """
        Return `True` if transaction failed to update data at some point.

        Failed transactions cannot be re-used.
        :return: bool
        """
        ...

    @property
    def completed(self) -> bool:
        """
        Return `True` if transaction is successfully completed.

        Completed transactions cannot be re-used.
        :return: bool
        """
        ...

    @property
    def prepared(self) -> bool:
        """
        Return `True` if transaction is prepared completed.

        Prepared transactions cannot receive new updates, but can be flushed.
        :return: bool
        """
        ...

    def prepare(self, processed_offset: int):
        """
        Produce changelog messages to the changelog topic for all changes accumulated
        in this transaction and prepare transcation to flush its state to the state
        store.

        After successful `prepare()`, the transaction status is changed to PREPARED,
        and it cannot receive updates anymore.

        If changelog is disabled for this application, no updates will be produced
        to the changelog topic.

        :param processed_offset: the offset of the latest processed message
        """

    @property
    def changelog_topic_partition(self) -> Optional[Tuple[str, int]]:
        """
        Return the changelog topic-partition for the StorePartition of this transaction.

        Returns `None` if changelog_producer is not provided.

        :return: (topic, partition) or None
        """

    def flush(
        self,
        processed_offset: Optional[int] = None,
        changelog_offset: Optional[int] = None,
    ):
        """
        Flush the recent updates to the storage.

        :param processed_offset: offset of the last processed message, optional.
        :param changelog_offset: offset of the last produced changelog message,
            optional.
        """

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...


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
        ...

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
        ...

    def get_latest_timestamp(self) -> int:
        """
        Get the latest observed timestamp for the current state partition.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """
        ...

    def expire_windows(
        self, duration_ms: int, grace_ms: int = 0
    ) -> List[Tuple[Tuple[int, int], Any]]:
        """
        Get a list of expired windows from RocksDB considering the current
        latest timestamp, window duration and grace period.

        It also marks the latest found window as expired in the expiration index, so
        calling this method multiple times will yield different results for the same
        "latest timestamp".

        :param duration_ms: duration of the windows in milliseconds
        :param grace_ms: grace period in milliseconds. Default - "0"
        """
        ...


class WindowedPartitionTransaction(Protocol):

    @property
    def failed(self) -> bool:
        """
        Return `True` if transaction failed to update data at some point.

        Failed transactions cannot be re-used.
        :return: bool
        """
        ...

    @property
    def completed(self) -> bool:
        """
        Return `True` if transaction is successfully completed.

        Completed transactions cannot be re-used.
        :return: bool
        """
        ...

    @property
    def prepared(self) -> bool:
        """
        Return `True` if transaction is prepared completed.

        Prepared transactions cannot receive new updates, but can be flushed.
        :return: bool
        """
        ...

    def prepare(self, processed_offset: int):
        """
        Produce changelog messages to the changelog topic for all changes accumulated
        in this transaction and prepare transcation to flush its state to the state
        store.

        After successful `prepare()`, the transaction status is changed to PREPARED,
        and it cannot receive updates anymore.

        If changelog is disabled for this application, no updates will be produced
        to the changelog topic.

        :param processed_offset: the offset of the latest processed message
        """

    def as_state(self, prefix: Any) -> WindowedState: ...

    def get_window(
        self,
        start_ms: int,
        end_ms: int,
        prefix: bytes,
        default: Any = None,
    ) -> Optional[Any]:
        """
        Get the value of the window defined by `start` and `end` timestamps
        if the window is present in the state, else default

        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param prefix: a key prefix
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        ...

    def update_window(
        self, start_ms: int, end_ms: int, value: Any, timestamp_ms: int, prefix: bytes
    ):
        """
        Set a value for the window.

        This method will also update the latest observed timestamp in state partition
        using the provided `timestamp`.

        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param value: value of the window
        :param timestamp_ms: current message timestamp in milliseconds
        :param prefix: a key prefix
        """
        ...

    def get_latest_timestamp(self) -> int:
        """
        Get the latest observed timestamp for the current state partition.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """
        ...

    def expire_windows(self, duration_ms: int, prefix: bytes, grace_ms: int = 0):
        """
        Get a list of expired windows from RocksDB considering the current
        latest timestamp, window duration and grace period.

        It also marks the latest found window as expired in the expiration index, so
        calling this method multiple times will yield different results for the same
        "latest timestamp".

        :param duration_ms: duration of the windows in milliseconds
        :param prefix: a key prefix
        :param grace_ms: grace period in milliseconds. Default - "0"
        """
        ...

    def flush(
        self,
        processed_offset: Optional[int] = None,
        changelog_offset: Optional[int] = None,
    ):
        """
        Flush the recent updates to the storage.

        :param processed_offset: offset of the last processed message, optional.
        :param changelog_offset: offset of the last produced changelog message,
            optional.
        """

    @property
    def changelog_topic_partition(self) -> Optional[Tuple[str, int]]:
        """
        Return the changelog topic-partition for the StorePartition of this transaction.

        Returns `None` if changelog_producer is not provided.

        :return: (topic, partition) or None
        """

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...


class PartitionRecoveryTransaction(Protocol):
    """
    A class for managing recovery for a StorePartition from a changelog message
    """

    def write_from_changelog_message(self): ...

    def flush(self):
        """
        Flush the recovery update to the storage.
        """
        ...


class PartitionTransactionStatus(enum.Enum):
    STARTED = 1  # Transaction is started and accepts updates

    PREPARED = 2  # Transaction is prepared, it can no longer receive updates
    # and can only be flushed

    COMPLETE = 3  # Transaction is fully completed, it cannot be used anymore

    FAILED = 4  # Transaction is failed, it cannot be used anymore
