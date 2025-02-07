import logging
from typing import Any, Optional, Protocol, Tuple

logger = logging.getLogger(__name__)


class WindowedState(Protocol):
    """
    A windowed state to be provided into `StreamingDataFrame` window functions.
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

    def update_window(
        self,
        start_ms: int,
        end_ms: int,
        value: Any,
        timestamp_ms: int,
    ):
        """
        Set a value for the window.

        This method will also update the latest observed timestamp in state partition
        using the provided `timestamp_ms`.

        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param value: value of the window
        :param timestamp_ms: current message timestamp in milliseconds
        """
        ...

    def add_to_collection(self, value: Any, id: Optional[int]) -> int:
        """
        Collect a value for collection-type window aggregations.

        This method is used internally by collection windows (created using
        .collect()) to store individual values. These values are later combined
        during window expiration.

        :param value: value to be collected
        :param id: current message ID, for example timestamp in milliseconds, does not have to be unique.

        :return: the message ID, auto-generated if not provided
        """
        ...

    def get_from_collection(self, start: int, end: int) -> list[Any]:
        """
        Return all values from a collection-type window aggregation.

        :param start: starting id of values to fetch (inclusive)
        :param end: end id of values to fetch (exclusive)
        """
        ...

    def delete_from_collection(self, end: int, *, start: Optional[int] = None) -> None:
        """
        Delete collected values with id less than end.

        This method maintains a deletion index to track progress and avoid
        re-scanning previously deleted values. It:
        1. Retrieves the last deleted id from the cache
        2. Scans values from last deleted id up to end
        3. Updates the deletion index with the latest deleted id

        :param end: Delete values with id less than this value
        """
        ...

    def get_latest_timestamp(self) -> Optional[int]:
        """
        Get the latest observed timestamp for the current state partition.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """
        ...

    def expire_windows(
        self,
        max_start_time: int,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all expired windows from RocksDB up to the specified `max_start_time` timestamp.

        This method marks the latest found window as expired in the expiration index,
        so consecutive calls may yield different results for the same "latest timestamp".

        :param max_start_time: The timestamp up to which windows are considered expired, inclusive.
        :param delete: If True, expired windows will be deleted.
        :param collect: If True, values will be collected into windows.
        :param end_inclusive: If True, the end of the window will be inclusive.
            Relevant only together with `collect=True`.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        ...

    def delete_windows(self, max_start_time: int, delete_values: bool) -> None:
        """
        Delete windows from RocksDB up to the specified `max_start_time` timestamp.

        This method removes all window entries that have a start time less than or equal
        to the given `max_start_time`. It ensures that expired data is cleaned up
        efficiently without affecting unexpired windows.

        :param max_start_time: The timestamp up to which windows should be deleted, inclusive.
        :param delete_values: If True, values with timestamps less than max_start_time
            will be deleted, as they can no longer belong to any active window.
        """
        ...

    def get_windows(
        self, start_from_ms: int, start_to_ms: int, backwards: bool = False
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all windows that start between "start_from_ms" and "start_to_ms".

        :param start_from_ms: The minimal window start time, exclusive.
        :param start_to_ms: The maximum window start time, inclusive.
        :param backwards: If True, yields windows in reverse order.
        :return: A sorted list of tuples in the format `((start, end), value)`.
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

    def prepare(self, processed_offset: Optional[int]):
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

    def add_to_collection(self, value: Any, id: Optional[int]) -> int:
        """
        Collect a value for collection-type window aggregations.

        This method is used internally by collection windows (created using
        .collect()) to store individual values. These values are later combined
        during window expiration.

        :param value: value to be collected
        :param id: current message ID (for example, timestamp in milliseconds)

        :return: the message ID, auto-generated if not provided
        """
        ...

    def get_from_collection(self, start: int, end: int) -> list[Any]:
        """
        Return all values from a collection-type window aggregation.

        :param start: starting id of values to fetch (inclusive)
        :param end: end id of values to fetch (exclusive)
        """
        ...

    def delete_from_collection(self, end: int) -> None:
        """
        Delete collected values with id less than end.

        This method maintains a deletion index to track progress and avoid
        re-scanning previously deleted values. It:
        1. Retrieves the last deleted id from the cache
        2. Scans values from last deleted id up to end
        3. Updates the deletion index with the latest deleted id

        :param end: Delete values with id less than this value
        """
        ...

    def get_latest_timestamp(self, prefix: bytes) -> int:
        """
        Get the latest observed timestamp for the current state prefix
        (same as message key).

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """
        ...

    def expire_windows(
        self,
        max_start_time: int,
        prefix: bytes,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all expired windows from RocksDB up to the specified `max_start_time` timestamp.

        This method marks the latest found window as expired in the expiration index,
        so consecutive calls may yield different results for the same "latest timestamp".

        :param max_start_time: The timestamp up to which windows are considered expired, inclusive.
        :param prefix: The key prefix for filtering windows.
        :param delete: If True, expired windows will be deleted.
        :param collect: If True, values will be collected into windows.
        :param end_inclusive: If True, the end of the window will be inclusive.
            Relevant only together with `collect=True`.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        ...

    def delete_windows(
        self, max_start_time: int, delete_values: bool, prefix: bytes
    ) -> None:
        """
        Delete windows from RocksDB up to the specified `max_start_time` timestamp.

        This method removes all window entries that have a start time less than or equal
        to the given `max_start_time`. It ensures that expired data is cleaned up
        efficiently without affecting unexpired windows.

        :param max_start_time: The timestamp up to which windows should be deleted, inclusive.
        :param delete_values: If True, values with timestamps less than max_start_time
            will be deleted, as they can no longer belong to any active window.
        :param prefix: The key prefix used to identify and filter relevant windows.
        """
        ...

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

        :param start_from_ms: The minimal window start time, exclusive.
        :param start_to_ms: The maximum window start time, inclusive.
        :param prefix: The key prefix for filtering windows.
        :param backwards: If True, yields windows in reverse order.
        :return: A sorted list of tuples in the format `((start, end), value)`.
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
