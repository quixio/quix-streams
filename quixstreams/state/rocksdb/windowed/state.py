from typing import TYPE_CHECKING, Any, Iterable, Optional

from quixstreams.state.base import TransactionState
from quixstreams.state.types import ExpiredWindowDetail, WindowDetail, WindowedState

if TYPE_CHECKING:
    from .transaction import WindowedRocksDBPartitionTransaction


class WindowedTransactionState(TransactionState, WindowedState):
    __slots__ = ("_transaction", "_prefix")

    def __init__(
        self, transaction: "WindowedRocksDBPartitionTransaction", prefix: bytes
    ):
        """
        A windowed state to be provided into `StreamingDataFrame` window functions.

        :param transaction: instance of `WindowedRocksDBPartitionTransaction`
        """
        super().__init__(prefix=prefix, transaction=transaction)
        self._transaction: WindowedRocksDBPartitionTransaction = transaction

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
        return self._transaction.get_window(
            start_ms=start_ms, end_ms=end_ms, default=default, prefix=self._prefix
        )

    def update_window(
        self,
        start_ms: int,
        end_ms: int,
        value: Any,
        timestamp_ms: int,
    ) -> None:
        """
        Set a value for the window.

        This method will also update the latest observed timestamp in state partition
        using the provided `timestamp_ms`.

        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param value: value of the window
        :param timestamp_ms: current message timestamp in milliseconds
        """
        return self._transaction.update_window(
            start_ms=start_ms,
            end_ms=end_ms,
            value=value,
            timestamp_ms=timestamp_ms,
            prefix=self._prefix,
        )

    def add_to_collection(self, value: Any, id: Optional[int]) -> int:
        """
        Collect a value for collection-type window aggregations.

        This method is used internally by collection windows (created using
        .collect()) to store individual values. These values are later combined
        during window expiration.

        :param value: value to be collected
        :param timestamp_ms: current message timestamp in milliseconds
        """
        return self._transaction.add_to_collection(
            value=value,
            id=id,
            prefix=self._prefix,
        )

    def get_from_collection(self, start: int, end: int) -> list[Any]:
        """
        Return all values from a collection-type window aggregation.

        :param start: starting id of values to fetch (inclusive)
        :param end: end id of values to fetch (exclusive)
        """
        return self._transaction.get_from_collection(
            start=start, end=end, prefix=self._prefix
        )

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
        return self._transaction.delete_from_collection(
            end=end, start=start, prefix=self._prefix
        )

    def get_latest_timestamp(self) -> Optional[int]:
        """
        Get the latest observed timestamp for the current message key.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """

        return self._transaction.get_latest_timestamp(prefix=self._prefix)

    def expire_windows(
        self,
        max_start_time: int,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False,
    ) -> Iterable[ExpiredWindowDetail]:
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
        return self._transaction.expire_windows(
            max_start_time=max_start_time,
            prefix=self._prefix,
            delete=delete,
            collect=collect,
            end_inclusive=end_inclusive,
        )

    def get_windows(
        self, start_from_ms: int, start_to_ms: int, backwards: bool = False
    ) -> list[WindowDetail]:
        """
        Get all windows that start between "start_from_ms" and "start_to_ms".

        :param start_from_ms: The minimal window start time, exclusive.
        :param start_to_ms: The maximum window start time, inclusive.
        :param backwards: If True, yields windows in reverse order.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        return self._transaction.get_windows(
            start_from_ms=start_from_ms,
            start_to_ms=start_to_ms,
            prefix=self._prefix,
            backwards=backwards,
        )

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
        return self._transaction.delete_windows(
            max_start_time=max_start_time,
            delete_values=delete_values,
            prefix=self._prefix,
        )
