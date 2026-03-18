from typing import TYPE_CHECKING, Any, Optional

from quixstreams.state.base import TransactionState
from quixstreams.state.types import WindowDetail, WindowedState

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
