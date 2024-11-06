from typing import TYPE_CHECKING, Any, Optional

from quixstreams.state.types import WindowedState

if TYPE_CHECKING:
    from .transaction import WindowedRocksDBPartitionTransaction


class WindowedTransactionState(WindowedState):
    __slots__ = ("_transaction", "_prefix")

    def __init__(
        self, transaction: "WindowedRocksDBPartitionTransaction", prefix: bytes
    ):
        """
        A windowed state to be provided into `StreamingDataFrame` window functions.

        :param transaction: instance of `WindowedRocksDBPartitionTransaction`
        """
        self._transaction = transaction
        self._prefix = prefix

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
        window_timestamp_ms: Optional[int] = None,
    ) -> None:
        """
        Set a value for the window.

        This method will also update the latest observed timestamp in state partition
        using the provided `timestamp`.


        :param start_ms: start of the window in milliseconds
        :param end_ms: end of the window in milliseconds
        :param value: value of the window
        :param timestamp_ms: current message timestamp in milliseconds
        :param window_timestamp_ms: arbitrary timestamp stored with the window value
        """
        return self._transaction.update_window(
            start_ms=start_ms,
            end_ms=end_ms,
            timestamp_ms=timestamp_ms,
            value=value,
            prefix=self._prefix,
            window_timestamp_ms=window_timestamp_ms,
        )

    def get_latest_timestamp(self) -> int:
        """
        Get the latest observed timestamp for the current message key.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """

        return self._transaction.get_latest_timestamp(prefix=self._prefix)

    def expire_windows(
        self, max_start_time: int, delete: bool = True
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all expired windows from RocksDB up to the specified `max_start_time` timestamp.

        This method marks the latest found window as expired in the expiration index,
        so consecutive calls may yield different results for the same "latest timestamp".

        :param max_start_time: The timestamp up to which windows are considered expired, inclusive.
        :param delete: If True, expired windows will be deleted.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        return self._transaction.expire_windows(
            max_start_time=max_start_time, prefix=self._prefix, delete=delete
        )

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
        return self._transaction.get_windows(
            start_from_ms=start_from_ms,
            start_to_ms=start_to_ms,
            prefix=self._prefix,
            backwards=backwards,
        )

    def delete_windows(self, max_start_time: int) -> None:
        """
        Delete windows from RocksDB up to the specified `max_start_time` timestamp.

        This method removes all window entries that have a start time less than or equal to the given
        `max_start_time`. It ensures that expired data is cleaned up efficiently without affecting
        unexpired windows.

        :param max_start_time: The timestamp up to which windows should be deleted, inclusive.
        """
        return self._transaction.delete_windows(
            max_start_time=max_start_time, prefix=self._prefix
        )
