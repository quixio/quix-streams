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
        return self._transaction.update_window(
            start_ms=start_ms,
            end_ms=end_ms,
            timestamp_ms=timestamp_ms,
            value=value,
            prefix=self._prefix,
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
        self, duration_ms: int, grace_ms: int = 0
    ) -> list[tuple[tuple[int, int], Any]]:
        """
        Get all expired windows from RocksDB based on the latest timestamp,
        window duration, and an optional grace period.

        This method marks the latest found window as expired in the expiration index,
        so consecutive calls may yield different results for the same "latest timestamp".

        :param duration_ms: The duration of each window in milliseconds.
        :param grace_ms: An optional grace period in milliseconds to delay expiration.
            Defaults to 0, meaning no grace period is applied.
        :return: A sorted list of tuples in the format `((start, end), value)`.
        """
        return self._transaction.expire_windows(
            duration_ms=duration_ms, grace_ms=grace_ms, prefix=self._prefix
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
