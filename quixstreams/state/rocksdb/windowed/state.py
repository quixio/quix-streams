from typing import Any, Optional, List, Tuple, TYPE_CHECKING

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
        Get the latest observed timestamp for the current state partition.

        Use this timestamp to determine if the arriving event is late and should be
        discarded from the processing.

        :return: latest observed event timestamp in milliseconds
        """

        return self._transaction.get_latest_timestamp()

    def expire_windows(
        self, duration_ms: int, grace_ms: int = 0
    ) -> List[Tuple[Tuple[int, int], Any]]:
        """
        Get a list of expired windows from RocksDB considering the current
        latest timestamp, window duration and grace period.

        It also marks the latest found window as expired in the expiration index, so
        calling this method multiple times will yield different results for the same
        "latest timestamp".
        """
        return self._transaction.expire_windows(
            duration_ms=duration_ms, grace_ms=grace_ms, prefix=self._prefix
        )
