from datetime import timedelta
from typing_extensions import TypedDict
from typing import Any, Optional, Callable, TypeVar, List

from quixstreams.state.rocksdb.windowed.store import WindowedTransactionState
from quixstreams.context import message_context


class WindowResult(TypedDict):
    start: float
    end: float
    value: Any


T = TypeVar("T")
DataFrameWindowFunc = Callable[[float, float, float, T, WindowedTransactionState], Any]


def get_window_range(timestamp: float, duration: float):
    closest_step = (timestamp // duration) * duration
    left = closest_step - duration + duration
    right = left + duration - 0.1
    return left, right


class TumblingWindowDefinition:
    StreamingDataFrame = TypeVar("StreamingDataFrame")

    def __init__(
        self,
        duration: float | timedelta,
        grace: float | timedelta,
        dataframe: StreamingDataFrame,
        name: Optional[str] = None,
    ):
        self._duration = (
            duration.total_seconds() if isinstance(duration, timedelta) else duration
        )
        self._grace = grace.total_seconds() if isinstance(grace, timedelta) else grace
        self._dataframe = dataframe
        self._name = name

    def _get_name(self, func_name: str) -> str:
        return self._name or f"tumbling_window_{self._duration}_{func_name}"

    def sum(self) -> "TumblingWindow":
        """
        Configure the tumbling window to aggregate data by summing up values within each window period.

        :return: TumblingWindow instance configured to perform sum aggregation.
        """
        name = self._get_name(func_name="sum")

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end) or 0
            updated_value = current_value + value

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=name,
            func=func,
            dataframe=self._dataframe,
        )

    def count(self) -> "TumblingWindow":
        """
        Configure the tumbling window to aggregate data by counting the number of records within each window period.

        :return: TumblingWindow instance configured to perform record count.
        """
        name = self._get_name(func_name="count")

        def func(start, end, timestamp, _: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end) or 0
            updated_value = current_value + 1

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=name,
            func=func,
            dataframe=self._dataframe,
        )

    def mean(self) -> "TumblingWindow":
        """
        Configure the tumbling window to aggregate data by calculating the mean of the values within each window period.

        :return: TumblingWindow instance configured to calculate the mean of the data values.
        """
        name = self._get_name(func_name="mean")

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_window_value = state.get_window(start=start, end=end) or (0.0, 0)

            agg = current_window_value[0] + value
            count = current_window_value[1] + 1

            updated_window_value = (agg, count)

            state.update_window(
                start, end, timestamp=timestamp, value=updated_window_value
            )
            return agg / count

        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=name,
            func=func,
            dataframe=self._dataframe,
        )

    def reduce(self, reduce_func: Callable[[Any, Any], Any]) -> "TumblingWindow":
        """
        Configure the tumbling window to perform a custom aggregation using a user-provided reduce function.

        :param reduce_func: A Callable function that takes two arguments (the accumulated value and the current value)
        and returns a single value. This function is used for aggregating data within the window.

        :return: TumblingWindow instance configured to perform custom reduce aggregation on the data.

        Note: The initial value for the reduce operation within each window is the first element of that window, and
        subsequent elements are combined using the reduce function.
        """
        name = self._get_name(func_name="reduce")

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end)

            if current_value is None:
                updated_value = value
            else:
                updated_value = reduce_func(current_value, value)

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=name,
            func=func,
            dataframe=self._dataframe,
        )

    def max(self) -> "TumblingWindow":
        """
        Configure the tumbling window to find the maximum value within each window period.

        :return: TumblingWindow instance configured to find the maximum value within each window period.
        """
        name = self._get_name(func_name="max")

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end)

            if current_value is None:
                updated_value = value
            else:
                updated_value = max(current_value, value)

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=name,
            func=func,
            dataframe=self._dataframe,
        )

    def min(self) -> "TumblingWindow":
        """
        Configure the tumbling window to find the minimum value within each window period.

        :return: TumblingWindow instance configured to find the minimum value within each window period.
        """
        name = self._get_name(func_name="min")

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end)

            if current_value is None:
                updated_value = value
            else:
                updated_value = min(current_value, value)

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=name,
            func=func,
            dataframe=self._dataframe,
        )


class TumblingWindow:
    StreamingDataFrame = TypeVar("StreamingDataFrame")

    def __init__(
        self,
        duration: float,
        grace: float,
        name: str,
        func: DataFrameWindowFunc,
        dataframe: StreamingDataFrame,
    ):
        self._duration = duration
        self._grace = grace
        self._name = name
        self._func = func
        self._dataframe = dataframe

        if self._duration < 0:
            raise ValueError(f"Window duration must be positive, got {self._duration}")

        if self._grace < 0:
            raise ValueError(f"Window grace must be positive, got {self._grace}")

        if self._name is None:
            raise ValueError("Window name must not be empty")

    def _stale(self, timestamp: float, latest_timestamp: float) -> bool:
        return timestamp + self._grace <= latest_timestamp

    def _process_window(
        self, value, state: WindowedTransactionState, timestamp: float
    ) -> (list[WindowResult], list[WindowResult]):
        latest_timestamp = state.get_latest_timestamp() or 0

        if self._stale(timestamp=timestamp, latest_timestamp=latest_timestamp):
            # The timestamp is out-of-order and shouldn't be processed
            # But the windows can be closed anyway
            expired_windows = []  # state.get_expired_windows(timestamp)
            return [], expired_windows

        start, end = get_window_range(timestamp, self._duration)

        updated_window = WindowResult(
            value=self._func(start, end, timestamp, value, state), start=start, end=end
        )
        expired_windows = []  # state.get_expired_windows(timestamp)

        return [updated_window], expired_windows

    def latest(self) -> StreamingDataFrame:
        """
        Apply the window transformation to the StreamingDataFrame to return the latest result from the latest window.

        This method processes streaming data and returns the most recent value from the latest window.
        It is useful when you need the latest aggregated result up to the current moment in a streaming data context.
        """
        return self._dataframe.apply_window(
            lambda value, state, process_window=self._process_window: process_window(
                value=value,
                state=state,
                timestamp=message_context().timestamp.milliseconds / 1000,
            )[0][-1],
            name=self._name,
        )

    def final(self) -> StreamingDataFrame:
        """
        Apply the window transformation to the StreamingDataFrame to return the results when a window closes.

        This method processes streaming data and returns results at the closure of each window.
        It's ideal for scenarios where you need the aggregated results after the complete data for a window is received.
        """
        return self._dataframe.apply_window(
            lambda value, state, process_window=self._process_window: process_window(
                value=value,
                state=state,
                timestamp=message_context().timestamp.milliseconds / 1000,
            )[1],
            expand=True,
            name=self._name,
        )

    def all(self) -> StreamingDataFrame:
        """
        Apply the window transformation to the StreamingDataFrame to return results for each window update.

        This method processes streaming data and returns results for every update within each window,
        regardless of whether the window is closed or not.
        It's suitable for scenarios where you need continuous feedback the aggregated data throughout the window's
        duration.
        """
        return self._dataframe.apply_window(
            lambda value, state, process_window=self._process_window: process_window(
                value=value,
                state=state,
                timestamp=message_context().timestamp.milliseconds / 1000,
            )[0],
            name=self._name,
        )
