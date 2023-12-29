import functools
from abc import ABC, abstractmethod
from typing_extensions import TypedDict
from typing import Any, Optional, Callable, TypeVar, Tuple, List

from quixstreams.state import StateStoreManager
from quixstreams.state.rocksdb.windowed.store import WindowedTransactionState
from quixstreams.context import (
    message_context,
    message_key,
)


class WindowResult(TypedDict):
    start: float
    end: float
    value: Any


T = TypeVar("T")
WindowAggregateFunc = Callable[[float, float, float, T, WindowedTransactionState], Any]
WindowedDataFrameFunc = Callable[
    [T, WindowedTransactionState, Optional[Callable]],
    Tuple[List[WindowResult], List[WindowResult]],
]
StreamingDataFrame = TypeVar("StreamingDataFrame")


def get_window_ranges(
    timestamp: float, window_duration: float, step: Optional[float] = None
) -> List[Tuple[float, float]]:
    if not step:
        step = window_duration

    window_ranges = []
    current_window_start = timestamp - (timestamp % step)

    while (
        current_window_start > timestamp - window_duration and current_window_start >= 0
    ):
        window_end = current_window_start + window_duration
        window_ranges.insert(0, (current_window_start, window_end))
        current_window_start -= step

    return window_ranges


class FixedWindowDefinition(ABC):
    def __init__(
        self,
        duration: float,
        grace: float,
        dataframe: StreamingDataFrame,
        name: Optional[str] = None,
        step: Optional[float] = None,
    ):
        self._duration = duration
        self._grace = grace
        self._dataframe = dataframe
        self._name = name
        self._step = step

        if self._duration < 0:
            raise ValueError(f"Window duration must be positive, got {self._duration}")

        if self._grace < 0:
            raise ValueError(f"Window grace must be positive, got {self._grace}")

        if self._step is not None and (self._step <= 0 or self._step >= duration):
            raise ValueError(
                f"Window step size must be smaller than duration and bigger than zero, got {self._step}"
            )

    @abstractmethod
    def _create_window(
        self, func_name: str, func: WindowAggregateFunc
    ) -> "FixedWindow":
        ...

    def sum(self) -> "FixedWindow":
        """
        Configure the window to aggregate data by summing up values within each window period.

        :return: A window configured to perform sum aggregation.
        """

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end) or 0
            updated_value = current_value + value

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return self._create_window(func_name="sum", func=func)

    def count(self) -> "FixedWindow":
        """
        Configure the window to aggregate data by counting the number of records within each window period.

        :return: A window configured to perform record count.
        """

        def func(start, end, timestamp, _: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end) or 0
            updated_value = current_value + 1

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return self._create_window(func_name="count", func=func)

    def mean(self) -> "FixedWindow":
        """
        Configure the window to aggregate data by calculating the mean of the values within each window period.

        :return: A window configured to calculate the mean of the data values.
        """

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_window_value = state.get_window(start=start, end=end) or (0.0, 0)

            agg = current_window_value[0] + value
            count = current_window_value[1] + 1

            updated_window_value = (agg, count)

            state.update_window(
                start, end, timestamp=timestamp, value=updated_window_value
            )
            return agg / count

        return self._create_window(func_name="mean", func=func)

    def reduce(
        self, reduce_func: Callable[[Any, Any], T], initializer: Callable[[Any], T]
    ) -> "FixedWindow":
        """
        Configure the window to perform a custom aggregation using a user-provided reduce function.

        :param reduce_func: A function that takes two arguments (the accumulated value and a new value)
            and returns a single value. This function is used for aggregating data within the window.
        :param initializer: A function to call for every first element of the window. This function is used to
            initialize the value of the first element of the window.

        :return: A window configured to perform custom reduce aggregation on the data.
        """

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end)

            if current_value is None:
                updated_value = initializer(value)
            else:
                updated_value = reduce_func(current_value, value)

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return self._create_window(func_name="reduce", func=func)

    def max(self) -> "FixedWindow":
        """
        Configure the window to find the maximum value within each window period.

        :return: A window instance configured to find the maximum value within each window period.
        """

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end)

            if current_value is None:
                updated_value = value
            else:
                updated_value = max(current_value, value)

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return self._create_window(func_name="max", func=func)

    def min(self) -> "FixedWindow":
        """
        Configure the window to find the minimum value within each window period.

        :return: A window instance configured to find the minimum value within each window period.
        """

        def func(start, end, timestamp, value: Any, state: WindowedTransactionState):
            current_value = state.get_window(start=start, end=end)

            if current_value is None:
                updated_value = value
            else:
                updated_value = min(current_value, value)

            state.update_window(start, end, timestamp=timestamp, value=updated_value)
            return updated_value

        return self._create_window(func_name="min", func=func)


class FixedWindow(ABC):
    def __init__(
        self,
        duration: float,
        grace: float,
        name: str,
        func: WindowAggregateFunc,
        dataframe: StreamingDataFrame,
        step: Optional[float] = None,
    ):
        self._duration = duration
        self._grace = grace
        self._name = name
        self._func = func
        self._dataframe = dataframe
        self._step = step

        if self._name is None:
            raise ValueError("Window name must not be empty")

    def _stale(self, window_end: float, latest_timestamp: float) -> bool:
        return latest_timestamp > window_end + self._grace

    def _process_window(
        self, value, state: WindowedTransactionState, timestamp: float
    ) -> (List[WindowResult], List[WindowResult]):
        latest_timestamp = state.get_latest_timestamp() or 0
        ranges = get_window_ranges(
            timestamp=timestamp, window_duration=self._duration, step=self._step
        )

        updated_windows = []
        for start, end in ranges:
            if not self._stale(window_end=end, latest_timestamp=latest_timestamp):
                updated_windows.append(
                    WindowResult(
                        value=self._func(start, end, timestamp, value, state),
                        start=start,
                        end=end,
                    )
                )

        expired_windows = []  # state.get_expired_windows(timestamp)

        return updated_windows, expired_windows

    def latest(self) -> StreamingDataFrame:
        """
        Apply the window transformation to the StreamingDataFrame to return the latest result from the latest window.

        This method processes streaming data and returns the most recent value from the latest window.
        It is useful when you need the latest aggregated result up to the current moment in a streaming data context.
        """
        return self._apply_window(
            lambda value, state, process_window=self._process_window: process_window(
                value=value,
                state=state,
                timestamp=message_context().timestamp.milliseconds / 1000,
            )[0][-1],
            name=self._name,
        )

    def final(self, expand: bool = True) -> StreamingDataFrame:
        """
        Apply the window transformation to the StreamingDataFrame to return the results when a window closes.

        This method processes streaming data and returns results at the closure of each window.
        It's ideal for scenarios where you need the aggregated results after the complete data for a window is received.

        :param expand: if True, expand the returned iterable into individual values
            downstream. If returned value is not iterable, `TypeError` will be raised.
            Default - `True`
        """
        return self._apply_window(
            lambda value, state, process_window=self._process_window: process_window(
                value=value,
                state=state,
                timestamp=message_context().timestamp.milliseconds / 1000,
            )[1],
            expand=expand,
            name=self._name,
        )

    def all(self, expand: bool = True) -> StreamingDataFrame:
        """
        Apply the window transformation to the StreamingDataFrame to return results for each window update.

        This method processes streaming data and returns results for every update within each window,
        regardless of whether the window is closed or not.
        It's suitable for scenarios where you need continuous feedback the aggregated data throughout the window's
        duration.

        :param expand: if True, expand the returned iterable into individual values
            downstream. If returned value is not iterable, `TypeError` will be raised.
            Default - `True`.
        """
        return self._apply_window(
            lambda value, state, process_window=self._process_window: process_window(
                value=value,
                state=state,
                timestamp=message_context().timestamp.seconds,
            )[0],
            expand=expand,
            name=self._name,
        )

    def _apply_window(
        self,
        func: WindowedDataFrameFunc,
        name: str,
        expand: bool = False,
    ) -> StreamingDataFrame:
        self._dataframe.state_manager.register_windowed_store(
            topic_name=self._dataframe.topic.name, store_name=name
        )

        func = _as_window(
            func=func, state_manager=self._dataframe.state_manager, store_name=name
        )

        return self._dataframe.apply(func=func, expand=expand)


def _as_window(
    func: WindowedDataFrameFunc, state_manager: StateStoreManager, store_name: str
) -> WindowAggregateFunc:
    @functools.wraps(func)
    def wrapper(value: object) -> object:
        transaction = state_manager.get_store_transaction(store_name=store_name)
        key = message_key()
        # Prefix all the state keys by the message key
        with transaction.with_prefix(prefix=key):
            # Pass a State object with an interface limited to the key updates only
            return func(value, transaction.state)

    return wrapper
