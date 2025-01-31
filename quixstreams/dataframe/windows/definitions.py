import abc
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple

from .base import (
    Window,
    WindowAggregateFunc,
    WindowMergeFunc,
    WindowOnLateCallback,
)
from .sliding import SlidingWindow
from .time_based import FixedTimeWindow

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


def _mean_merge_func(state_value: Tuple[float, int]):
    sum_, count_ = state_value
    return sum_ / count_


class WindowDefinition(abc.ABC):
    def __init__(
        self,
        name: Optional[str],
        dataframe: "StreamingDataFrame",
        on_late: Optional[WindowOnLateCallback] = None,
    ) -> None:
        super().__init__()

        self._name = name
        self._on_late = on_late
        self._dataframe = dataframe

    @abstractmethod
    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool = False,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> Window: ...

    def sum(self) -> "Window":
        """
        Configure the window to aggregate data by summing up values within
        each window period.

        :return: an instance of `FixedTimeWindow` configured to perform sum aggregation.
        """

        def func(old: Any, new: Any) -> Any:
            return old + new

        return self._create_window(
            func_name="sum", aggregate_func=func, aggregate_default=0
        )

    def count(self) -> "Window":
        """
        Configure the window to aggregate data by counting the number of values
        within each window period.

        :return: an instance of `FixedTimeWindow` configured to perform record count.
        """

        def func(old: Any, _: Any) -> Any:
            return old + 1

        return self._create_window(
            func_name="count", aggregate_func=func, aggregate_default=0
        )

    def mean(self) -> "Window":
        """
        Configure the window to aggregate data by calculating the mean of the values
        within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the mean
            of the values.
        """

        def func(old: Any, new: Any) -> Any:
            sum_, count_ = old
            return sum_ + new, count_ + 1

        return self._create_window(
            func_name="mean",
            aggregate_func=func,
            merge_func=_mean_merge_func,
            aggregate_default=(0.0, 0),
        )

    def reduce(
        self, reducer: Callable[[Any, Any], Any], initializer: Callable[[Any], Any]
    ) -> "Window":
        """
        Configure the window to perform a custom aggregation using `reducer`
        and `initializer` functions.

        Example Snippet:
        ```python
        sdf = StreamingDataFrame(...)

        # Using "reduce()" to calculate multiple aggregates at once
        def reducer(agg: dict, current: int):
            aggregated = {
                'min': min(agg['min'], current),
                'max': max(agg['max'], current),
                'count': agg['count'] + 1
            }
            return aggregated

        def initializer(current) -> dict:
            return {'min': current, 'max': current, 'count': 1}

        window = (
            sdf.tumbling_window(duration_ms=1000)
            .reduce(reducer=reducer, initializer=initializer)
            .final()
        )
        ```

        :param reducer: A function that takes two arguments
            (the accumulated value and a new value) and returns a single value.
            The returned value will be saved to the state store and sent downstream.
        :param initializer: A function to call for every first element of the window.
            This function is used to initialize the aggregation within a window.

        :return: A window configured to perform custom reduce aggregation on the data.
        """

        def func(old: Any, new: Any) -> Any:
            return initializer(new) if old is None else reducer(old, new)

        return self._create_window(
            func_name="reduce", aggregate_func=func, aggregate_default=None
        )

    def max(self) -> "Window":
        """
        Configure a window to aggregate the maximum value within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the maximum
            value within each window period.
        """

        def func(old: Any, new: Any) -> Any:
            return new if old is None else max(old, new)

        return self._create_window(
            func_name="max", aggregate_func=func, aggregate_default=None
        )

    def min(self) -> "Window":
        """
        Configure a window to aggregate the minimum value within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the maximum
            value within each window period.
        """

        def func(old: Any, new: Any) -> Any:
            return new if old is None else min(old, new)

        return self._create_window(
            func_name="min", aggregate_func=func, aggregate_default=None
        )

    def collect(self) -> "Window":
        """
        Configure the window to collect all values within each window period into a
        list, without performing any aggregation.

        This method is useful when you need to gather all raw values that fall
        within a window period for further processing or analysis.

        Example Snippet:
        ```python
        # Collect all values in 1-second windows
        window = df.tumbling_window(duration_ms=1000).collect()
        # Each window will contain a list of all values that occurred
        # within that second
        ```

        :return: an instance of `FixedTimeWindow` configured to collect all values
            within each window period.
        """

        def func(old: Any, new: Any) -> None:
            return None

        return self._create_window(
            func_name="collect",
            aggregate_func=func,
            aggregate_default=None,
            aggregate_collection=True,
        )


class TimeWindowDefinition(WindowDefinition):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
        step_ms: Optional[int] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        if not isinstance(duration_ms, int):
            raise TypeError("Window size must be an integer")
        if duration_ms < 1:
            raise ValueError("Window size cannot be smaller than 1ms")
        if grace_ms < 0:
            raise ValueError("Window grace cannot be smaller than 0ms")

        if step_ms is not None and (step_ms <= 0 or step_ms >= duration_ms):
            raise ValueError(
                f"Window step size must be smaller than duration and bigger than 0ms, "
                f"got {step_ms}ms"
            )

        super().__init__(name, dataframe, on_late)

        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
        self._step_ms = step_ms

    @property
    def duration_ms(self) -> int:
        return self._duration_ms

    @property
    def grace_ms(self) -> int:
        return self._grace_ms

    @property
    def step_ms(self) -> Optional[int]:
        return self._step_ms


class HoppingTimeWindowDefinition(TimeWindowDefinition):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        step_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=dataframe,
            name=name,
            step_ms=step_ms,
            on_late=on_late,
        )

    def _get_name(self, func_name: str) -> str:
        prefix = f"{self._name}_hopping_window" if self._name else "hopping_window"
        return f"{prefix}_{self._duration_ms}_{self._step_ms}_{func_name}"

    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool = False,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> FixedTimeWindow:
        return FixedTimeWindow(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            step_ms=self._step_ms,
            name=self._get_name(func_name=func_name),
            dataframe=self._dataframe,
            aggregate_func=aggregate_func,
            aggregate_default=aggregate_default,
            aggregate_collection=aggregate_collection,
            merge_func=merge_func,
            on_late=self._on_late,
        )


class TumblingTimeWindowDefinition(TimeWindowDefinition):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=dataframe,
            name=name,
            on_late=on_late,
        )

    def _get_name(self, func_name: str) -> str:
        prefix = f"{self._name}_tumbling_window" if self._name else "tumbling_window"
        return f"{prefix}_{self._duration_ms}_{func_name}"

    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool = False,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> FixedTimeWindow:
        return FixedTimeWindow(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            name=self._get_name(func_name=func_name),
            dataframe=self._dataframe,
            aggregate_func=aggregate_func,
            aggregate_default=aggregate_default,
            aggregate_collection=aggregate_collection,
            merge_func=merge_func,
            on_late=self._on_late,
        )


class SlidingTimeWindowDefinition(TimeWindowDefinition):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=dataframe,
            name=name,
            on_late=on_late,
        )

    def _get_name(self, func_name: str) -> str:
        prefix = f"{self._name}_sliding_window" if self._name else "sliding_window"
        return f"{prefix}_{self._duration_ms}_{func_name}"

    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool = False,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> SlidingWindow:
        return SlidingWindow(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            name=self._get_name(func_name=func_name),
            dataframe=self._dataframe,
            aggregate_func=aggregate_func,
            aggregate_default=aggregate_default,
            aggregate_collection=aggregate_collection,
            merge_func=merge_func,
            on_late=self._on_late,
        )
