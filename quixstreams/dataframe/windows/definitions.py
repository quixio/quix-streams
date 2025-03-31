import abc
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from .aggregations import (
    BaseAggregator,
    BaseCollector,
    Collect,
    Count,
    Max,
    Mean,
    Min,
    Reduce,
    Sum,
)
from .base import (
    Window,
    WindowOnLateCallback,
)
from .count_based import (
    CountWindow,
    CountWindowMultiAggregation,
    CountWindowSingleAggregation,
)
from .sliding import (
    SlidingWindow,
    SlidingWindowMultiAggregation,
    SlidingWindowSingleAggregation,
)
from .time_based import (
    TimeWindow,
    TimeWindowMultiAggregation,
    TimeWindowSingleAggregation,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = [
    "TumblingCountWindowDefinition",
    "HoppingCountWindowDefinition",
    "SlidingCountWindowDefinition",
    "HoppingTimeWindowDefinition",
    "SlidingTimeWindowDefinition",
    "TumblingTimeWindowDefinition",
]


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
        func_name: Optional[str],
        aggregators: Optional[dict[str, BaseAggregator]] = None,
        collectors: Optional[dict[str, BaseCollector]] = None,
    ) -> Window: ...

    def sum(self) -> "Window":
        """
        Configure the window to aggregate data by summing up values within
        each window period.

        :return: an instance of `FixedTimeWindow` configured to perform sum aggregation.
        """

        return self._create_window(
            func_name="sum",
            aggregators={"value": Sum(column=None)},
        )

    def count(self) -> "Window":
        """
        Configure the window to aggregate data by counting the number of values
        within each window period.

        :return: an instance of `FixedTimeWindow` configured to perform record count.
        """

        return self._create_window(
            func_name="count",
            aggregators={"value": Count()},
        )

    def mean(self) -> "Window":
        """
        Configure the window to aggregate data by calculating the mean of the values
        within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the mean
            of the values.
        """

        return self._create_window(
            func_name="mean",
            aggregators={"value": Mean(column=None)},
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

        return self._create_window(
            func_name="reduce",
            aggregators={"value": Reduce(reducer=reducer, initializer=initializer)},
        )

    def max(self) -> "Window":
        """
        Configure a window to aggregate the maximum value within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the maximum
            value within each window period.
        """

        return self._create_window(
            func_name="max",
            aggregators={"value": Max(column=None)},
        )

    def min(self) -> "Window":
        """
        Configure a window to aggregate the minimum value within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the maximum
            value within each window period.
        """

        return self._create_window(
            func_name="min",
            aggregators={"value": Min(column=None)},
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
        window = sdf.tumbling_window(duration_ms=1000).collect()
        # Each window will contain a list of all values that occurred
        # within that second
        ```

        :return: an instance of `FixedTimeWindow` configured to collect all values
            within each window period.
        """

        return self._create_window(
            func_name="collect",
            collectors={"value": Collect(column=None)},
        )

    def agg(self, **operations: Union[BaseAggregator, BaseCollector]) -> "Window":
        if "start" in operations or "end" in operations:
            raise ValueError(
                "`start` and `end` are reserved keywords for the window boundaries"
            )

        aggregators: dict[str, BaseAggregator] = {}
        collectors: dict[str, BaseCollector] = {}

        for column, op in operations.items():
            if isinstance(op, BaseAggregator):
                aggregators[column] = op
            elif isinstance(op, BaseCollector):
                collectors[column] = op
            else:
                raise TypeError(
                    f"operation `{column}:{op}` must be either BaseAggregator or BaseCollector"
                )

        return self._create_window(
            func_name=None,
            aggregators=aggregators,
            collectors=collectors,
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

    def _get_name(self, func_name: Optional[str]) -> str:
        prefix = f"{self._name}_hopping_window" if self._name else "hopping_window"
        if func_name:
            return f"{prefix}_{self._duration_ms}_{self._step_ms}_{func_name}"
        else:
            return f"{prefix}_{self._duration_ms}_{self._step_ms}"

    def _create_window(
        self,
        func_name: Optional[str],
        aggregators: Optional[dict[str, BaseAggregator]] = None,
        collectors: Optional[dict[str, BaseCollector]] = None,
    ) -> TimeWindow:
        if func_name:
            window_type: Union[
                type[TimeWindowSingleAggregation], type[TimeWindowMultiAggregation]
            ] = TimeWindowSingleAggregation
        else:
            window_type = TimeWindowMultiAggregation

        return window_type(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            step_ms=self._step_ms,
            name=self._get_name(func_name=func_name),
            dataframe=self._dataframe,
            aggregators=aggregators or {},
            collectors=collectors or {},
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

    def _get_name(self, func_name: Optional[str]) -> str:
        prefix = f"{self._name}_tumbling_window" if self._name else "tumbling_window"
        if func_name:
            return f"{prefix}_{self._duration_ms}_{func_name}"
        else:
            return f"{prefix}_{self._duration_ms}"

    def _create_window(
        self,
        func_name: Optional[str],
        aggregators: Optional[dict[str, BaseAggregator]] = None,
        collectors: Optional[dict[str, BaseCollector]] = None,
    ) -> TimeWindow:
        if func_name:
            window_type: Union[
                type[TimeWindowSingleAggregation], type[TimeWindowMultiAggregation]
            ] = TimeWindowSingleAggregation
        else:
            window_type = TimeWindowMultiAggregation

        return window_type(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            name=self._get_name(func_name=func_name),
            dataframe=self._dataframe,
            aggregators=aggregators or {},
            collectors=collectors or {},
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

    def _get_name(self, func_name: Optional[str]) -> str:
        prefix = f"{self._name}_sliding_window" if self._name else "sliding_window"
        if func_name:
            return f"{prefix}_{self._duration_ms}_{func_name}"
        else:
            return f"{prefix}_{self._duration_ms}"

    def _create_window(
        self,
        func_name: Optional[str],
        aggregators: Optional[dict[str, BaseAggregator]] = None,
        collectors: Optional[dict[str, BaseCollector]] = None,
    ) -> SlidingWindow:
        if func_name:
            window_type: Union[
                type[SlidingWindowSingleAggregation],
                type[SlidingWindowMultiAggregation],
            ] = SlidingWindowSingleAggregation
        else:
            window_type = SlidingWindowMultiAggregation

        return window_type(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            name=self._get_name(func_name=func_name),
            dataframe=self._dataframe,
            aggregators=aggregators or {},
            collectors=collectors or {},
            on_late=self._on_late,
        )


class CountWindowDefinition(WindowDefinition):
    def __init__(
        self, count: int, dataframe: "StreamingDataFrame", name: Optional[str] = None
    ) -> None:
        super().__init__(name, dataframe)

        if count < 2:
            raise ValueError("Window count must be greater than 1")

        self._count = count


class TumblingCountWindowDefinition(CountWindowDefinition):
    def _create_window(
        self,
        func_name: Optional[str],
        aggregators: Optional[dict[str, BaseAggregator]] = None,
        collectors: Optional[dict[str, BaseCollector]] = None,
    ) -> CountWindow:
        if func_name:
            window_type: Union[
                type[CountWindowSingleAggregation], type[CountWindowMultiAggregation]
            ] = CountWindowSingleAggregation
        else:
            window_type = CountWindowMultiAggregation

        return window_type(
            name=self._get_name(func_name=func_name),
            count=self._count,
            aggregators=aggregators or {},
            collectors=collectors or {},
            dataframe=self._dataframe,
        )

    def _get_name(self, func_name: Optional[str]) -> str:
        prefix = (
            f"{self._name}_tumbling_count_window"
            if self._name
            else "tumbling_count_window"
        )
        if func_name:
            return f"{prefix}_{func_name}"
        else:
            return prefix


class HoppingCountWindowDefinition(CountWindowDefinition):
    def __init__(
        self,
        count: int,
        dataframe: "StreamingDataFrame",
        step: int = 1,
        name: Optional[str] = None,
    ):
        super().__init__(count=count, dataframe=dataframe, name=name)

        if step < 1:
            raise ValueError("Window step must be greater or equal to 1")

        self._step = step

    def _create_window(
        self,
        func_name: Optional[str],
        aggregators: Optional[dict[str, BaseAggregator]] = None,
        collectors: Optional[dict[str, BaseCollector]] = None,
    ) -> CountWindow:
        if func_name:
            window_type: Union[
                type[CountWindowSingleAggregation], type[CountWindowMultiAggregation]
            ] = CountWindowSingleAggregation
        else:
            window_type = CountWindowMultiAggregation

        return window_type(
            name=self._get_name(func_name=func_name),
            count=self._count,
            aggregators=aggregators or {},
            collectors=collectors or {},
            dataframe=self._dataframe,
            step=self._step,
        )

    def _get_name(self, func_name: Optional[str]) -> str:
        prefix = (
            f"{self._name}_hopping_count_window"
            if self._name
            else "hopping_count_window"
        )
        if func_name:
            return f"{prefix}_{func_name}"
        return prefix


class SlidingCountWindowDefinition(HoppingCountWindowDefinition):
    def __init__(
        self, count: int, dataframe: "StreamingDataFrame", name: Optional[str] = None
    ):
        super().__init__(count=count, dataframe=dataframe, step=1, name=name)

    def _get_name(self, func_name: Optional[str]) -> str:
        prefix = (
            f"{self._name}_sliding_count_window"
            if self._name
            else "sliding_count_window"
        )
        if func_name:
            return f"{prefix}_{func_name}"
        return prefix
