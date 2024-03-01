import abc
from abc import abstractmethod
from typing import Any, Optional, Callable, Tuple, TYPE_CHECKING

from quixstreams.state import (
    WindowedState,
)
from .base import WindowAggregateFunc, WindowMergeFunc
from .time_based import FixedTimeWindow

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


def _mean_merge_func(state_value: Tuple[float, int]):
    sum_, count_ = state_value
    return sum_ / count_


class FixedTimeWindowDefinition(abc.ABC):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
        step_ms: Optional[int] = None,
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
        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
        self._dataframe = dataframe
        self._name = name
        self._step_ms = step_ms

    @abstractmethod
    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> "FixedTimeWindow":
        ...

    @property
    def duration_ms(self) -> int:
        return self._duration_ms

    @property
    def grace_ms(self) -> int:
        return self._grace_ms

    @property
    def step_ms(self) -> Optional[int]:
        return self._step_ms

    def sum(self) -> "FixedTimeWindow":
        """
        Configure the window to aggregate data by summing up values within
        each window period.

        :return: an instance of `FixedTimeWindow` configured to perform sum aggregation.
        """

        def func(
            start_ms: int,
            end_ms: int,
            timestamp_ms: int,
            value: Any,
            state: WindowedState,
        ):
            current_value = state.get_window(
                start_ms=start_ms, end_ms=end_ms, default=0
            )
            updated_value = current_value + value

            state.update_window(
                start_ms, end_ms, timestamp_ms=timestamp_ms, value=updated_value
            )
            return updated_value

        return self._create_window(func_name="sum", aggregate_func=func)

    def count(self) -> "FixedTimeWindow":
        """
        Configure the window to aggregate data by counting the number of values
        within each window period.

        :return: an instance of `FixedTimeWindow` configured to perform record count.
        """

        def func(
            start_ms: int,
            end_ms: int,
            timestamp_ms: int,
            _: Any,
            state: WindowedState,
        ):
            current_value = state.get_window(
                start_ms=start_ms, end_ms=end_ms, default=0
            )
            updated_value = current_value + 1

            state.update_window(
                start_ms, end_ms, timestamp_ms=timestamp_ms, value=updated_value
            )
            return updated_value

        return self._create_window(func_name="count", aggregate_func=func)

    def mean(self) -> "FixedTimeWindow":
        """
        Configure the window to aggregate data by calculating the mean of the values
        within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the mean
            of the values.
        """

        def func(
            start_ms: int,
            end_ms: int,
            timestamp_ms: int,
            value: Any,
            state: WindowedState,
        ):
            sum_, count_ = state.get_window(
                start_ms=start_ms, end_ms=end_ms, default=(0.0, 0)
            )

            sum_ += +value
            count_ += +1
            state.update_window(
                start_ms, end_ms, timestamp_ms=timestamp_ms, value=(sum_, count_)
            )
            return sum_, count_

        return self._create_window(
            func_name="mean", aggregate_func=func, merge_func=_mean_merge_func
        )

    def reduce(
        self, reducer: Callable[[Any, Any], Any], initializer: Callable[[Any], Any]
    ) -> "FixedTimeWindow":
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
                'max': max(agg['max'], current)
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

        def func(
            start_ms: int,
            end_ms: int,
            timestamp_ms: int,
            value: Any,
            state: WindowedState,
        ):
            current_value = state.get_window(start_ms=start_ms, end_ms=end_ms)

            if current_value is None:
                updated_value = initializer(value)
            else:
                updated_value = reducer(current_value, value)

            state.update_window(
                start_ms, end_ms, timestamp_ms=timestamp_ms, value=updated_value
            )
            return updated_value

        return self._create_window(func_name="reduce", aggregate_func=func)

    def max(self) -> "FixedTimeWindow":
        """
        Configure a window to aggregate the maximum value within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the maximum
            value within each window period.
        """

        def func(
            start_ms: int,
            end_ms: int,
            timestamp_ms: int,
            value: Any,
            state: WindowedState,
        ):
            current_value = state.get_window(start_ms=start_ms, end_ms=end_ms)

            if current_value is None:
                updated_value = value
            else:
                updated_value = max(current_value, value)

            state.update_window(
                start_ms, end_ms, timestamp_ms=timestamp_ms, value=updated_value
            )
            return updated_value

        return self._create_window(func_name="max", aggregate_func=func)

    def min(self) -> "FixedTimeWindow":
        """
        Configure a window to aggregate the minimum value within each window period.

        :return: an instance of `FixedTimeWindow` configured to calculate the maximum
            value within each window period.
        """

        def func(
            start_ms: int,
            end_ms: int,
            timestamp_ms: int,
            value: Any,
            state: WindowedState,
        ):
            current_value = state.get_window(start_ms=start_ms, end_ms=end_ms)

            if current_value is None:
                updated_value = value
            else:
                updated_value = min(current_value, value)

            state.update_window(
                start_ms, end_ms, timestamp_ms=timestamp_ms, value=updated_value
            )
            return updated_value

        return self._create_window(func_name="min", aggregate_func=func)


class HoppingWindowDefinition(FixedTimeWindowDefinition):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        step_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
    ):
        super().__init__(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=dataframe,
            name=name,
            step_ms=step_ms,
        )

    def _get_name(self, func_name: str) -> str:
        return (
            self._name
            or f"hopping_window_{self._duration_ms}_{self._step_ms}_{func_name}"
        )

    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> "FixedTimeWindow":
        return FixedTimeWindow(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            step_ms=self._step_ms,
            name=self._get_name(func_name=func_name),
            aggregate_func=aggregate_func,
            merge_func=merge_func,
            dataframe=self._dataframe,
        )


class TumblingWindowDefinition(FixedTimeWindowDefinition):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        dataframe: "StreamingDataFrame",
        name: Optional[str] = None,
    ):
        super().__init__(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=dataframe, name=name
        )

    def _get_name(self, func_name: str) -> str:
        return self._name or f"tumbling_window_{self._duration_ms}_{func_name}"

    def _create_window(
        self,
        func_name: str,
        aggregate_func: WindowAggregateFunc,
        merge_func: Optional[WindowMergeFunc] = None,
    ) -> "FixedTimeWindow":
        return FixedTimeWindow(
            duration_ms=self._duration_ms,
            grace_ms=self._grace_ms,
            name=self._get_name(func_name=func_name),
            aggregate_func=aggregate_func,
            merge_func=merge_func,
            dataframe=self._dataframe,
        )
