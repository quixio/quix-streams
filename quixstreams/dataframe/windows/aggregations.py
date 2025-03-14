from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Any,
    Callable,
    Final,
    Generic,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

from typing_extensions import TypeAlias

__all__ = [
    "Collect",
    "Count",
    "Max",
    "Mean",
    "Min",
    "Reduce",
    "Sum",
]


class _ROOT(Enum):
    ROOT = 1


ROOT: Final = _ROOT.ROOT
Column: TypeAlias = Union[str, _ROOT]

S = TypeVar("S")


class BaseAggregator(ABC, Generic[S]):
    """
    Base class for window aggregation.

    Subclass it to implement custom aggregations.

    An Aggregator reduce incoming items into a single value or group of values. When the window
    is closed the aggregator produce a result based on the reduced value.

    To store all incoming items without reducing them use a `Collector`.
    """

    @property
    @abstractmethod
    def state_suffix(self) -> str:
        """
        The state suffix used to store the aggregation state in the window.

        The complete state key is built using the result column name and this suffix. If any of these
        values change the state key will change and the aggregation state restart from zero.

        Aggregations should change the state suffix when there parameters change to avoid
        conflicts with previous state values.
        """
        ...

    @abstractmethod
    def initialize(self) -> S:
        """
        This method is triggered once to build the aggregation starting value.
        It should return the initial value for the aggregation.
        """
        ...

    @abstractmethod
    def agg(self, old: S, new: Any) -> S:
        """
        This method is trigged when a window is updated with a new value.
        It should return the updated aggregated value.
        """
        ...

    @abstractmethod
    def result(self, value: S) -> Any:
        """
        This method is triggered when a window is closed.
        It should return the final aggregation result.
        """
        ...


class Aggregator(BaseAggregator):
    """
    Implementation of the `BaseAggregator` interface.

    Provides default implementations for the `state_suffix` property.
    """

    @property
    def state_suffix(self) -> str:
        return self.__class__.__name__


class Count(Aggregator):
    """
    Use `Count()` to aggregate the total number of events  within each window period..
    """

    def initialize(self) -> int:
        return 0

    def agg(self, old: int, new: Any) -> int:
        return old + 1

    def result(self, value: int) -> int:
        return value


V = TypeVar("V", int, float)


class Sum(Aggregator):
    """
    Use `Sum()` to aggregate the sum of the events, or a column of the events, within each window period.

    :param column: The column to sum. Use `ROOT` to sum the whole message.
        Default - `ROOT`
    """

    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

    @property
    def state_suffix(self) -> str:
        if self.column is ROOT:
            return self.__class__.__name__
        return f"{self.__class__.__name__}/{self.column}"

    def initialize(self) -> int:
        return 0

    def agg(self, old: V, new: Any) -> V:
        new = new if self.column is ROOT else new.get(self.column)
        return old + (new or 0)

    def result(self, value: V) -> V:
        return value


class Mean(Aggregator):
    """
    Use `Mean()` to aggregate the mean of the events, or a column of the events, within each window period.

    :param column: The column to mean. Use `ROOT` to mean the whole message.
        Default - `ROOT`
    """

    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

    @property
    def state_suffix(self) -> str:
        if self.column is ROOT:
            return self.__class__.__name__
        return f"{self.__class__.__name__}/{self.column}"

    def initialize(self) -> tuple[float, int]:
        return 0.0, 0

    def agg(self, old: tuple[V, int], new: Any) -> tuple[V, int]:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old

        old_sum, old_count = old
        return old_sum + new, old_count + 1

    def result(self, value: tuple[Union[int, float], int]) -> Optional[float]:
        sum_, count_ = value
        if count_ == 0:
            return None
        return sum_ / count_


class Max(Aggregator):
    """
    Use `Max()` to aggregate the max of the events, or a column of the events, within each window period.

    :param column: The column to max. Use `ROOT` to max the whole message.
        Default - `ROOT`
    """

    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

    @property
    def state_suffix(self) -> str:
        if self.column is ROOT:
            return self.__class__.__name__
        return f"{self.__class__.__name__}/{self.column}"

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[V], new: Any) -> V:
        new = new if self.column is ROOT else new.get(self.column)
        if old is None:
            return new
        elif new is None:
            return old
        return max(old, new)

    def result(self, value: V) -> V:
        return value


class Min(Aggregator):
    """
    Use `Min()` to aggregate the min of the events, or a column of the events, within each window period.

    :param column: The column to min. Use `ROOT` to min the whole message.
        Default - `ROOT`
    """

    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

    @property
    def state_suffix(self) -> str:
        if self.column is ROOT:
            return self.__class__.__name__
        return f"{self.__class__.__name__}/{self.column}"

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[V], new: Any) -> V:
        new = new if self.column is ROOT else new.get(self.column)
        if old is None:
            return new
        elif new is None:
            return old
        return min(old, new)

    def result(self, value: V) -> V:
        return value


R = TypeVar("R")


class Reduce(Aggregator, Generic[R]):
    """
    `Reduce()` allows you to perform complex aggregations using custom "reducer" and "initializer" functions.
    """

    def __init__(
        self,
        reducer: Callable[[R, Any], R],
        initializer: Callable[[Any], R],
    ) -> None:
        self._initializer: Callable[[Any], R] = initializer
        self._reducer: Callable[[R, Any], R] = reducer

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[R], new: Any) -> R:
        return self._initializer(new) if old is None else self._reducer(old, new)

    def result(self, value: R) -> R:
        return value


I = TypeVar("I")


class BaseCollector(ABC, Generic[I]):
    """
    Base class for window collections.

    Subclass it to implement custom collections.

    A Collector store incoming items un-modified in an optimized way.

    To reduce incoming items as they come in use an `Aggregator`.
    """

    @property
    @abstractmethod
    def column(self) -> Column:
        """
        The column to collect.

        Use `ROOT` to collect the whole message.
        """
        ...

    @abstractmethod
    def result(self, items: Iterable[I]) -> Any:
        """
        This method is triggered when a window is closed.
        It should return the final collection result.
        """
        ...


class Collector(BaseCollector):
    """
    Implementation of the `BaseCollector` interface.

    Provides a default implementation for the `column` property.
    """

    @property
    def column(self) -> Column:
        return ROOT


class Collect(Collector):
    """
    Use `Collect()` to gather all events within each window period. into a list.

    :param column: The column to collect. Use `ROOT` to collect the whole message.
        Default - `ROOT`
    """

    def __init__(self, column: Column = ROOT) -> None:
        self._column = column

    @property
    def column(self) -> Column:
        return self._column

    def result(self, items: Iterable[Any]) -> list[Any]:
        return list(items)
