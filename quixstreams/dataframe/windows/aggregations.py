from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

__all__ = [
    "Collect",
    "Count",
    "Max",
    "Mean",
    "Min",
    "Reduce",
    "Sum",
    "Aggregator",
    "BaseAggregator",
    "Collector",
    "BaseCollector",
    "Earliest",
    "Latest",
    "First",
    "Last",
]


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
        The state suffix is used to store the aggregation state in the window.

        The complete state key is built using the result column name and this suffix.
        If these values change, the state key will also change, and the aggregation state will restart from zero.

        Aggregations should change the state suffix when their parameters change to avoid
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
    def agg(self, old: S, new: Any, timestamp: int) -> S:
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

    def __init__(self, column: Optional[str] = None) -> None:
        self.column = column

    @property
    def state_suffix(self) -> str:
        if self.column is None:
            return self.__class__.__name__
        return f"{self.__class__.__name__}/{self.column}"


class Count(Aggregator):
    """
    Use `Count()` to aggregate the total number of events  within each window period..
    """

    def initialize(self) -> int:
        return 0

    def agg(self, old: int, new: Any, timestamp: int) -> int:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old

        return old + 1

    def result(self, value: int) -> int:
        return value


V = TypeVar("V", int, float)


class Sum(Aggregator):
    """
    Use `Sum()` to aggregate the sum of the events, or a column of the events, within each window period.

    :param column: The column to sum. Use `None` to sum the whole message.
        Default - `None`
    """

    def initialize(self) -> int:
        return 0

    def agg(self, old: V, new: Any, timestamp: int) -> V:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old

        return old + new

    def result(self, value: V) -> V:
        return value


class Mean(Aggregator):
    """
    Use `Mean()` to aggregate the mean of the events, or a column of the events, within each window period.

    :param column: The column to mean. Use `None` to mean the whole message.
        Default - `None`
    """

    def initialize(self) -> tuple[float, int]:
        return 0.0, 0

    def agg(self, old: tuple[V, int], new: Any, timestamp: int) -> tuple[V, int]:
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

    :param column: The column to max. Use `None` to max the whole message.
        Default - `None`
    """

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[V], new: Any, timestamp: int) -> Optional[V]:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old
        if old is None:
            return new
        return max(old, new)

    def result(self, value: V) -> V:
        return value


class Min(Aggregator):
    """
    Use `Min()` to aggregate the min of the events, or a column of the events, within each window period.

    :param column: The column to min. Use `None` to min the whole message.
        Default - `None`
    """

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[V], new: Any, timestamp: int) -> Optional[V]:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old
        if old is None:
            return new
        return min(old, new)

    def result(self, value: V) -> V:
        return value


class Earliest(Aggregator):
    """
    Use `Earliest()` to get the event (or its column) with the smallest timestamp within each window period.

    :param column: The column to aggregate. Use `None` to earliest the whole message.
        Default - `None`
    """

    def initialize(self) -> None:
        return None

    def agg(self, old: Any, new: Any, timestamp: int) -> Any:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old
        if old is None:
            return (new, timestamp)

        old_value, old_timestamp = old
        if timestamp < old_timestamp:
            return (new, timestamp)
        return old

    def result(self, value: Optional[tuple[Any, int]]) -> Any:
        if value is None:
            return value
        return value[0]


class Latest(Aggregator):
    """
    Use `Latest()` to get the event (or its column) with the latest timestamp within each window period.

    :param column: The column to aggregate. Use `None` to latest the whole message.
        Default - `None`
    """

    def initialize(self) -> None:
        return None

    def agg(self, old: Any, new: Any, timestamp: int) -> tuple[Any, int]:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old
        if old is None:
            return (new, timestamp)

        old_value, old_timestamp = old
        if timestamp >= old_timestamp:
            return (new, timestamp)
        return old

    def result(self, value: Optional[tuple[Any, int]]) -> Any:
        if value is None:
            return value
        return value[0]


class First(Aggregator):
    """
    Use `First()` to get the first event, or a column of the event, within each window period.
    This aggregation works based on the processing order.

    :param column: The column to aggregate. Use `None` to first the whole message.
        Default - `None`
    """

    def initialize(self) -> None:
        return None

    def agg(self, old: Any, new: Any, timestamp: int) -> Any:
        if self.column is not None:
            new = new.get(self.column)

        if old is None:
            return new
        return old

    def result(self, value: Any) -> Any:
        return value


class Last(Aggregator):
    """
    Use `Last()` to get the last event, or a column of the event, within each window period.
    This aggregation works based on the processing order.

    :param column: The column to aggregate. Use `None` to last the whole message.
        Default - `None`
    """

    def initialize(self) -> None:
        return None

    def agg(self, old: Any, new: Any, timestamp: int) -> Any:
        if self.column is not None:
            new = new.get(self.column)

        if new is None:
            return old
        return new

    def result(self, value: Any) -> Any:
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

    def agg(self, old: Optional[R], new: Any, timestamp: int) -> R:
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
    def column(self) -> Optional[str]:
        """
        The column to collect.

        Use `None` to collect the whole message.
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

    def __init__(self, column: Optional[str] = None) -> None:
        self._column = column

    @property
    def column(self) -> Optional[str]:
        return self._column


class Collect(Collector):
    """
    Use `Collect()` to gather all events within each window period. into a list.

    :param column: The column to collect. Use `None` to collect the whole message.
        Default - `None`
    """

    def result(self, items: Iterable[Any]) -> list[Any]:
        return list(items)
