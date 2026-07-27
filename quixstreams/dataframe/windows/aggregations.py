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

    def merge(self, a: S, b: S) -> S:
        """
        Combine the aggregation states of two windows that are being merged into one.

        Only session windows call this method: an out-of-order event that falls
        within the inactivity gap of two open sessions bridges them, and the two
        aggregation states have to be combined. `a` is the state of the session that
        starts earlier in event time, `b` the state of the session that starts later;
        the two sessions never overlap.

        The default implementation raises: an aggregation that cannot be merged
        cannot be used with `session_window()`, and that is rejected when the window
        is built rather than when a merge happens. All other window types are
        unaffected and never call this method.

        >***NOTE:*** Collectors (`BaseCollector`) deliberately have no `merge()`.
        Their values live in a separate column family keyed by timestamp and are
        range-fetched over the window's `[start, end)` at expiry. A merged session's
        range is the hull of the two merged ranges, so the fetch already returns
        both sessions' values in timestamp order.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support merging and cannot be used "
            f"with session windows"
        )

    @property
    def mergeable(self) -> bool:
        """
        Whether this aggregator can be used with session windows.

        True when `merge()` is overridden. Override this property directly only if
        you implement merging some other way.
        """
        return type(self).merge is not BaseAggregator.merge


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

    def merge(self, a: int, b: int) -> int:
        return a + b


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

    def merge(self, a: V, b: V) -> V:
        return a + b


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

    def merge(self, a: tuple[V, int], b: tuple[V, int]) -> tuple[V, int]:
        return a[0] + b[0], a[1] + b[1]


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

    def merge(self, a: Optional[V], b: Optional[V]) -> Optional[V]:
        if a is None:
            return b
        if b is None:
            return a
        return max(a, b)


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

    def merge(self, a: Optional[V], b: Optional[V]) -> Optional[V]:
        if a is None:
            return b
        if b is None:
            return a
        return min(a, b)


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

    def merge(
        self, a: Optional[tuple[Any, int]], b: Optional[tuple[Any, int]]
    ) -> Optional[tuple[Any, int]]:
        if a is None:
            return b
        if b is None:
            return a
        return a if a[1] <= b[1] else b


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

    def merge(
        self, a: Optional[tuple[Any, int]], b: Optional[tuple[Any, int]]
    ) -> Optional[tuple[Any, int]]:
        if a is None:
            return b
        if b is None:
            return a
        return b if b[1] >= a[1] else a


class First(Aggregator):
    """
    Use `First()` to get the first event, or a column of the event, within each window period.
    This aggregation works based on the processing order.

    >***NOTE:*** When two session windows are merged, processing order is not
    recoverable across two independently built sessions, so `First()` falls back to
    **session order** and keeps the earlier session's value. Use `Earliest()` when
    the result must be order-independent.

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

    def merge(self, a: Any, b: Any) -> Any:
        # `a` comes from the session that starts earlier in event time.
        return b if a is None else a


class Last(Aggregator):
    """
    Use `Last()` to get the last event, or a column of the event, within each window period.
    This aggregation works based on the processing order.

    >***NOTE:*** When two session windows are merged, processing order is not
    recoverable across two independently built sessions, so `Last()` falls back to
    **session order** and keeps the later session's value. Use `Latest()` when the
    result must be order-independent.

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

    def merge(self, a: Any, b: Any) -> Any:
        # `b` comes from the session that starts later in event time.
        return a if b is None else b


R = TypeVar("R")


class Reduce(Aggregator, Generic[R]):
    """
    `Reduce()` allows you to perform complex aggregations using custom "reducer" and "initializer" functions.

    :param reducer: A function combining the accumulated state with a new value.
    :param initializer: A function building the state from the first value.
    :param merger: A function combining two accumulated states, required only for
        session windows. The reducer cannot be reused for this because it takes a
        raw value, not a second state. Default - `None`.
    """

    def __init__(
        self,
        reducer: Callable[[R, Any], R],
        initializer: Callable[[Any], R],
        merger: Optional[Callable[[R, R], R]] = None,
    ) -> None:
        super().__init__()
        self._initializer: Callable[[Any], R] = initializer
        self._reducer: Callable[[R, Any], R] = reducer
        self._merger: Optional[Callable[[R, R], R]] = merger

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[R], new: Any, timestamp: int) -> R:
        return self._initializer(new) if old is None else self._reducer(old, new)

    def result(self, value: R) -> R:
        return value

    def merge(self, a: R, b: R) -> R:
        if self._merger is None:
            raise NotImplementedError(
                "Reduce does not support merging and cannot be used with session "
                "windows unless a `merger=` function is provided"
            )
        return self._merger(a, b)

    @property
    def mergeable(self) -> bool:
        return self._merger is not None


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
