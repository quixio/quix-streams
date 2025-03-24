from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

from typing_extensions import TypeAlias


class Aggregator(ABC):
    """
    Base class for window aggregation.

    Subclass it to implement custom aggregations.

    An Aggregator reduce incoming items into a single value or group of values. When the window
    is closed the aggregator produce a result based on the reduced value.

    To store all incoming items without reducing them use a `Collector`.
    """

    @abstractmethod
    def initialize(self) -> Any:
        """
        This method is triggered once to build the aggregation starting value.
        It should return the initial value for the aggregation.
        """
        ...

    @abstractmethod
    def agg(self, old: Any, new: Any) -> Any:
        """
        This method is trigged when a window is updated with a new value.
        It should return the updated aggregated value.
        """
        ...

    @abstractmethod
    def result(self, value: Any) -> Any:
        """
        This method is triggered when a window is closed.
        It should return the final aggregation result.
        """
        ...


V = TypeVar("V", int, float)


class ROOT:
    pass


Column: TypeAlias = Union[Hashable, type[ROOT]]


class Sum(Aggregator):
    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

    def initialize(self) -> int:
        return 0

    def agg(self, old: V, new: Any) -> V:
        new = new if self.column is ROOT else new.get(self.column)
        return old + (new or 0)

    def result(self, value: V) -> V:
        return value


class Count(Aggregator):
    def initialize(self) -> int:
        return 0

    def agg(self, old: int, new: Any) -> int:
        return old + 1

    def result(self, value: int) -> int:
        return value


class Mean(Aggregator):
    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

    def initialize(self) -> tuple[float, int]:
        return 0.0, 0

    def agg(self, old: tuple[V, int], new: Any) -> tuple[V, int]:
        new = new if self.column is ROOT else new.get(self.column)
        if new is None:
            return old

        old_sum, old_count = old
        return old_sum + new, old_count + 1

    def result(self, value: tuple[Union[int, float], int]) -> Optional[float]:
        sum_, count_ = value
        if count_ == 0:
            return None
        return sum_ / count_


R = TypeVar("R", int, float)


class Reduce(Aggregator, Generic[R]):
    def __init__(
        self,
        reducer: Callable[[R, Any], R],
        initializer: Callable[[Any], R],
    ) -> None:
        self._initializer: Callable[[Any], R] = initializer
        self._reducer: Callable[[R, Any], R] = reducer

    def initialize(self) -> Any:
        return None

    def agg(self, old: R, new: Any) -> Any:
        return self._initializer(new) if old is None else self._reducer(old, new)

    def result(self, value: R) -> R:
        return value


class Max(Aggregator):
    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

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
    def __init__(self, column: Column = ROOT) -> None:
        self.column = column

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


I = TypeVar("I")


class Collector(ABC, Generic[I]):
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


class Collect(Collector):
    def __init__(self, column: Column = ROOT) -> None:
        self._column = column

    @property
    def column(self) -> Column:
        return self._column

    def result(self, items: Iterable[Any]) -> list[Any]:
        return list(items)
