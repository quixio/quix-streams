import sys
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Iterable, Optional, TypeVar, Union


class Aggregation(ABC):
    """
    Base class for window aggregation.

    Subclass it to implement custom aggregations.
    """

    @abstractmethod
    def start(self) -> Any:
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

ROOT = object()


class Sum(Aggregation):
    def __init__(self, column: Any = ROOT) -> None:
        self.column = column

    def start(self) -> int:
        return 0

    def agg(self, old: V, new: Any) -> V:
        if self.column is ROOT:
            return old + new
        return old + new[self.column]

    def result(self, value: V) -> V:
        return value


class Count(Aggregation):
    def start(self) -> int:
        return 0

    def agg(self, old: int, new: Any) -> int:
        return old + 1

    def result(self, value: int) -> int:
        return value


class Mean(Aggregation):
    def __init__(self, column: Any = ROOT) -> None:
        self.column = column

    def start(self) -> tuple[float, int]:
        return 0.0, 0

    def agg(self, old: tuple[V, int], new: Any) -> tuple[V, int]:
        old_sum, old_count = old
        if self.column is ROOT:
            return old_sum + new, old_count + 1
        return old_sum + new[self.column], old_count + 1

    def result(self, value: tuple[Union[int, float], int]) -> float:
        sum_, count_ = value
        return sum_ / count_


R = TypeVar("R", int, float)


class Reduce(Aggregation, Generic[R]):
    def __init__(
        self,
        reducer: Callable[[R, Any], R],
        initializer: Callable[[Any], R],
    ) -> None:
        self._initializer: Callable[[Any], R] = initializer
        self._reducer: Callable[[R, Any], R] = reducer

    def start(self) -> Any:
        return None

    def agg(self, old: R, new: Any) -> Any:
        return self._initializer(new) if old is None else self._reducer(old, new)

    def result(self, value: R) -> R:
        return value


class Max(Aggregation):
    def __init__(self, column: Any = ROOT) -> None:
        self.column = column

    def start(self) -> float:
        return sys.float_info.min

    def agg(self, old: Optional[V], new: Any) -> V:
        if self.column is ROOT:
            return max(old, new)
        return max(old, new[self.column])

    def result(self, value: V) -> V:
        return value


class Min(Aggregation):
    def __init__(self, column: Any = ROOT) -> None:
        self.column = column

    def start(self) -> float:
        return sys.float_info.max

    def agg(self, old: Optional[V], new: Any) -> V:
        if self.column is ROOT:
            return min(old, new)
        return min(old, new[self.column])

    def result(self, value: V) -> V:
        return value


I = TypeVar("I")


class Collector(ABC, Generic[I]):
    """
    Base class for window collections.

    Subclass it to implement custom collections.
    """

    @abstractmethod
    def add(self, item: Any) -> I:
        """
        This method is triggered when a new value is added to the collection.
        It should return the individual value used by the collection.
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
    def __init__(self, column: Any = ROOT) -> None:
        self.column = column

    def add(self, item: Any) -> Any:
        if self.column is ROOT:
            return item
        return item[self.column]

    def result(self, items: Iterable[Any]) -> list[Any]:
        return list(items)
