from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Optional,
    TypeAlias,
    TypeVar,
    Union,
)


class Aggregator(ABC):
    """
    Base class for window aggregation.

    Subclass it to implement custom aggregations.
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


COLUMN: TypeAlias = Union[Hashable, type[ROOT]]


class Sum(Aggregator):
    def __init__(self, column: COLUMN = ROOT) -> None:
        self.column = column

    def initialize(self) -> int:
        return 0

    def agg(self, old: V, new: Any) -> V:
        if self.column is ROOT:
            return old + new
        return old + new[self.column]

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
    def __init__(self, column: COLUMN = ROOT) -> None:
        self.column = column

    def initialize(self) -> tuple[float, int]:
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
    def __init__(self, column: COLUMN = ROOT) -> None:
        self.column = column

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[V], new: Any) -> V:
        if self.column is not ROOT:
            v = new[self.column]
        if old is None:
            return v
        return max(old, v)

    def result(self, value: V) -> V:
        return value


class Min(Aggregator):
    def __init__(self, column: COLUMN = ROOT) -> None:
        self.column = column

    def initialize(self) -> None:
        return None

    def agg(self, old: Optional[V], new: Any) -> V:
        if self.column is not ROOT:
            v = new[self.column]
        if old is None:
            return v
        return min(old, v)

    def result(self, value: V) -> V:
        return value


I = TypeVar("I")


class Collector(ABC, Generic[I]):
    """
    Base class for window collections.

    Subclass it to implement custom collections.
    """

    @property
    @abstractmethod
    def column(self) -> COLUMN:
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
    def __init__(self, column: COLUMN = ROOT) -> None:
        self._column = column

    @property
    def column(self) -> COLUMN:
        return self._column

    def result(self, items: Iterable[Any]) -> list[Any]:
        return list(items)
