from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Optional, TypeVar


class Aggregation(ABC):
    @abstractmethod
    def start(self) -> Any: ...

    @abstractmethod
    def agg(self, old: Any, new: Any) -> Any: ...

    @abstractmethod
    def result(self, value: Any) -> Any: ...


V = TypeVar("V", int, float)


class Sum(Aggregation):
    def start(self) -> int:
        return 0

    def agg(self, old: V, new: V) -> V:
        return old + new

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
    def start(self) -> tuple[float, int]:
        return 0.0, 0

    def agg(self, old: tuple[V, int], new: V) -> tuple[V, int]:
        old_sum, old_count = old
        return old_sum + new, old_count + 1

    def result(self, value: tuple[int | float, int]) -> float:
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
    def start(self) -> None:
        return None

    def agg(self, old: Optional[V], new: V) -> V:
        if old is None:
            return new
        return max(old, new)

    def result(self, value: V) -> V:
        return value


class Min(Aggregation):
    def start(self) -> None:
        return None

    def agg(self, old: Optional[V], new: V) -> V:
        if old is None:
            return new
        return min(old, new)

    def result(self, value: V) -> V:
        return value


class Collector(ABC):
    @abstractmethod
    def add(self, item: Any) -> Any: ...

    @abstractmethod
    def result(self, items: list[Any]) -> Any: ...


class Collect(Collector):
    def add(self, item: Any) -> Any:
        return item

    def result(self, items: list[Any]) -> list[Any]:
        return items
