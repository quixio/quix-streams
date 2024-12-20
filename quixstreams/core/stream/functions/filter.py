from typing import Any

from .base import StreamFunction
from .types import FilterCallback, FilterWithMetadataCallback, VoidExecutor

__all__ = ("FilterFunction", "FilterWithMetadataFunction")


class FilterFunction(StreamFunction):
    """
    Wraps a function into a "Filter" function.
    The result of a Filter function is interpreted as boolean.
    If it's `True`, the input will be return downstream.
    If it's `False`, the `Filtered` exception will be raised to signal that the
    value is filtered out.
    """

    def __init__(self, func: FilterCallback):
        super().__init__(func)
        self.func: FilterCallback

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        def wrapper(
            value: Any,
            key: Any,
            timestamp: int,
            headers: Any,
        ):
            # Filter a single value
            if func(value):
                child_executor(value, key, timestamp, headers)

        return wrapper


class FilterWithMetadataFunction(StreamFunction):
    """
    Wraps a function into a "Filter" function.

    The passed callback must accept value, key, and timestamp, and it's expected to
    return a boolean-like result.

    If the result is `True`, the input will be passed downstream.
    Otherwise, the value will be filtered out.
    """

    def __init__(self, func: FilterWithMetadataCallback):
        super().__init__(func)
        self.func: FilterWithMetadataCallback

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        def wrapper(
            value: Any,
            key: Any,
            timestamp: int,
            headers: Any,
        ):
            # Filter a single value
            if func(value, key, timestamp, headers):
                child_executor(value, key, timestamp, headers)

        return wrapper
