from typing import Any, Optional

from .base import StreamFunction
from .types import UpdateCallback, UpdateWithMetadataCallback, VoidExecutor

__all__ = ("UpdateFunction", "UpdateWithMetadataFunction")


class UpdateFunction(StreamFunction):
    """
    Wrap a function into an "Update" function.

    The provided function must accept a value, and it's expected to mutate it
    or to perform some side effect.

    The result of the callback is always ignored, and the original input is passed
    downstream.
    """

    def __init__(self, func: UpdateCallback, name: Optional[str] = None):
        super().__init__(func, name=name)

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)

        def wrapper(value: Any, key: Any, timestamp: int, headers: Any, func=self.func):
            # Update a single value and forward it
            func(value)
            child_executor(value, key, timestamp, headers)

        return wrapper


class UpdateWithMetadataFunction(StreamFunction):
    """
    Wrap a function into an "Update" function.

    The provided function must accept a value, a key, and a timestamp.
    The callback is expected to mutate the value or to perform some side effect with it.

    The result of the callback is always ignored, and the original input is passed
    downstream.
    """

    def __init__(self, func: UpdateWithMetadataCallback, name: Optional[str] = None):
        super().__init__(func, name=name)

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)

        def wrapper(value: Any, key: Any, timestamp: int, headers: Any, func=self.func):
            # Update a single value and forward it
            func(value, key, timestamp, headers)
            child_executor(value, key, timestamp, headers)

        return wrapper
