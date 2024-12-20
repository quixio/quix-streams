import abc
from typing import Any

from .types import StreamCallback, VoidExecutor
from .utils import pickle_copier

__all__ = ("StreamFunction",)


class StreamFunction(abc.ABC):
    """
    A base class for all the streaming operations in Quix Streams.

    It provides a `get_executor` method to return a closure to be called with the input
    values.
    """

    expand: bool = False

    def __init__(self, func: StreamCallback):
        self.func = func

    @abc.abstractmethod
    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        """
        Returns a wrapper to be called on a value, key, timestamp and headers.
        """

    def _resolve_branching(self, *child_executors: VoidExecutor) -> VoidExecutor:
        """
        Wrap a list of child executors for the particular operator to determine whether
        the data needs to be copied at this point, and return a new executor.

        If there's more than one executor - it's a branching point in the data flow,
        and we need to copy the value for the downstream branches
        in case they mutate it.

        If there's only one executor - copying is not neccessary, and the executor
        is returned as is.
        """
        if not child_executors:
            raise ValueError("At least one executor is required")

        if len(child_executors) > 1:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                first_branch_executor, *branch_executors = child_executors
                copier = pickle_copier(value)

                # Pass the original value to the first branch to reduce copying
                first_branch_executor(value, key, timestamp, headers)
                # Copy the value for the rest of the branches
                for branch_executor in branch_executors:
                    branch_executor(copier(), key, timestamp, headers)

            return wrapper

        else:
            return child_executors[0]
