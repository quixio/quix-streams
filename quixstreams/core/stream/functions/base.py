import abc
from typing import Any, TypeVar, Callable
from pickle import dumps, loads
from .types import StreamCallback, VoidExecutor

__all__ = ("StreamFunction",)


T = TypeVar("T")


def _pickle_copier(obj: T) -> Callable[[], T]:
    """
    A utility function to copy objects using a "pickle" library.
    It accepts an object and returns a closure that produces copies of the object.

    Pickle is faster on average than "copy.deepcopy()", and this approach also
    makes sure that the serialization step is done only once.

    """
    serialized = dumps(obj)
    return lambda: loads(serialized)


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
        Returns a wrapper to be called on a value, key and timestamp.
        """

    def _resolve_splitting(self, *child_executors: VoidExecutor) -> VoidExecutor:
        """
        Wrap a list of child executors for the particular operator to determine whether
        the data needs to be copied at this poin, and return a new executor.

        If there's more than one executor - it's a split in the data flow, and we need to
        copy the data for N-1 children.
        If there's only one executor - copying is not neccessary, and the executor
        is returned as is.
        """
        if len(child_executors) > 1:

            def wrapper(value: Any, key: Any, timestamp: int, headers: Any):
                value_copier = _pickle_copier(value)
                *executors, last_executor = child_executors
                for executor in executors:
                    executor(value_copier(), key, timestamp, headers)
                last_executor(value, key, timestamp, headers)

            return wrapper

        else:
            return child_executors[0]
