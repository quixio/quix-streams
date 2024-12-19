from typing import Any, Literal, Union, overload

from .base import StreamFunction
from .types import (
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    VoidExecutor,
)

__all__ = ("ApplyFunction", "ApplyWithMetadataFunction")


class ApplyFunction(StreamFunction):
    """
    Wrap a function into "Apply" function.

    The provided callback is expected to return a new value based on input,
    and its result will always be passed downstream.
    """

    @overload
    def __init__(self, func: ApplyCallback, expand: Literal[False] = False) -> None: ...

    @overload
    def __init__(self, func: ApplyExpandedCallback, expand: Literal[True]) -> None: ...

    def __init__(
        self,
        func: Union[ApplyCallback, ApplyExpandedCallback],
        expand: bool = False,
    ):
        super().__init__(func)

        self.func: Union[ApplyCallback, ApplyExpandedCallback]
        self.expand = expand

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        if self.expand:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ) -> None:
                # Execute a function on a single value and wrap results into a list
                # to expand them downstream
                result = func(value)
                for item in result:
                    child_executor(item, key, timestamp, headers)

        else:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ) -> None:
                # Execute a function on a single value and return its result
                result = func(value)
                child_executor(result, key, timestamp, headers)

        return wrapper


class ApplyWithMetadataFunction(StreamFunction):
    """
    Wrap a function into "Apply" function.

    The provided function is expected to accept value, and timestamp and return
    a new value based on input,
    and its result will always be passed downstream.
    """

    @overload
    def __init__(
        self, func: ApplyWithMetadataCallback, expand: Literal[False] = False
    ) -> None: ...

    @overload
    def __init__(
        self, func: ApplyWithMetadataExpandedCallback, expand: Literal[True]
    ) -> None: ...

    def __init__(
        self,
        func: Union[ApplyWithMetadataCallback, ApplyWithMetadataExpandedCallback],
        expand: bool = False,
    ):
        super().__init__(func)

        self.func: Union[ApplyWithMetadataCallback, ApplyWithMetadataExpandedCallback]
        self.expand = expand

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)
        func = self.func

        if self.expand:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                # Execute a function on a single value and wrap results into a list
                # to expand them downstream
                result = func(value, key, timestamp, headers)
                for item in result:
                    child_executor(item, key, timestamp, headers)

        else:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                # Execute a function on a single value and return its result
                result = func(value, key, timestamp, headers)
                child_executor(result, key, timestamp, headers)

        return wrapper
