from typing import Any, Literal, Union, cast, overload

from .base import StreamFunction
from .types import (
    TransformCallback,
    TransformExpandedCallback,
    TransformWallClockCallback,
    TransformWallClockExpandedCallback,
    VoidExecutor,
)

__all__ = ("TransformFunction",)


class TransformFunction(StreamFunction):
    """
    Wrap a function into a "Transform" function.

    The provided callback must accept a value, a key and a timestamp.
    It's expected to return a new value, new key and new timestamp.

    This function must be used with caution, because it can technically change the
    key.
    It's supposed to be used by the library internals and not be a part of the public
    API.

    The result of the callback will always be passed downstream.
    """

    @overload
    def __init__(
        self,
        func: TransformCallback,
        expand: Literal[False] = False,
        wall_clock: Literal[False] = False,
    ) -> None: ...

    @overload
    def __init__(
        self,
        func: TransformExpandedCallback,
        expand: Literal[True],
        wall_clock: Literal[False] = False,
    ) -> None: ...

    @overload
    def __init__(
        self,
        func: TransformWallClockCallback,
        expand: Literal[False] = False,
        wall_clock: Literal[True] = True,
    ) -> None: ...

    @overload
    def __init__(
        self,
        func: TransformWallClockExpandedCallback,
        expand: Literal[True],
        wall_clock: Literal[True],
    ) -> None: ...

    def __init__(
        self,
        func: Union[
            TransformCallback,
            TransformExpandedCallback,
            TransformWallClockCallback,
            TransformWallClockExpandedCallback,
        ],
        expand: bool = False,
        wall_clock: bool = False,
    ):
        super().__init__(func)

        self.func: Union[
            TransformCallback,
            TransformExpandedCallback,
            TransformWallClockCallback,
            TransformWallClockExpandedCallback,
        ]
        self.expand = expand
        self.wall_clock = wall_clock

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)

        if self.expand and self.wall_clock:
            wall_clock_expanded_func = cast(
                TransformWallClockExpandedCallback, self.func
            )

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                for (
                    new_value,
                    new_key,
                    new_timestamp,
                    new_headers,
                ) in wall_clock_expanded_func(timestamp):
                    child_executor(new_value, new_key, new_timestamp, new_headers)

        elif self.expand:
            expanded_func = cast(TransformExpandedCallback, self.func)

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                result = expanded_func(value, key, timestamp, headers)
                for new_value, new_key, new_timestamp, new_headers in result:
                    child_executor(new_value, new_key, new_timestamp, new_headers)

        elif self.wall_clock:
            wall_clock_func = cast(TransformWallClockCallback, self.func)

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                new_value, new_key, new_timestamp, new_headers = wall_clock_func(
                    timestamp
                )
                child_executor(new_value, new_key, new_timestamp, new_headers)

        else:
            regular_func = cast(TransformCallback, self.func)

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
            ):
                # Execute a function on a single value and return its result
                new_value, new_key, new_timestamp, new_headers = regular_func(
                    value, key, timestamp, headers
                )
                child_executor(new_value, new_key, new_timestamp, new_headers)

        return wrapper
