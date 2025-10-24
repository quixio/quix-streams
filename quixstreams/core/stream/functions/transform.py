from typing import Any, Literal, Union, cast, overload

from .base import StreamFunction
from .types import (
    TransformCallback,
    TransformExpandedCallback,
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

    func: Union[TransformCallback, TransformExpandedCallback]

    @overload
    def __init__(
        self,
        func: TransformCallback,
        expand: Literal[False] = False,
        on_watermark: Union[TransformCallback, None] = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        func: TransformExpandedCallback,
        expand: Literal[True],
        on_watermark: Union[TransformExpandedCallback, None] = None,
    ) -> None: ...

    def __init__(
        self,
        func: Union[TransformCallback, TransformExpandedCallback],
        expand: bool = False,
        on_watermark: Union[TransformCallback, TransformExpandedCallback, None] = None,
    ):
        super().__init__(func=func, on_watermark=on_watermark)

        self.expand = expand

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)

        if self.expand:
            expanded_func = cast(TransformExpandedCallback, self.func)

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
                is_watermark: bool = False,
                on_watermark=self.on_watermark,
            ):
                if is_watermark:
                    if on_watermark is not None:
                        # React on the new watermark if "on_watermark" is defined
                        result = self.on_watermark(None, None, timestamp, ())
                        for new_value, new_key, new_timestamp, new_headers in result:
                            child_executor(
                                new_value,
                                new_key,
                                new_timestamp,
                                new_headers,
                                False,
                            )
                    # Always pass the watermark downstream so other operators can react
                    # on it as well.
                    child_executor(
                        value,
                        key,
                        timestamp,
                        headers,
                        True,
                    )
                else:
                    result = expanded_func(value, key, timestamp, headers)
                    for new_value, new_key, new_timestamp, new_headers in result:
                        child_executor(new_value, new_key, new_timestamp, new_headers)

        else:
            func = cast(TransformCallback, self.func)

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
                is_watermark: bool = False,
                on_watermark=self.on_watermark,
            ):
                if is_watermark:
                    if on_watermark is not None:
                        # React on the new watermark if "on_watermark" is defined
                        new_value, new_key, new_timestamp, new_headers = (
                            self.on_watermark(None, None, timestamp, ())
                        )
                        child_executor(
                            new_value,
                            new_key,
                            new_timestamp,
                            new_headers,
                            False,
                        )
                    # Always pass the watermark downstream so other operators can react
                    # on it as well.
                    child_executor(
                        value,
                        key,
                        timestamp,
                        headers,
                        True,
                    )
                else:
                    # Execute a function on a single value and return its result
                    new_value, new_key, new_timestamp, new_headers = func(
                        value, key, timestamp, headers
                    )
                    child_executor(new_value, new_key, new_timestamp, new_headers)

        return wrapper
