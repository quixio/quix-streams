import abc
from typing import Callable, Any, Tuple, Union, Protocol, Iterable

__all__ = (
    "StreamCallback",
    "VoidExecutor",
    "ReturningExecutor",
    "StreamFunction",
    "ApplyFunction",
    "UpdateFunction",
    "FilterFunction",
    "ApplyCallback",
    "ApplyExpandedCallback",
    "FilterCallback",
    "UpdateCallback",
    "ApplyWithMetadataCallback",
    "ApplyWithMetadataExpandedCallback",
    "ApplyWithMetadataFunction",
    "UpdateWithMetadataCallback",
    "UpdateWithMetadataFunction",
    "FilterWithMetadataCallback",
    "FilterWithMetadataFunction",
    "TransformFunction",
    "TransformCallback",
    "TransformExpandedCallback",
)


class SupportsBool(Protocol):
    def __bool__(self) -> bool: ...


ApplyCallback = Callable[[Any], Any]
ApplyExpandedCallback = Callable[[Any], Iterable[Any]]
UpdateCallback = Callable[[Any], None]
FilterCallback = Callable[[Any], bool]

ApplyWithMetadataCallback = Callable[[Any, Any, int, Any], Any]
ApplyWithMetadataExpandedCallback = Callable[[Any, Any, int, Any], Iterable[Any]]
UpdateWithMetadataCallback = Callable[[Any, Any, int, Any], None]
FilterWithMetadataCallback = Callable[[Any, Any, int, Any], SupportsBool]

TransformCallback = Callable[[Any, Any, int, Any], Tuple[Any, Any, int, Any]]
TransformExpandedCallback = Callable[
    [Any, Any, int, Any], Iterable[Tuple[Any, Any, int, Any]]
]

StreamCallback = Union[
    ApplyCallback,
    ApplyExpandedCallback,
    UpdateCallback,
    FilterCallback,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    UpdateWithMetadataCallback,
    FilterWithMetadataCallback,
    TransformCallback,
    TransformExpandedCallback,
]

VoidExecutor = Callable[[Any, Any, int, Any], None]
ReturningExecutor = Callable[[Any, Any, int, Any], Tuple[Any, Any, int, Any]]


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
    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        """
        Returns a wrapper to be called on a value, key and timestamp.
        """


class ApplyFunction(StreamFunction):
    """
    Wrap a function into "Apply" function.

    The provided callback is expected to return a new value based on input,
    and its result will always be passed downstream.
    """

    def __init__(
        self,
        func: ApplyCallback,
        expand: bool = False,
    ):
        super().__init__(func)
        self.expand = expand

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        if self.expand:

            def wrapper(
                value: Any, key: Any, timestamp: int, headers: Any, func=self.func
            ):
                # Execute a function on a single value and wrap results into a list
                # to expand them downstream
                result = func(value)
                for item in result:
                    child_executor(item, key, timestamp, headers)

        else:

            def wrapper(
                value: Any, key: Any, timestamp: int, headers: Any, func=self.func
            ):
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

    def __init__(
        self,
        func: ApplyWithMetadataCallback,
        expand: bool = False,
    ):
        super().__init__(func)
        self.expand = expand

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        if self.expand:

            def wrapper(
                value: Any, key: Any, timestamp: int, headers: Any, func=self.func
            ):
                # Execute a function on a single value and wrap results into a list
                # to expand them downstream
                result = func(value, key, timestamp, headers)
                for item in result:
                    child_executor(item, key, timestamp, headers)

        else:

            def wrapper(
                value: Any, key: Any, timestamp: int, headers: Any, func=self.func
            ):
                # Execute a function on a single value and return its result
                result = func(value, key, timestamp, headers)
                child_executor(result, key, timestamp, headers)

        return wrapper


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

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        def wrapper(value: Any, key: Any, timestamp: int, headers: Any, func=self.func):
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

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        def wrapper(value: Any, key: Any, timestamp: int, headers: Any, func=self.func):
            # Filter a single value
            if func(value, key, timestamp, headers):
                child_executor(value, key, timestamp, headers)

        return wrapper


class UpdateFunction(StreamFunction):
    """
    Wrap a function into an "Update" function.

    The provided function must accept a value, and it's expected to mutate it
    or to perform some side effect.

    The result of the callback is always ignored, and the original input is passed
    downstream.
    """

    def __init__(self, func: UpdateCallback):
        super().__init__(func)

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
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

    def __init__(self, func: UpdateWithMetadataCallback):
        super().__init__(func)

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        def wrapper(value: Any, key: Any, timestamp: int, headers: Any, func=self.func):
            # Update a single value and forward it
            func(value, key, timestamp, headers)
            child_executor(value, key, timestamp, headers)

        return wrapper


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

    def __init__(
        self,
        func: Union[TransformCallback, TransformExpandedCallback],
        expand: bool = False,
    ):
        super().__init__(func)
        self.expand = expand

    def get_executor(self, child_executor: VoidExecutor) -> VoidExecutor:
        if self.expand:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
                func: TransformExpandedCallback = self.func,
            ):
                result = func(value, key, timestamp, headers)
                for new_value, new_key, new_timestamp, new_headers in result:
                    child_executor(new_value, new_key, new_timestamp, new_headers)

        else:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                headers: Any,
                func: TransformCallback = self.func,
            ):
                # Execute a function on a single value and return its result
                new_value, new_key, new_timestamp, new_headers = func(
                    value, key, timestamp, headers
                )
                child_executor(new_value, new_key, new_timestamp, new_headers)

        return wrapper
