import abc
from itertools import zip_longest
from typing import Callable, List, Optional, Any, Tuple, Union, Protocol, Iterable

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
    "compose",
)


class SupportsBool(Protocol):
    def __bool__(self) -> bool: ...


ApplyCallback = Callable[[Any], Any]
ApplyExpandedCallback = Callable[[Any], Iterable[Any]]
UpdateCallback = Callable[[Any], None]
FilterCallback = Callable[[Any], bool]

ApplyWithMetadataCallback = Callable[[Any, Any, int], Any]
ApplyWithMetadataExpandedCallback = Callable[[Any, Any, int], Iterable[Any]]
UpdateWithMetadataCallback = Callable[[Any, Any, int], None]
FilterWithMetadataCallback = Callable[[Any, Any, int], SupportsBool]

TransformCallback = Callable[[Any, Any, int], Tuple[Any, Any, int]]
TransformExpandedCallback = Callable[[Any, Any, int], Iterable[Tuple[Any, Any, int]]]

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

VoidExecutor = Callable[[Any, Any, int], None]
ReturningExecutor = Callable[[Any, Any, int], Tuple[Any, Any, int]]


def _default_sink(value: Any, key: Any, timestamp: int):
    pass


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
        Returns a wrapper to be called on a single value.
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

            def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
                # Execute a function on a single value and wrap results into a list
                # to expand them downstream
                result = func(value)
                for item in result:
                    child_executor(item, key, timestamp)

        else:

            def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
                # Execute a function on a single value and return its result
                result = func(value)
                child_executor(result, key, timestamp)

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

            def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
                # Execute a function on a single value and wrap results into a list
                # to expand them downstream
                result = func(value, key, timestamp)
                for item in result:
                    child_executor(item, key, timestamp)

        else:

            def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
                # Execute a function on a single value and return its result
                result = func(value, key, timestamp)
                child_executor(result, key, timestamp)

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
        def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
            # Filter a single value
            if func(value):
                child_executor(value, key, timestamp)

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
        def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
            # Filter a single value
            if func(value, key, timestamp):
                child_executor(value, key, timestamp)

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
        def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
            # Update a single value and forward it
            func(value)
            child_executor(value, key, timestamp)

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
        def wrapper(value: Any, key: Any, timestamp: int, func=self.func):
            # Update a single value and forward it
            func(value, key, timestamp)
            child_executor(value, key, timestamp)

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
                func: TransformExpandedCallback = self.func,
            ):
                result = func(value, key, timestamp)
                for new_value, new_key, new_timestamp in result:
                    child_executor(new_value, new_key, new_timestamp)

        else:

            def wrapper(
                value: Any,
                key: Any,
                timestamp: int,
                func: TransformCallback = self.func,
            ):
                # Execute a function on a single value and return its result
                new_value, new_key, new_timestamp = func(value, key, timestamp)
                child_executor(new_value, new_key, new_timestamp)

        return wrapper


def compose(
    functions: List[StreamFunction],
    allow_filters: bool = True,
    allow_updates: bool = True,
    allow_expands: bool = True,
    allow_transforms: bool = True,
    sink: Optional[Callable[[Any, Any, int], None]] = None,
) -> VoidExecutor:
    """
    Composes a list of functions and its parents into a single
    big closure like this:
    ```
    [func, func, func] -> func(func(func()))
    ```

    Closures are more performant than calling all functions one by one in a loop.

    :param functions: list of `StreamFunction` objects to compose
    :param allow_filters: If False, will fail with `ValueError` if
        the list has `FilterFunction`. Default - True.
    :param allow_updates: If False, will fail with `ValueError` if
        the list has `UpdateFunction`. Default - True.
    :param allow_expands: If False, will fail with `ValueError` if
        the list has `ApplyFunction` with "expand=True". Default - True.
    :param allow_transforms: If False, will fail with `ValueError` if
        the list has `TransformRecordFunction`. Default - True.
    :param sink: callable to accumulate the results of the execution.

    :raises ValueError: if disallowed functions are present in the list of functions.
    """
    composed = None

    # Create a reversed zipped list of functions to chain them together.
    # Example:
    #  Input: [func1, func2, func3, func4]
    #  Zipped: [(func4, None), (func3, func4), (func2, func3), (func1, func2)]
    functions_zipped = list(zip_longest(functions, functions[1:]))
    functions_zipped.reverse()

    # Iterate over a reversed zipped list of functions
    for func, child_func in functions_zipped:
        # Validate that only allowed functions are passed
        if not allow_updates and isinstance(
            func, (UpdateFunction, UpdateWithMetadataFunction)
        ):
            raise ValueError("Update functions are not allowed")
        elif not allow_filters and isinstance(
            func, (FilterFunction, FilterWithMetadataFunction)
        ):
            raise ValueError("Filter functions are not allowed")
        elif not allow_transforms and isinstance(func, TransformFunction):
            raise ValueError("Transform functions are not allowed")
        elif not allow_expands and func.expand:
            raise ValueError("Expand functions are not allowed")

        if child_func is None:
            child_func = sink or _default_sink

        if composed is None:
            composed = func.get_executor(child_func)
        else:
            composed = func.get_executor(composed)

    return composed
