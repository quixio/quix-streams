import abc
import functools
from itertools import chain
from typing import TypeVar, Callable, List, Any

__all__ = (
    "StreamCallable",
    "StreamFunction",
    "ApplyFunction",
    "ApplyExpandFunction",
    "UpdateFunction",
    "FilterFunction",
    "Filtered",
    "compose",
)

R = TypeVar("R")
T = TypeVar("T")

StreamCallable = Callable[[T], R]


class Filtered(Exception):
    ...


class StreamFunction(abc.ABC):
    """
    A base class for all the streaming operations in Quix Streams.

    It provides two methods that return closures to be called on the input values:
    - `get_executor` - a wrapper to execute on a single value
    - `get_executor_expanded` - a wrapper to execute on an expanded value.
        Expanded value is a list, where each item should be treated as a separate value.
    """

    expand: bool = False

    def __init__(self, func: StreamCallable):
        self._func = func
        self._expand = False

    @property
    def func(self) -> StreamCallable:
        """
        The original function
        """
        return self._func

    @abc.abstractmethod
    def get_executor(self) -> StreamCallable:
        """
        Returns a wrapper to be called on a single value.
        """

    @abc.abstractmethod
    def get_executor_expanded(self) -> StreamCallable:
        """
        Returns a wrapper to be called on a list of expanded values.
        """


class ApplyFunction(StreamFunction):
    """
    Wrap a function into "Apply" function.

    The provided function is expected to return a new value based on input,
    and its result will always be passed downstream.
    """

    def get_executor(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> R:
            # Execute a function on a single value and return its result
            return func(value)

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)

    def get_executor_expanded(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> R:
            # Execute a function on an expanded value and return a list with results
            return [func(i) for i in value]

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)


class ApplyExpandFunction(StreamFunction):
    """
    Wrap a function into "Apply" function and expand the returned iterable
    into separate values downstream.

    The provided function is expected to return an `Iterable`.
    If the returned value is not `Iterable`, `TypeError` will be raised.
    """

    expand = True

    def get_executor(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> List[Any]:
            # Execute a function on a single value and wrap results into a list
            # to expand them downstream
            return list(func(value))

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)

    def get_executor_expanded(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> R:
            # Execute a function on an expanded value and flatten the results
            # (expanded value is an iterable, and the function itself
            # also returns an iterable)
            return list(chain.from_iterable(func(i) for i in value))

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)


class FilterFunction(StreamFunction):
    """
    Wraps a function into a "Filter" function.
    The result of a Filter function is interpreted as boolean.
    If it's `True`, the input will be return downstream.
    If it's `False`, the `Filtered` exception will be raised to signal that the
    value is filtered out.
    """

    def get_executor(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> T:
            # Filter a single value
            if func(value):
                return value
            raise Filtered()

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)

    def get_executor_expanded(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> T:
            # Filter an expanded value.
            # If all items from expanded list are filtered, raise Filtered()
            # exception to abort the function chain

            value = [i for i in value if func(i)]
            if not value:
                raise Filtered()
            return value

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)


class UpdateFunction(StreamFunction):
    """
    Wrap a function into an "Update" function.

    The provided function is expected to mutate the value
    or to perform some side effect.
    Its result will always be ignored, and its input is passed
    downstream.
    """

    def get_executor(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> T:
            # Update a single value and return it
            func(value)
            return value

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)

    def get_executor_expanded(self) -> StreamCallable:
        def wrapper(value: T, func=self._func) -> T:
            # Apply the function to each item in expanded value and return the
            # original list
            for i in value:
                func(i)
            return value

        return functools.update_wrapper(wrapper=wrapper, wrapped=self._func)


def compose(
    functions: List[StreamFunction],
    allow_filters: bool = True,
    allow_updates: bool = True,
    allow_expands: bool = True,
) -> StreamCallable:
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

    :raises ValueError: if disallowed functions are present in the list of functions.
    """
    composed = None
    has_expanded = False
    for func in functions:
        if not allow_updates and isinstance(func, UpdateFunction):
            raise ValueError("Update functions are not allowed")
        elif not allow_filters and isinstance(func, FilterFunction):
            raise ValueError("Filter functions are not allowed")
        elif not allow_expands and func.expand:
            raise ValueError("Expand functions are not allowed")

        if composed is None:
            composed = func.get_executor()
        else:
            composed = composer(
                func.get_executor()
                if not has_expanded
                else func.get_executor_expanded(),
                composed,
            )

        has_expanded = has_expanded or func.expand

    return composed


def composer(
    outer_func: StreamCallable,
    inner_func: StreamCallable,
) -> Callable[[T], R]:
    """
    A function that wraps two other functions into a closure.
    It passes the result of the inner function as an input to the outer function.

    :return: a function with one argument (value)
    """

    def wrapper(v: T) -> R:
        return outer_func(inner_func(v))

    return functools.update_wrapper(wrapper, outer_func)
