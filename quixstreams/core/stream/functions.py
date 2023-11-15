import enum
import functools
from typing import TypeVar, Callable, Protocol, Optional

__all__ = (
    "StreamCallable",
    "SupportsBool",
    "Filtered",
    "Apply",
    "Update",
    "Filter",
    "is_filter_function",
    "is_update_function",
    "is_apply_function",
    "get_stream_function_type",
)

R = TypeVar("R")
T = TypeVar("T")

StreamCallable = Callable[[T], R]


class SupportsBool(Protocol):
    def __bool__(self) -> bool:
        ...


class Filtered(Exception):
    ...


class StreamFunctionType(enum.IntEnum):
    FILTER = 1
    UPDATE = 2
    APPLY = 3


_STREAM_FUNC_TYPE_ATTR = "__stream_function_type__"


def get_stream_function_type(func: StreamCallable) -> Optional[StreamFunctionType]:
    return getattr(func, _STREAM_FUNC_TYPE_ATTR, None)


def set_stream_function_type(func: StreamCallable, type_: StreamFunctionType):
    setattr(func, _STREAM_FUNC_TYPE_ATTR, type_)


def is_filter_function(func: StreamCallable) -> bool:
    func_type = get_stream_function_type(func)
    return func_type is not None and func_type == StreamFunctionType.FILTER


def is_update_function(func: StreamCallable) -> bool:
    func_type = get_stream_function_type(func)
    return func_type is not None and func_type == StreamFunctionType.UPDATE


def is_apply_function(func: StreamCallable) -> bool:
    func_type = get_stream_function_type(func)
    return func_type is not None and func_type == StreamFunctionType.APPLY


def Filter(func: Callable[[T], SupportsBool]) -> StreamCallable:
    """
    Wraps function into a "Filter" function.
    The result of a Filter function is interpreted as boolean.
    If it's `True`, the input will be return downstream.
    If it's `False`, the `Filtered` exception will be raised to signal that the
    value is filtered out.

    :param func: a function to filter value
    :return: a Filter function
    """

    def wrapper(value: T) -> T:
        result = func(value)
        if not result:
            raise Filtered()
        return value

    wrapper = functools.update_wrapper(wrapper=wrapper, wrapped=func)
    set_stream_function_type(wrapper, StreamFunctionType.FILTER)
    return wrapper


def Update(func: StreamCallable) -> StreamCallable:
    """
    Wrap a function into "Update" function.

    The provided function is expected to mutate the value.
    Its result will always be ignored, and its input is passed
    downstream.

    :param func: a function to mutate values
    :return: an Update function
    """

    def wrapper(value: T) -> T:
        func(value)
        return value

    wrapper = functools.update_wrapper(wrapper=wrapper, wrapped=func)
    set_stream_function_type(wrapper, StreamFunctionType.UPDATE)
    return wrapper


def Apply(func: StreamCallable) -> StreamCallable:
    """
    Wrap a function into "Apply" function.

    The provided function is expected to return a new value based on input,
    and its result will always be passed downstream.

    :param func: a function to generate a new value
    :return: an Apply function
    """

    def wrapper(value: T) -> R:
        return func(value)

    wrapper = functools.update_wrapper(wrapper=wrapper, wrapped=func)
    set_stream_function_type(wrapper, StreamFunctionType.APPLY)
    return wrapper
