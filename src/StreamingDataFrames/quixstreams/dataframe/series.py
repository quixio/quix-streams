import contextvars
import operator
from typing import Optional, Union, Callable, Container, Any

from typing_extensions import Self

from quixstreams.context import set_message_context
from quixstreams.core.stream.functions import StreamCallable, Apply
from quixstreams.core.stream.stream import Stream
from quixstreams.models.messagecontext import MessageContext
from .base import BaseStreaming

__all__ = ("StreamingSeries",)


class StreamingSeries(BaseStreaming):
    def __init__(
        self,
        name: Optional[str] = None,
        stream: Optional[Stream] = None,
    ):
        if not (name or stream):
            raise ValueError('Either "name" or "stream" must be passed')
        self._stream = stream or Stream(func=Apply(lambda v: v[name]))

    @property
    def stream(self) -> Stream:
        return self._stream

    def apply(self, func: StreamCallable) -> Self:
        """
        Add a callable to the execution list for this series.

        The provided callable should accept a single argument, which will be its input.
        The provided callable should similarly return one output, or None

        :param func: a callable with one argument and one output
        :return: a new `StreamingSeries` with the new callable added
        """
        child = self._stream.add_apply(func)
        return self.__class__(stream=child)

    def compile(
        self,
        allow_filters: bool = True,
        allow_updates: bool = True,
    ) -> StreamCallable:
        """
        Compile all functions of this StreamingSeries into one big closure.

        Closures are more performant than calling all the functions in the
        `StreamingDataFrame` one-by-one.

        :param allow_filters: If False, this function will fail with ValueError if
            the stream has filter functions in the tree. Default - True.
        :param allow_updates: If False, this function will fail with ValueError if
            the stream has update functions in the tree. Default - True.

        :raises ValueError: if disallowed functions are present in the tree of
            underlying `Stream`.

        :return: a function that accepts "value"
            and returns a result of StreamingDataFrame
        """

        return self._stream.compile(
            allow_filters=allow_filters, allow_updates=allow_updates
        )

    def test(self, value: Any, ctx: Optional[MessageContext] = None) -> Any:
        """
        A shorthand to test `StreamingSeries` with provided value
        and `MessageContext`.

        :param value: value to pass through `StreamingSeries`
        :param ctx: instance of `MessageContext`, optional.
            Provide it if the StreamingSeries instance has
            functions calling `get_current_key()`.
            Default - `None`.
        :return: result of `StreamingSeries`
        """
        context = contextvars.copy_context()
        context.run(set_message_context, ctx)
        compiled = self.compile()
        return context.run(compiled, value)

    @classmethod
    def _from_func(cls, func: StreamCallable) -> Self:
        return cls(stream=Stream(Apply(func)))

    def _operation(
        self, other: Union[Self, object], operator_: Callable[[object, object], object]
    ) -> Self:
        self_compiled = self.compile()
        if isinstance(other, self.__class__):
            other_compiled = other.compile()
            return self._from_func(
                func=lambda v, op=operator_: op(self_compiled(v), other_compiled(v))
            )
        else:
            return self._from_func(
                func=lambda v, op=operator_: op(self_compiled(v), other)
            )

    def isin(self, other: Container) -> Self:
        """
        Check if series value is in "other".
        Same as "StreamingSeries in other".

        :param other: a container to check
        :return: new StreamingSeries
        """
        return self._operation(
            other, lambda a, b, contains=operator.contains: contains(b, a)
        )

    def contains(self, other: object) -> Self:
        """
        Check if series value contains "other"
        Same as "other in StreamingSeries".

        :param other: object to check
        :return: new StreamingSeries
        """
        return self._operation(other, operator.contains)

    def is_(self, other: object) -> Self:
        """
        Check if series value refers to the same object as `other`
        :param other: object to check for "is"
        :return: new StreamingSeries
        """
        return self._operation(other, operator.is_)

    def isnot(self, other: object) -> Self:
        """
        Check if series value refers to the same object as `other`
        :param other: object to check for "is"
        :return: new StreamingSeries
        """
        return self._operation(other, operator.is_not)

    def isnull(self) -> Self:
        """
        Check if series value is None

        :return: new StreamingSeries
        """
        return self._operation(None, operator.is_)

    def notnull(self) -> Self:
        """
        Check if series value is not None
        """
        return self._operation(None, operator.is_not)

    def abs(self) -> Self:
        """
        Get absolute value of the series value
        """
        return self.apply(func=lambda v: abs(v))

    def __getitem__(self, item: Union[str, int]) -> Self:
        return self._operation(item, operator.getitem)

    def __mod__(self, other: object) -> Self:
        return self._operation(other, operator.mod)

    def __add__(self, other: object) -> Self:
        return self._operation(other, operator.add)

    def __sub__(self, other: object) -> Self:
        return self._operation(other, operator.sub)

    def __mul__(self, other: object) -> Self:
        return self._operation(other, operator.mul)

    def __truediv__(self, other: object) -> Self:
        return self._operation(other, operator.truediv)

    def __eq__(self, other: object) -> Self:
        return self._operation(other, operator.eq)

    def __ne__(self, other: object) -> Self:
        return self._operation(other, operator.ne)

    def __lt__(self, other: object) -> Self:
        return self._operation(other, operator.lt)

    def __le__(self, other: object) -> Self:
        return self._operation(other, operator.le)

    def __gt__(self, other: object) -> Self:
        return self._operation(other, operator.gt)

    def __ge__(self, other: object) -> Self:
        return self._operation(other, operator.ge)

    def __and__(self, other: object) -> Self:
        """
        Do a logical "and" comparison.

        .. note:: It behaves differently than `pandas`. `pandas` performs
            a bitwise "and" if one of the arguments is a number.
            This function always does a logical "and" instead.
        """
        return self._operation(other, lambda x, y: x and y)

    def __or__(self, other: object) -> Self:
        """
        Do a logical "or" comparison.

        .. note:: It behaves differently than `pandas`. `pandas` performs
            a bitwise "or" if one of the arguments is a number.
            This function always does a logical "or" instead.
        """
        return self._operation(other, lambda x, y: x or y)

    def __invert__(self) -> Self:
        """
        Do a logical "not".

        .. note:: It behaves differently than `pandas`. `pandas` performs
            a bitwise "not" if argument is a number.
            This function always does a logical "not" instead.
        """
        return self.apply(lambda v: not v)
