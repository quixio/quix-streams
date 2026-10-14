from typing import Any, cast

from quixstreams.context import copy_context, message_context, set_message_context
from quixstreams.core.stream import (
    TransformExpandedCallback,
    TransformFunction,
    VoidExecutor,
)
from quixstreams.models.messagecontext import MessageContext

from .buffer_envelope import Emission
from .buffer_operator import BufferOperator

__all__ = ("BufferTransformFunction",)


class BufferTransformFunction(TransformFunction):
    """The stream node wrapping a `BufferOperator`."""

    def __init__(self, operator: BufferOperator) -> None:
        super().__init__(cast(TransformExpandedCallback, operator), expand=True)
        self._operator = operator

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        """
        :param child_executors: The executors of everything after this node.
        :return: The executor to install, which also binds the deadline tick's
            downstream for the lifetime of the composed dataframe.
        """
        downstream = self._resolve_branching(*child_executors)
        self._operator.bind_downstream(downstream)

        def emit(emissions: list[Emission], origin: MessageContext) -> None:
            for emission in emissions:
                set_message_context(
                    MessageContext(
                        topic=emission.topic or origin.topic,
                        partition=origin.partition,
                        offset=emission.offset,
                        size=origin.size,
                    )
                )
                downstream(
                    emission.value,
                    emission.key,
                    emission.timestamp,
                    emission.headers,
                )

        def wrapper(value: Any, key: Any, timestamp: int, headers: Any) -> None:
            emissions = self._operator(value, key, timestamp, headers)
            if emissions:
                # A copied context, because `emit` overwrites the message context
                # per emission and the caller's must survive this node.
                copy_context().run(emit, emissions, message_context())

        return wrapper
