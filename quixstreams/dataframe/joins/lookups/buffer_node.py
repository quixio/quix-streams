"""
The buffer's `Stream` node.

Separated from `buffer_operator.py` only because it belongs to a different
layer: everything here is about how the operator is wired into a composed
`Stream`, nothing about what the buffer does with a record.
"""

from typing import Any, cast

from quixstreams.core.stream import (
    TransformExpandedCallback,
    TransformFunction,
    VoidExecutor,
)

from .buffer_operator import BufferOperator

__all__ = ("BufferTransformFunction",)


class BufferTransformFunction(TransformFunction):
    """
    The node `join_lookup(..., buffer=...)` appends to the `Stream`.

    Identical in behaviour to an expanded `TransformFunction`, except that it
    hands the resolved child executor to the operator at compose time. That is
    the only reason it exists: `TransformFunction`'s expanded wrapper expands an
    iterable returned from a callback *applied to an input record*, and on a
    deadline tick there is no input record and therefore no callback invocation
    to return from.

    Branching is resolved once and shared by both paths, so a tick-emitted record
    fans out to multiple branches with exactly the pickle-copy semantics a
    record-path emission gets. `Stream.compose()` may run more than once (tests,
    several roots); the binding is idempotent-by-overwrite and the last call
    wins, which is by construction the executor the record path is using too.
    """

    def __init__(self, operator: BufferOperator) -> None:
        # The operator emits six-field `Emission`s, not the four-tuples
        # `TransformExpandedCallback` describes; the extra two carry the
        # record's origin, which only the tick reads. The cast keeps the base
        # class's `self.func` populated - `Stream` machinery reads it - without
        # widening the public callback type for everyone else.
        super().__init__(cast(TransformExpandedCallback, operator), expand=True)
        self._operator = operator

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        downstream = self._resolve_branching(*child_executors)
        self._operator.bind_downstream(downstream)

        def wrapper(value: Any, key: Any, timestamp: int, headers: Any) -> None:
            for emission in self._operator(value, key, timestamp, headers):
                downstream(
                    emission.value,
                    emission.key,
                    emission.timestamp,
                    emission.headers,
                )

        return wrapper
