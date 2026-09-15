from typing import Any, cast

from quixstreams.core.stream import (
    TransformExpandedCallback,
    TransformFunction,
    VoidExecutor,
)

from .buffer_operator import BufferOperator

__all__ = ("BufferTransformFunction",)


class BufferTransformFunction(TransformFunction):
    def __init__(self, operator: BufferOperator) -> None:
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
