from typing import Any

from quixstreams.context import message_context

from .base import StreamFunction
from .types import HeartbeatCallback, VoidExecutor

__all__ = ("HeartbeatFunction", "is_heartbeat_message")


class HeartbeatFunction(StreamFunction):
    def __init__(self, func: HeartbeatCallback) -> None:
        super().__init__(func)
        self.func: HeartbeatCallback

    def get_executor(self, *child_executors: VoidExecutor) -> VoidExecutor:
        child_executor = self._resolve_branching(*child_executors)

        func = self.func

        def wrapper(
            value: Any,
            key: Any,
            timestamp: int,
            headers: Any,
        ):
            if is_heartbeat_message(key, value):
                # TODO: Heartbeats may return values (like expired windows)
                func(timestamp)
            child_executor(value, key, timestamp, headers)

        return wrapper


def is_heartbeat_message(key: Any, value: Any) -> bool:
    return message_context().heartbeat and key is None and value is None
