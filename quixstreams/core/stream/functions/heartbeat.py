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
            if is_heartbeat_message(key, value) and (result := func(timestamp)):
                for new_value, new_key, new_timestamp, new_headers in result:
                    child_executor(new_value, new_key, new_timestamp, new_headers)
            child_executor(value, key, timestamp, headers)

        return wrapper


def is_heartbeat_message(key: Any, value: Any) -> bool:
    return message_context().heartbeat and key is None and value is None
