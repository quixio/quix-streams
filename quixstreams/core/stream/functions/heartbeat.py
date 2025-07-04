from functools import wraps
from typing import Any

from quixstreams.context import message_context

from .base import StreamFunction
from .types import VoidExecutor

__all__ = ("HeartbeatFunction", "is_heartbeat_message", "ignore_heartbeat")


class HeartbeatFunction(StreamFunction):
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


def ignore_heartbeat(func):
    """
    Decorator that wraps a function to return early if the message is a heartbeat.

    The decorated function should expect (value, key, timestamp, headers) parameters.
    If is_heartbeat_message(key, value) returns True, the function returns early
    without executing the wrapped function.
    """

    @wraps(func)
    def wrapper(value: Any, key: Any, timestamp: int, headers: Any):
        if is_heartbeat_message(key, value):
            return
        return func(value, key, timestamp, headers)

    return wrapper
