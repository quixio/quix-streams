from typing import Optional

from .types import (
    Headers,
    MessageKey,
    MessageValue,
)


class KafkaMessage:
    __slots__ = ("key", "value", "headers", "timestamp")

    def __init__(
        self,
        key: MessageKey,
        value: Optional[MessageValue],
        headers: Optional[Headers],
        timestamp: Optional[int] = None,
    ):
        self.key = key
        self.value = value
        self.headers = headers
        self.timestamp = timestamp
