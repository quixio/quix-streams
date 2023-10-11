from typing import Optional, Union

from .types import (
    MessageKey,
    MessageValue,
    MessageHeadersTuples,
    MessageHeadersMapping,
)


class KafkaMessage:
    __slots__ = ("key", "value", "headers", "timestamp")

    def __init__(
        self,
        key: Optional[MessageKey],
        value: Optional[MessageValue],
        headers: Optional[Union[MessageHeadersTuples, MessageHeadersMapping]],
        timestamp: Optional[int] = None,
    ):
        self.key = key
        self.value = value
        self.headers = headers
        self.timestamp = timestamp
