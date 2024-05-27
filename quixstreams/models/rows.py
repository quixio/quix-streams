from typing import Optional, Union, Any, Mapping

from .messagecontext import MessageContext
from .types import MessageHeadersTuples


class Row:
    """
    Row is a dict-like interface on top of the message data + some Kafka props
    """

    __slots__ = (
        "value",
        "key",
        "timestamp",
        "context",
    )

    def __init__(
        self,
        value: Optional[Any],
        key: Optional[Any],
        timestamp: int,
        context: MessageContext,
    ):
        self.value = value
        self.key = key
        self.timestamp = timestamp
        self.context = context

    @property
    def topic(self) -> str:
        return self.context.topic

    @property
    def partition(self) -> int:
        return self.context.partition

    @property
    def offset(self) -> int:
        return self.context.offset

    @property
    def size(self) -> int:
        return self.context.size

    @property
    def headers(self) -> Optional[Union[Mapping, MessageHeadersTuples]]:
        return self.context.headers

    @property
    def leader_epoch(self) -> Optional[int]:
        return self.context.leader_epoch
