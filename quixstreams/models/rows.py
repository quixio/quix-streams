from typing import Optional, Any

from .messagecontext import MessageContext
from .types import MessageHeadersTuples


class Row:
    __slots__ = (
        "value",
        "key",
        "timestamp",
        "headers",
        "context",
    )

    def __init__(
        self,
        value: Optional[Any],
        key: Optional[Any],
        timestamp: int,
        context: MessageContext,
        headers: Optional[MessageHeadersTuples] = None,
    ):
        self.value = value
        self.key = key
        self.timestamp = timestamp
        self.context = context
        self.headers = headers

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
    def leader_epoch(self) -> Optional[int]:
        return self.context.leader_epoch
