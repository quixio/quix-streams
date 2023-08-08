import dataclasses
from typing import Mapping, Optional

from .timestamps import MessageTimestamp
from .types import MessageKey, MessageHeaders


@dataclasses.dataclass(eq=True, kw_only=True, slots=True)
class Row:
    """
    Row is a dict-like interface on top of the message data + some Kafka props
    """

    value: Optional[Mapping]
    topic: str
    partition: int
    offset: int
    size: int
    timestamp: MessageTimestamp
    key: Optional[MessageKey] = None
    headers: Optional[Mapping | MessageHeaders] = None
    latency: Optional[float] = None
    leader_epoch: Optional[int] = None
