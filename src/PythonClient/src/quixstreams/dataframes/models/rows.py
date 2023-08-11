import dataclasses
from typing import Mapping, Optional
from .timestamps import MessageTimestamp
from .types import MessageKey, MessageHeaders


@dataclasses.dataclass(eq=True, kw_only=True, slots=True, frozen=True)
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

    def __getitem__(self, item):
        return self.value[item]

    def __bool__(self):
        return bool(self.value)

    def items(self):
        return self.value.items()

    def _clone(self, **kwargs):
        return Row(**{**dataclasses.asdict(self), **kwargs})
