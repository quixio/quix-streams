import dataclasses
from typing import Optional

from .types import MessageKey, MessageValue


@dataclasses.dataclass()
class KafkaMessage:
    key: Optional[MessageKey]
    value: Optional[MessageValue]
    headers: Optional[dict]
    timestamp: Optional[int] = None
