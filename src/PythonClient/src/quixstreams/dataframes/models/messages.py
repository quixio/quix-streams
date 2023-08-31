import dataclasses
from typing import Optional, Union

from .types import MessageKey, MessageValue, MessageHeadersTuples, MessageHeadersMapping


@dataclasses.dataclass()
class KafkaMessage:
    key: Optional[MessageKey]
    value: Optional[MessageValue]
    headers: Optional[Union[MessageHeadersTuples, MessageHeadersMapping]]
    timestamp: Optional[int] = None
