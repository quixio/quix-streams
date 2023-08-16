import dataclasses
from typing import Mapping, Optional, Union, List
from .timestamps import MessageTimestamp
from .types import MessageKey, MessageHeaders


@dataclasses.dataclass(eq=True, kw_only=True, slots=True)
class Row:
    """
    Row is a dict-like interface on top of the message data + some Kafka props
    """

    value: Optional[dict]
    topic: str
    partition: int
    offset: int
    size: int
    timestamp: MessageTimestamp
    key: Optional[MessageKey] = None
    headers: Optional[Union[Mapping, MessageHeaders]] = None
    latency: Optional[float] = None
    leader_epoch: Optional[int] = None

    def __getitem__(self, item: Union[str, List[str]]):
        if isinstance(item, list):
            return {k: self.value[k] for k in item}
        return self.value[item]

    def __setitem__(self, key: str, value: any):
        self.value[key] = value

    def keys(self):
        """
        Also allows unpacking row.value via **row
        """
        return self.value.keys()

    def values(self):
        return self.value.values()

    def items(self):
        return self.value.items()