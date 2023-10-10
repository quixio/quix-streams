from typing import Mapping, Optional, Union, List

from .timestamps import MessageTimestamp
from .types import MessageKey, MessageHeadersTuples, SlottedClass


# TODO: add other dict functions like .get() , __contains__  and .copy()
class Row(SlottedClass):
    """
    Row is a dict-like interface on top of the message data + some Kafka props
    """

    __slots__ = (
        "value",
        "topic",
        "partition",
        "offset",
        "size",
        "timestamp",
        "key",
        "headers",
        "latency",
        "leader_epoch",
    )

    def __init__(
        self,
        value: Optional[dict],
        topic: str,
        partition: int,
        offset: int,
        size: int,
        timestamp: MessageTimestamp,
        key: Optional[MessageKey] = None,
        headers: Optional[Union[Mapping, MessageHeadersTuples]] = None,
        latency: Optional[float] = None,
        leader_epoch: Optional[int] = None,
    ):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.size = size
        self.timestamp = timestamp
        self.key = key
        self.headers = headers
        self.latency = latency
        self.leader_epoch = leader_epoch

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
