from typing import Mapping, Optional, Union, List

from .timestamps import MessageTimestamp
from .types import MessageKey, MessageHeadersTuples
from copy import deepcopy, copy


# TODO: add other dict functions like .get() , __contains__  and .copy()
class Row:
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

    # TODO: Maybe include headers here for if/when it's a dict?
    _copy_map = {
        "value": lambda self, k: deepcopy(getattr(self, k)),
        "timestamp": lambda self, k: copy(getattr(self, k)),
    }

    def __init__(
        self,
        value: Optional[dict],
        topic: Optional[str] = None,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        size: Optional[int] = None,
        timestamp: Optional[MessageTimestamp] = None,
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

    def clone_new_value(self, value):
        return self.__class__(
            value=value,
            topic=self.topic,
            partition=self.partition,
            offset=self.offset,
            size=self.size,
            timestamp=copy(self.timestamp),
            key=self.key,
            headers=deepcopy(self.headers),
            latency=self.latency,
            leader_epoch=self.leader_epoch,
        )

    def clone(self, **kwargs):
        """
        Manually clone the Row; doing it this way is much faster than doing a deepcopy
        on the entire Row object.
        """
        for k in self.__slots__:
            if k not in kwargs:
                kwargs[k] = self._copy_map.get(k, getattr)(self, k)
        return self.__class__(**kwargs)
