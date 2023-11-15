from typing import Any, Optional, Union, Mapping

from .messages import MessageHeadersTuples
from .timestamps import MessageTimestamp


class MessageContext:
    """
    An object with Kafka message properties.

    It is made pseudo-immutable (i.e. public attributes don't have setters), and
    it should not be mutated during message processing.
    """

    __slots__ = (
        "_topic",
        "_partition",
        "_offset",
        "_size",
        "_timestamp",
        "_key",
        "_headers",
        "_latency",
        "_leader_epoch",
    )

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        size: int,
        timestamp: MessageTimestamp,
        key: Optional[Any] = None,
        headers: Optional[Union[Mapping, MessageHeadersTuples]] = None,
        latency: Optional[float] = None,
        leader_epoch: Optional[int] = None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._size = size
        self._timestamp = timestamp
        self._key = key
        self._headers = headers
        self._latency = latency
        self._leader_epoch = leader_epoch

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def size(self) -> int:
        return self._size

    @property
    def timestamp(self) -> MessageTimestamp:
        return self._timestamp

    @property
    def key(self) -> Optional[Any]:
        return self._key

    @property
    def headers(self) -> Optional[Union[Mapping, MessageHeadersTuples]]:
        return self._headers

    @property
    def latency(self) -> Optional[float]:
        return self._latency

    @property
    def leader_epoch(self) -> Optional[int]:
        return self._leader_epoch
