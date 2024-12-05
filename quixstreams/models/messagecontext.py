from typing import Optional


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
        "_headers",
        "_leader_epoch",
    )

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        size: int,
        leader_epoch: Optional[int] = None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._size = size
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
    def leader_epoch(self) -> Optional[int]:
        return self._leader_epoch
