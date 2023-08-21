import struct
from typing import Optional, List, Tuple, Union


def float_to_bytes(value: float) -> bytes:
    return struct.pack(">d", value)


def int_to_bytes(value: int) -> bytes:
    return struct.pack(">i", value)


class ConfluentKafkaMessageStub:
    """
    A stub object to mock `confluent_kafka.Message`.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def __init__(
        self,
        topic: str = "test",
        partition: int = 0,
        offset: int = 0,
        timestamp: Tuple[int, int] = (1, 123),
        key: bytes = None,
        value: bytes = None,
        headers: List[Tuple[str, bytes]] = None,
        latency: float = None,
        leader_epoch: int = None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._key = key
        self._value = value
        self._headers = headers or []
        self._latency = latency
        self._leader_epoch = leader_epoch

    def headers(self, *args, **kwargs) -> Optional[List[Tuple[str, bytes]]]:
        return self._headers

    def key(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        return self._key

    def offset(self, *args, **kwargs) -> int:
        return self._offset

    def partition(self, *args, **kwargs) -> int:
        return self._partition

    def timestamp(self, *args, **kwargs) -> (int, int):
        return self._timestamp

    def topic(self, *args, **kwargs) -> str:
        return self._topic

    def value(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        return self._value

    def latency(self, *args, **kwargs) -> Optional[float]:
        return self._latency

    def leader_epoch(self, *args, **kwargs) -> Optional[int]:
        return self._leader_epoch

    def __len__(self) -> int:
        return len(self._value)
