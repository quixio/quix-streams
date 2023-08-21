from typing import (
    Union,
    Protocol,
    List,
    Tuple,
    Optional,
)

MessageKey = Union[str, bytes]
MessageValue = Union[str, bytes]
MessageHeaders = List[Tuple[str, bytes]]


class ConfluentKafkaMessageProto(Protocol):
    """
    An interface of `confluent_kafka.Message`.

    Use it to not depend on exact implementation and simplify testing.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def headers(self, *args, **kwargs) -> Optional[List[Tuple[str, bytes]]]:
        ...

    def key(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        ...

    def offset(self, *args, **kwargs) -> int:
        ...

    def partition(self, *args, **kwargs) -> int:
        ...

    def timestamp(self, *args, **kwargs) -> (int, int):
        ...

    def topic(self, *args, **kwargs) -> str:
        ...

    def value(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        ...

    def latency(self, *args, **kwargs) -> Optional[float]:
        ...

    def leader_epoch(self, *args, **kwargs) -> Optional[int]:
        ...

    def __len__(self) -> int:
        ...
