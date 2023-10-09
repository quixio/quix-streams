from typing import (
    Union,
    Protocol,
    List,
    Tuple,
    Optional,
    Mapping,
)

MessageKey = Union[str, bytes]
MessageValue = Union[str, bytes]
MessageHeadersTuples = List[Tuple[str, bytes]]
MessageHeadersMapping = Mapping[str, Union[str, bytes, None]]


class ConfluentKafkaMessageProto(Protocol):
    """
    An interface of `confluent_kafka.Message`.

    Use it to not depend on exact implementation and simplify testing.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def headers(self, *args, **kwargs) -> Optional[List[Tuple[str, bytes]]]:
        pass

    def key(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        pass

    def offset(self, *args, **kwargs) -> int:
        pass

    def partition(self, *args, **kwargs) -> int:
        pass

    def timestamp(self, *args, **kwargs) -> (int, int):
        pass

    def topic(self, *args, **kwargs) -> str:
        pass

    def value(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        pass

    def latency(self, *args, **kwargs) -> Optional[float]:
        pass

    def leader_epoch(self, *args, **kwargs) -> Optional[int]:
        pass

    def __len__(self) -> int:
        pass
