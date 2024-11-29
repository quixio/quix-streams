from typing import List, Mapping, Optional, Sequence, Tuple, Union

from typing_extensions import Protocol

MessageKey = Optional[Union[str, bytes]]
MessageValue = Union[str, bytes]

HeadersValue = Union[str, bytes]
HeadersMapping = Mapping[str, HeadersValue]
HeadersTuples = Sequence[Tuple[str, HeadersValue]]
Headers = Union[HeadersTuples, HeadersMapping]

KafkaHeaders = Optional[List[Tuple[str, bytes]]]


class ConfluentKafkaMessageProto(Protocol):
    """
    An interface of `confluent_kafka.Message`.

    Use it to not depend on exact implementation and simplify testing.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def headers(self, *args, **kwargs) -> KafkaHeaders: ...

    def key(self, *args, **kwargs) -> MessageKey: ...

    def offset(self, *args, **kwargs) -> int: ...

    def partition(self, *args, **kwargs) -> Optional[int]: ...

    def timestamp(self, *args, **kwargs) -> Tuple[int, int]: ...

    def topic(self, *args, **kwargs) -> str: ...

    def value(self, *args, **kwargs) -> Optional[MessageValue]: ...

    def latency(self, *args, **kwargs) -> Optional[float]: ...

    def leader_epoch(self, *args, **kwargs) -> Optional[int]: ...

    def __len__(self) -> int: ...
