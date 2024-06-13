from typing import Union, List, Tuple, Optional, Dict

from typing_extensions import Protocol

MessageKey = Union[str, bytes]
MessageValue = Union[str, bytes]
HeaderValue = Optional[Union[str, bytes]]
MessageHeadersTuples = List[Tuple[str, HeaderValue]]
MessageHeadersMapping = Dict[str, HeaderValue]
Headers = Union[MessageHeadersTuples, MessageHeadersMapping]


class ConfluentKafkaMessageProto(Protocol):
    """
    An interface of `confluent_kafka.Message`.

    Use it to not depend on exact implementation and simplify testing.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def headers(self, *args, **kwargs) -> Optional[MessageHeadersTuples]: ...

    def key(self, *args, **kwargs) -> Optional[MessageKey]: ...

    def offset(self, *args, **kwargs) -> int: ...

    def partition(self, *args, **kwargs) -> int: ...

    def timestamp(self, *args, **kwargs) -> Tuple[int, int]: ...

    def topic(self, *args, **kwargs) -> str: ...

    def value(self, *args, **kwargs) -> Optional[MessageValue]: ...

    def latency(self, *args, **kwargs) -> Optional[float]: ...

    def leader_epoch(self, *args, **kwargs) -> Optional[int]: ...

    def __len__(self) -> int: ...
