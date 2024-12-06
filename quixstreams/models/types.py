from typing import List, Optional, Sequence, Tuple, Union

from confluent_kafka import KafkaError
from typing_extensions import Protocol

MessageKey = Optional[Union[str, bytes]]
MessageValue = Optional[Union[str, bytes]]

HeadersValue = Union[str, bytes]
HeadersMapping = dict[str, HeadersValue]
HeadersTuple = Tuple[str, HeadersValue]
HeadersTuples = Sequence[HeadersTuple]
Headers = Union[HeadersTuples, HeadersMapping]

KafkaHeaders = Optional[List[Tuple[str, bytes]]]


class RawConfluentKafkaMessageProto(Protocol):
    """
    An interface of `confluent_kafka.Message`.

    Use it to not depend on exact implementation and simplify testing and type hints.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.
    """

    def headers(self, *args, **kwargs) -> KafkaHeaders: ...

    def key(self, *args, **kwargs) -> MessageKey: ...

    def offset(self, *args, **kwargs) -> Optional[int]: ...

    def partition(self, *args, **kwargs) -> Optional[int]: ...

    def timestamp(self, *args, **kwargs) -> Tuple[int, int]: ...

    def topic(self, *args, **kwargs) -> str: ...

    def value(self, *args, **kwargs) -> MessageValue: ...

    def latency(self, *args, **kwargs) -> Optional[float]: ...

    def leader_epoch(self, *args, **kwargs) -> Optional[int]: ...

    def error(self) -> Optional[KafkaError]: ...

    def __len__(self) -> int: ...


class SuccessfulConfluentKafkaMessageProto(Protocol):
    """
    An interface of `confluent_kafka.Message` for successful message (messages that don't include an error)

    Use it to not depend on exact implementation and simplify testing and type hints.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def headers(self, *args, **kwargs) -> KafkaHeaders: ...

    def key(self, *args, **kwargs) -> MessageKey: ...

    def offset(self, *args, **kwargs) -> int: ...

    def partition(self, *args, **kwargs) -> int: ...

    def timestamp(self, *args, **kwargs) -> Tuple[int, int]: ...

    def topic(self, *args, **kwargs) -> str: ...

    def value(self, *args, **kwargs) -> MessageValue: ...

    def latency(self, *args, **kwargs) -> Optional[float]: ...

    def leader_epoch(self, *args, **kwargs) -> Optional[int]: ...

    def error(self) -> None: ...

    def __len__(self) -> int: ...
