import abc
from typing import Any, Union

from confluent_kafka.serialization import (
    MessageField,
)
from confluent_kafka.serialization import (
    SerializationContext as _SerializationContext,
)
from typing_extensions import Literal, TypeAlias

from ..types import Headers, HeadersMapping, KafkaHeaders

__all__ = (
    "SerializationContext",
    "MessageField",
    "Deserializer",
    "Serializer",
    "SerializerType",
    "DeserializerType",
)


class SerializationContext(_SerializationContext):
    """
    Provides additional context for message serialization/deserialization.

    Every `Serializer` and `Deserializer` receives an instance of `SerializationContext`
    """

    __slots__ = ("topic", "field", "headers")

    def __init__(
        self,
        topic: str,
        field: str,
        headers: Union[KafkaHeaders, Headers] = None,
    ) -> None:
        self.topic = topic
        self.field = field
        self.headers = headers


class Deserializer(abc.ABC):
    def __init__(self, *args, **kwargs):
        """
        A base class for all Deserializers
        """

    @property
    def split_values(self) -> bool:
        """
        Return True if the deserialized message should be considered as Iterable
        and each item in it should be processed as a separate message.
        """
        return False

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> Any: ...


class Serializer(abc.ABC):
    """
    A base class for all Serializers
    """

    @property
    def extra_headers(self) -> HeadersMapping:
        """
        Informs producer to set additional headers
        for the message it will be serializing

        Must return a dictionary with headers.
        Keys must be strings, and values must be strings, bytes or None.

        :return: dict with headers
        """
        return {}

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> Union[str, bytes]: ...


SerializerStr: TypeAlias = Literal[
    "str",
    "string",
    "bytes",
    "double",
    "int",
    "integer",
    "json",
    "quix_timeseries",
    "quix_events",
]

SerializerType: TypeAlias = Union[SerializerStr, Serializer]

DeserializerStr: TypeAlias = Literal[
    "str",
    "string",
    "bytes",
    "double",
    "int",
    "integer",
    "json",
    "quix",
]
DeserializerType: TypeAlias = Union[DeserializerStr, Deserializer]
