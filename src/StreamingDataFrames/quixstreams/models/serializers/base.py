import abc
from typing import Optional, Any, Union
from typing_extensions import TypeAlias, Literal


from confluent_kafka.serialization import (
    SerializationContext as _SerializationContext,
    MessageField,
)

from ..types import MessageHeadersTuples, MessageHeadersMapping

__all__ = (
    "SerializationContext",
    "Deserializer",
    "Serializer",
    "SerializerType",
    "DeserializerType",
)


class SerializationContext:
    """
    Provides additional context for message serialization/deserialization.

    Every `Serializer` and `Deserializer` receives an instance of `SerializationContext`
    """

    __slots__ = ("topic", "headers")

    def __init__(self, topic: str, headers: Optional[MessageHeadersTuples] = None):
        self.topic = topic
        self.headers = headers

    def to_confluent_ctx(self, field: MessageField) -> _SerializationContext:
        """
        Convert `SerializationContext` to `confluent_kafka.SerializationContext`
        in order to re-use serialization already provided by `confluent_kafka` library.
        :param field: instance of `confluent_kafka.serialization.MessageField`
        :return: instance of `confluent_kafka.serialization.SerializationContext`
        """
        return _SerializationContext(
            field=field, topic=self.topic, headers=self.headers
        )


class Deserializer(abc.ABC):
    def __init__(self, column_name: Optional[str] = None, *args, **kwargs):
        """
        A base class for all Deserializers

        :param column_name: if provided, the deserialized value will be wrapped into
            dictionary with `column_name` as a key.
        """
        self.column_name = column_name

    @property
    def split_values(self) -> bool:
        """
        Return True if the deserialized message should be considered as Iterable
        and each item in it should be processed as a separate message.
        """
        return False

    def _to_dict(self, value: Any) -> Union[Any, dict]:
        if self.column_name:
            return {self.column_name: value}
        return value

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> Any:
        ...


class Serializer(abc.ABC):
    """
    A base class for all Serializers
    """

    @property
    def extra_headers(self) -> MessageHeadersMapping:
        """
        Informs producer to set additional headers
        for the message it will be serializing

        Must return a dictionary with headers.
        Keys must be strings, and values must be strings, bytes or None.

        :return: dict with headers
        """
        return {}

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> bytes:
        ...


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
