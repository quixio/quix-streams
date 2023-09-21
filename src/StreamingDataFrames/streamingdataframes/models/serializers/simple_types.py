import functools
from typing import Optional, Union, Mapping

from confluent_kafka.serialization import (
    StringDeserializer as _StringDeserializer,
    StringSerializer as _StringSerializer,
    IntegerDeserializer as _IntegerDeserializer,
    IntegerSerializer as _IntegerSerializer,
    DoubleDeserializer as _DoubleDeserializer,
    DoubleSerializer as _DoubleSerializer,
    SerializationError as _SerializationError,
)

from .base import Deserializer, SerializationContext, Serializer
from .exceptions import SerializationError

__all__ = (
    "BytesDeserializer",
    "BytesSerializer",
    "StringSerializer",
    "StringDeserializer",
    "IntegerSerializer",
    "IntegerDeserializer",
    "DoubleSerializer",
    "DoubleDeserializer",
)


def wrap_serialization_error(func):
    """
    A decorator to wrap `confluent_kafka.SerializationError` into our own type.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (_SerializationError, AttributeError) as exc:
            raise SerializationError(str(exc)) from exc

    return wrapper


class BytesDeserializer(Deserializer):
    """
    A deserializer to bypass bytes without any changes
    """

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[bytes, Mapping[str, bytes]]:
        return self._to_dict(value)


class BytesSerializer(Serializer):
    """
    A serializer to bypass bytes without any changes
    """

    def __call__(self, value: bytes, ctx: SerializationContext) -> bytes:
        return value


class StringDeserializer(Deserializer):
    def __init__(self, column_name: Optional[str] = None, codec: str = "utf_8"):
        """
        Deserializes bytes to strings using the specified encoding.
        :param codec: string encoding

        A wrapper around `confluent_kafka.serialization.StringDeserializer`.
        """
        super().__init__(column_name=column_name)
        self._codec = codec
        self._deserializer = _StringDeserializer(codec=self._codec)

    @wrap_serialization_error
    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[str, Mapping[str, str]]:
        deserialized = self._deserializer(value=value)
        return self._to_dict(deserialized)


class IntegerDeserializer(Deserializer):
    """
    Deserializes bytes to integers.

    A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.
    """

    def __init__(self, column_name: Optional[str] = None):
        super().__init__(column_name=column_name)
        self._deserializer = _IntegerDeserializer()

    @wrap_serialization_error
    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[int, Mapping[str, int]]:
        deserialized = self._deserializer(value=value)
        return self._to_dict(deserialized)


class DoubleDeserializer(Deserializer):
    """
    Deserializes float to IEEE 764 binary64.

    A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.
    """

    def __init__(self, column_name: Optional[str] = None):
        super().__init__(column_name=column_name)
        self._deserializer = _DoubleDeserializer()

    @wrap_serialization_error
    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[float, Mapping[str, float]]:
        deserialized = self._deserializer(value=value)
        return self._to_dict(deserialized)


class StringSerializer(Serializer):
    def __init__(self, codec: str = "utf_8"):
        """
        Serializes strings to bytes using the specified encoding.
        :param codec: string encoding
        """
        self._serializer = _StringSerializer(codec=codec)

    @wrap_serialization_error
    def __call__(self, value: str, ctx: SerializationContext) -> bytes:
        return self._serializer(obj=value)


class IntegerSerializer(Serializer):
    """
    Serializes integers to bytes
    """

    def __init__(self):
        self._serializer = _IntegerSerializer()

    @wrap_serialization_error
    def __call__(self, value: int, ctx: SerializationContext) -> bytes:
        return self._serializer(obj=value)


class DoubleSerializer(Serializer):
    """
    Serializes floats to bytes
    """

    def __init__(self):
        self._serializer = _DoubleSerializer()

    @wrap_serialization_error
    def __call__(self, value: float, ctx: SerializationContext) -> bytes:
        return self._serializer(obj=value)
