import functools
from typing import Mapping, Union

from confluent_kafka.serialization import (
    DoubleDeserializer as _DoubleDeserializer,
)
from confluent_kafka.serialization import (
    DoubleSerializer as _DoubleSerializer,
)
from confluent_kafka.serialization import (
    IntegerDeserializer as _IntegerDeserializer,
)
from confluent_kafka.serialization import (
    IntegerSerializer as _IntegerSerializer,
)
from confluent_kafka.serialization import (
    SerializationError as _SerializationError,
)
from confluent_kafka.serialization import (
    StringDeserializer as _StringDeserializer,
)
from confluent_kafka.serialization import (
    StringSerializer as _StringSerializer,
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


def _wrap_serialization_error(func):
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

    def __call__(self, value: bytes, ctx: SerializationContext) -> bytes:
        return value


class BytesSerializer(Serializer):
    """
    A serializer to bypass bytes without any changes
    """

    def __call__(self, value: bytes, ctx: SerializationContext) -> bytes:
        return value


class StringDeserializer(Deserializer):
    def __init__(self, codec: str = "utf_8"):
        """
        Deserializes bytes to strings using the specified encoding.
        :param codec: string encoding

        A wrapper around `confluent_kafka.serialization.StringDeserializer`.
        """
        super().__init__()
        self._codec = codec
        self._deserializer = _StringDeserializer(codec=self._codec)

    @_wrap_serialization_error
    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[str, Mapping[str, str]]:
        return self._deserializer(value=value)


class IntegerDeserializer(Deserializer):
    """
    Deserializes bytes to integers.

    A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.
    """

    def __init__(self):
        super().__init__()
        self._deserializer = _IntegerDeserializer()

    @_wrap_serialization_error
    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[int, Mapping[str, int]]:
        return self._deserializer(value=value)


class DoubleDeserializer(Deserializer):
    """
    Deserializes float to IEEE 764 binary64.

    A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.
    """

    def __init__(self):
        super().__init__()
        self._deserializer = _DoubleDeserializer()

    @_wrap_serialization_error
    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[float, Mapping[str, float]]:
        return self._deserializer(value=value)


class StringSerializer(Serializer):
    def __init__(self, codec: str = "utf_8"):
        """
        Serializes strings to bytes using the specified encoding.
        :param codec: string encoding
        """
        self._serializer = _StringSerializer(codec=codec)

    @_wrap_serialization_error
    def __call__(self, value: str, ctx: SerializationContext) -> bytes:
        return self._serializer(obj=value)


class IntegerSerializer(Serializer):
    """
    Serializes integers to bytes
    """

    def __init__(self):
        self._serializer = _IntegerSerializer()

    @_wrap_serialization_error
    def __call__(self, value: int, ctx: SerializationContext) -> bytes:
        return self._serializer(obj=value)


class DoubleSerializer(Serializer):
    """
    Serializes floats to bytes
    """

    def __init__(self):
        self._serializer = _DoubleSerializer()

    @_wrap_serialization_error
    def __call__(self, value: float, ctx: SerializationContext) -> bytes:
        return self._serializer(obj=value)
