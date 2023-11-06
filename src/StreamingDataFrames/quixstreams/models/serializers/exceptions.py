from confluent_kafka.error import (
    KeyDeserializationError as _KeyDeserializationError,
    KeySerializationError as _KeySerializationError,
    ValueSerializationError as _ValueSerializationError,
    ValueDeserializationError as _ValueDeserializationError,
    SerializationError as _SerializationError,
)

from quixstreams import exceptions

__all__ = (
    "SerializationError",
    "KeyDeserializationError",
    "KeySerializationError",
    "ValueSerializationError",
    "ValueDeserializationError",
    "SerializerIsNotProvidedError",
    "DeserializerIsNotProvidedError",
    "IgnoreMessage",
)


class SerializationError(exceptions.QuixException, _SerializationError):
    ...


class KeyDeserializationError(exceptions.QuixException, _KeyDeserializationError):
    ...


class KeySerializationError(exceptions.QuixException, _KeySerializationError):
    ...


class ValueSerializationError(exceptions.QuixException, _ValueSerializationError):
    ...


class ValueDeserializationError(exceptions.QuixException, _ValueDeserializationError):
    ...


class SerializerIsNotProvidedError(exceptions.QuixException):
    ...


class DeserializerIsNotProvidedError(exceptions.QuixException):
    ...


class IgnoreMessage(exceptions.QuixException):
    """
    Raise this exception from Deserializer.__call__ in order to ignore the processing
    of the particular message.
    """
