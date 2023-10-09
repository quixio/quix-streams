from confluent_kafka.error import (
    KeyDeserializationError as _KeyDeserializationError,
    KeySerializationError as _KeySerializationError,
    ValueSerializationError as _ValueSerializationError,
    ValueDeserializationError as _ValueDeserializationError,
    SerializationError as _SerializationError,
)

from streamingdataframes import exceptions

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
    pass


class KeyDeserializationError(exceptions.QuixException, _KeyDeserializationError):
    pass


class KeySerializationError(exceptions.QuixException, _KeySerializationError):
    pass


class ValueSerializationError(exceptions.QuixException, _ValueSerializationError):
    pass


class ValueDeserializationError(exceptions.QuixException, _ValueDeserializationError):
    pass


class SerializerIsNotProvidedError(exceptions.QuixException):
    pass


class DeserializerIsNotProvidedError(exceptions.QuixException):
    pass


class IgnoreMessage(exceptions.QuixException):
    """
    Raise this exception from Deserializer.__call__ in order to ignore the processing
    of the particular message.
    """
