from typing import Dict, Iterable, Mapping, Optional, Union

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer as _ProtobufDeserializer,
    ProtobufSerializer as _ProtobufSerializer,
)
from google.protobuf.json_format import MessageToDict, ParseDict, ParseError
from google.protobuf.message import DecodeError, EncodeError, Message

from quixstreams.schema_registry import SchemaRegistryConfig
from .base import Deserializer, SerializationContext, Serializer
from .exceptions import SerializationError

__all__ = ("ProtobufSerializer", "ProtobufDeserializer")


class ProtobufSerializer(Serializer):
    def __init__(
        self,
        msg_type: Message,
        deterministic: bool = False,
        ignore_unknown_fields: bool = False,
        schema_registry_config: Optional[SchemaRegistryConfig] = None,
    ):
        """
        Serializer that returns data in protobuf format.

        Serialisation from a python dictionary can have a significant performance impact. An alternative is to pass the serializer an object of the `msg_type` class.

        :param msg_type: protobuf message class.
        :param deterministic: If true, requests deterministic serialization of the protobuf, with predictable ordering of map keys
            Default - `False`
        :param ignore_unknown_fields: If True, do not raise errors for unknown fields.
            Default - `False`
        """
        super().__init__()
        self._msg_type = msg_type

        self._deterministic = deterministic
        self._ignore_unknown_fields = ignore_unknown_fields
        self._schema_registry_serializer = None
        if schema_registry_config:
            conf = schema_registry_config.as_dict(plaintext_secrets=True)
            self._schema_registry_serializer = _ProtobufSerializer(
                msg_type=msg_type,
                schema_registry_client=SchemaRegistryClient(conf),
                conf={
                    # The use.deprecated.format has been mandatory since Confluent Kafka version 1.8.2.
                    # https://github.com/confluentinc/confluent-kafka-python/releases/tag/v1.8.2
                    "use.deprecated.format": False,
                },
            )

    def __call__(
        self, value: Union[Dict, Message], ctx: SerializationContext
    ) -> Union[str, bytes]:
        if isinstance(value, self._msg_type):
            msg = value
        else:
            try:
                msg = ParseDict(
                    value,
                    self._msg_type(),
                    ignore_unknown_fields=self._ignore_unknown_fields,
                )
            except ParseError as exc:
                raise SerializationError(str(exc)) from exc

        if self._schema_registry_serializer:
            return self._schema_registry_serializer(msg, ctx=ctx.to_confluent_ctx())

        try:
            return msg.SerializeToString(deterministic=self._deterministic)
        except EncodeError as exc:
            raise SerializationError(str(exc)) from exc


class ProtobufDeserializer(Deserializer):
    def __init__(
        self,
        msg_type: Message,
        use_integers_for_enums: bool = False,
        preserving_proto_field_name: bool = False,
        to_dict: bool = True,
        schema_registry_config: Optional[SchemaRegistryConfig] = None,
    ):
        """
        Deserializer that parses protobuf data into a dictionary suitable for a StreamingDataframe.

        Deserialisation to a python dictionary can have a significant performance impact. You can disable this behavior using `to_dict`, in that case the protobuf message will be used as the StreamingDataframe row value.

        :param msg_type: protobuf message class.
        :param use_integers_for_enums: If true, use integers instead of enum names.
            Default - `False`
        :param preserving_proto_field_name: If True, use the original proto field names as
            defined in the .proto file. If False, convert the field names to
            lowerCamelCase.
            Default - `False`
        :param to_dict: If false, return the protobuf message instead of a dict.
            Default - `True`
        """
        super().__init__()
        self._msg_type = msg_type
        self._to_dict = to_dict

        self._use_integers_for_enums = use_integers_for_enums
        self._preserving_proto_field_name = preserving_proto_field_name

        # Confluent's ProtobufDeserializer is not utilizing the
        # Schema Registry. However, we still accept a fully qualified
        # SchemaRegistryConfig to maintain a unified API and ensure future
        # compatibility in case we choose to bypass Confluent and interact
        # with the Schema Registry directly.
        self._schema_registry_deserializer = None
        if schema_registry_config:
            self._schema_registry_deserializer = _ProtobufDeserializer(
                message_type=msg_type,
                conf={
                    # The use.deprecated.format has been mandatory since Confluent Kafka version 1.8.2.
                    # https://github.com/confluentinc/confluent-kafka-python/releases/tag/v1.8.2
                    "use.deprecated.format": False,
                },
            )

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping, Message]:
        if self._schema_registry_deserializer:
            msg = self._schema_registry_deserializer(value, ctx.to_confluent_ctx())
        else:
            msg = self._msg_type()
            try:
                msg.ParseFromString(value)
            except DecodeError as exc:
                raise SerializationError(str(exc)) from exc

        if not self._to_dict:
            return msg

        return MessageToDict(
            msg,
            always_print_fields_with_no_presence=True,
            use_integers_for_enums=self._use_integers_for_enums,
            preserving_proto_field_name=self._preserving_proto_field_name,
        )
