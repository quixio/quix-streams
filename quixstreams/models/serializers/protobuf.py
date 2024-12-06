from typing import Dict, Iterable, Mapping, Optional, Type, Union

from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer as _ProtobufDeserializer,
)
from confluent_kafka.schema_registry.protobuf import (
    ProtobufSerializer as _ProtobufSerializer,
)
from confluent_kafka.serialization import SerializationError as _SerializationError
from google.protobuf.json_format import MessageToDict, ParseDict, ParseError
from google.protobuf.message import DecodeError, EncodeError, Message

from .base import Deserializer, SerializationContext, Serializer
from .exceptions import SerializationError
from .schema_registry import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)

__all__ = ("ProtobufSerializer", "ProtobufDeserializer")


class ProtobufSerializer(Serializer):
    def __init__(
        self,
        msg_type: Type[Message],
        deterministic: bool = False,
        ignore_unknown_fields: bool = False,
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
        schema_registry_serialization_config: Optional[
            SchemaRegistrySerializationConfig
        ] = None,
    ):
        """
        Serializer that returns data in protobuf format.

        Serialisation from a python dictionary can have a significant performance impact. An alternative is to pass the serializer an object of the `msg_type` class.

        :param msg_type: protobuf message class.
        :param deterministic: If true, requests deterministic serialization of the protobuf, with predictable ordering of map keys
            Default - `False`
        :param ignore_unknown_fields: If True, do not raise errors for unknown fields.
            Default - `False`
        :param schema_registry_client_config: If provided, serialization is offloaded to Confluent's ProtobufSerializer.
            Default - `None`
        :param schema_registry_serialization_config: Additional configuration for Confluent's ProtobufSerializer.
            Default - `None`
            >***NOTE:*** `schema_registry_client_config` must also be set.
        """
        super().__init__()
        self._msg_type = msg_type

        self._deterministic = deterministic
        self._ignore_unknown_fields = ignore_unknown_fields

        self._schema_registry_serializer = None
        if schema_registry_client_config:
            client_config = schema_registry_client_config.as_dict(
                plaintext_secrets=True,
            )

            if schema_registry_serialization_config:
                serialization_config = schema_registry_serialization_config.as_dict()
            else:
                # The use.deprecated.format has been mandatory since Confluent Kafka version 1.8.2.
                # https://github.com/confluentinc/confluent-kafka-python/releases/tag/v1.8.2
                serialization_config = SchemaRegistrySerializationConfig().as_dict(
                    include={"use_deprecated_format"},
                )

            self._schema_registry_serializer = _ProtobufSerializer(
                msg_type=msg_type,
                schema_registry_client=SchemaRegistryClient(client_config),
                conf=serialization_config,
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
            except TypeError as exc:
                raise SerializationError(
                    "Value to serialize must be of type "
                    f"`{self._msg_type}` or dict, not `{type(value)}`."
                ) from exc
            except ParseError as exc:
                raise SerializationError(str(exc)) from exc

        if self._schema_registry_serializer is not None:
            try:
                return self._schema_registry_serializer(msg, ctx)
            except (SchemaRegistryError, _SerializationError) as exc:
                raise SerializationError(str(exc)) from exc

        try:
            return msg.SerializeToString(deterministic=self._deterministic)
        except EncodeError as exc:
            raise SerializationError(str(exc)) from exc


class ProtobufDeserializer(Deserializer):
    def __init__(
        self,
        msg_type: Type[Message],
        use_integers_for_enums: bool = False,
        preserving_proto_field_name: bool = False,
        to_dict: bool = True,
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
        schema_registry_serialization_config: Optional[
            SchemaRegistrySerializationConfig
        ] = None,
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
        :param schema_registry_client_config: If provided, deserialization is offloaded to Confluent's ProtobufDeserializer.
            Default - `None`
        :param schema_registry_serialization_config: Additional configuration for Confluent's ProtobufDeserializer.
            Default - `None`
            >***NOTE:*** `schema_registry_client_config` must also be set.
        """
        super().__init__()
        self._msg_type = msg_type
        self._to_dict = to_dict

        self._use_integers_for_enums = use_integers_for_enums
        self._preserving_proto_field_name = preserving_proto_field_name

        # Confluent's ProtobufDeserializer is not utilizing the
        # Schema Registry. However, we still accept a fully qualified
        # SchemaRegistryClientConfig to maintain a unified API and ensure
        # future compatibility in case we choose to bypass Confluent
        # and interact with the Schema Registry directly.
        # On the other hand, ProtobufDeserializer requires
        # conf dict with a single key: `use.deprecated.format`.
        self._schema_registry_deserializer = None
        if schema_registry_client_config:
            # The use.deprecated.format has been mandatory since Confluent Kafka version 1.8.2.
            # https://github.com/confluentinc/confluent-kafka-python/releases/tag/v1.8.2
            serialization_config = (
                schema_registry_serialization_config
                or SchemaRegistrySerializationConfig()
            ).as_dict(include={"use_deprecated_format"})

            self._schema_registry_deserializer = _ProtobufDeserializer(
                message_type=msg_type,
                conf=serialization_config,
            )

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping, Message]:
        if self._schema_registry_deserializer is not None:
            try:
                msg = self._schema_registry_deserializer(value, ctx)
            except (_SerializationError, DecodeError) as exc:
                raise SerializationError(str(exc)) from exc
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
