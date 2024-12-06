import json
from io import BytesIO
from typing import Any, Iterable, Mapping, Optional, Union

from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.avro import (
    AvroDeserializer as _AvroDeserializer,
)
from confluent_kafka.schema_registry.avro import (
    AvroSerializer as _AvroSerializer,
)
from confluent_kafka.serialization import SerializationError as _SerializationError
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.types import Schema

from .base import Deserializer, SerializationContext, Serializer
from .exceptions import SerializationError
from .schema_registry import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)

__all__ = ("AvroSerializer", "AvroDeserializer")


class AvroSerializer(Serializer):
    def __init__(
        self,
        schema: Schema,
        strict: bool = False,
        strict_allow_default: bool = False,
        disable_tuple_notation: bool = False,
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
        schema_registry_serialization_config: Optional[
            SchemaRegistrySerializationConfig
        ] = None,
    ):
        """
        Serializer that returns data in Avro format.

        For more information see fastavro [schemaless_writer](https://fastavro.readthedocs.io/en/latest/writer.html#fastavro._write_py.schemaless_writer) method.

        :param schema: The avro schema.
        :param strict: If set to True, an error will be raised if records do not contain exactly the same fields that the schema states.
            Default - `False`
        :param strict_allow_default: If set to True, an error will be raised if records do not contain exactly the same fields that the schema states unless it is a missing field that has a default value in the schema.
            Default - `False`
        :param disable_tuple_notation: If set to True, tuples will not be treated as a special case. Therefore, using a tuple to indicate the type of a record will not work.
            Default - `False`
        :param schema_registry_client_config: If provided, serialization is offloaded to Confluent's AvroSerializer.
            Default - `None`
        :param schema_registry_serialization_config: Additional configuration for Confluent's AvroSerializer.
            Default - `None`
            >***NOTE:*** `schema_registry_client_config` must also be set.
        """
        if schema_registry_serialization_config and not schema_registry_client_config:
            raise ValueError(
                "If `schema_registry_serialization_config` is provided "
                "`schema_registry_client_config` must also be set."
            )

        self._schema = parse_schema(schema)
        self._strict = strict
        self._strict_allow_default = strict_allow_default
        self._disable_tuple_notation = disable_tuple_notation
        self._schema_registry_serializer = None
        if schema_registry_client_config:
            client_config = schema_registry_client_config.as_dict(
                plaintext_secrets=True,
            )

            serialization_config = {}
            if schema_registry_serialization_config:
                serialization_config = schema_registry_serialization_config.as_dict(
                    include={
                        "auto_register_schemas",
                        "normalize_schemas",
                        "use_latest_version",
                        "subject_name_strategy",
                    },
                )

            self._schema_registry_serializer = _AvroSerializer(
                schema_registry_client=SchemaRegistryClient(client_config),
                schema_str=json.dumps(schema),
                conf=serialization_config,
            )

    def __call__(self, value: Any, ctx: SerializationContext) -> bytes:
        if self._schema_registry_serializer is not None:
            try:
                return self._schema_registry_serializer(value, ctx)
            except (SchemaRegistryError, _SerializationError, ValueError) as exc:
                raise SerializationError(str(exc)) from exc

        with BytesIO() as data:
            try:
                schemaless_writer(
                    data,
                    self._schema,
                    value,
                    strict=self._strict,
                    strict_allow_default=self._strict_allow_default,
                    disable_tuple_notation=self._disable_tuple_notation,
                )
            except (ValueError, TypeError) as exc:
                raise SerializationError(str(exc)) from exc

            return data.getvalue()


class AvroDeserializer(Deserializer):
    def __init__(
        self,
        schema: Optional[Schema] = None,
        reader_schema: Optional[Schema] = None,
        return_record_name: bool = False,
        return_record_name_override: bool = False,
        return_named_type: bool = False,
        return_named_type_override: bool = False,
        handle_unicode_errors: str = "strict",
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    ):
        """
        Deserializer that parses data from Avro.

        For more information see fastavro [schemaless_reader](https://fastavro.readthedocs.io/en/latest/reader.html#fastavro._read_py.schemaless_reader) method.

        :param schema: The Avro schema.
        :param reader_schema: If the schema has changed since being written then the new schema can be given to allow for schema migration.
            Default - `None`
        :param return_record_name: If true, when reading a union of records, the result will be a tuple where the first value is the name of the record and the second value is the record itself.
            Default - `False`
        :param return_record_name_override: If true, this will modify the behavior of return_record_name so that the record name is only returned for unions where there is more than one record. For unions that only have one record, this option will make it so that the record is returned by itself, not a tuple with the name.
            Default - `False`
        :param return_named_type: If true, when reading a union of named types, the result will be a tuple where the first value is the name of the type and the second value is the record itself NOTE: Using this option will ignore return_record_name and return_record_name_override.
            Default - `False`
        :param return_named_type_override: If true, this will modify the behavior of return_named_type so that the named type is only returned for unions where there is more than one named type. For unions that only have one named type, this option will make it so that the named type is returned by itself, not a tuple with the name.
            Default - `False`
        :param handle_unicode_errors: Should be set to a valid string that can be used in the errors argument of the string decode() function.
            Default - `"strict"`
        :param schema_registry_client_config: If provided, deserialization is offloaded to Confluent's AvroDeserializer.
            Default - `None`
        """
        if not schema and not schema_registry_client_config:
            raise ValueError(
                "Either `schema` or `schema_registry_client_config` must be provided."
            )

        super().__init__()

        if schema is None and schema_registry_client_config is None:
            raise TypeError(
                "One of `schema` or `schema_registry_client_config` is required"
            )

        self._schema = parse_schema(schema) if schema else None
        self._reader_schema = parse_schema(reader_schema) if reader_schema else None
        self._return_record_name = return_record_name
        self._return_record_name_override = return_record_name_override
        self._return_named_type = return_named_type
        self._return_named_type_override = return_named_type_override
        self._handle_unicode_errors = handle_unicode_errors
        self._schema_registry_deserializer = None
        if schema_registry_client_config:
            client_config = schema_registry_client_config.as_dict(
                plaintext_secrets=True,
            )
            self._schema_registry_deserializer = _AvroDeserializer(
                schema_registry_client=SchemaRegistryClient(client_config),
                schema_str=json.dumps(schema) if schema else None,
                return_record_name=return_record_name,
            )

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping]:
        if self._schema_registry_deserializer is not None:
            try:
                return self._schema_registry_deserializer(value, ctx)
            except (SchemaRegistryError, _SerializationError, EOFError) as exc:
                raise SerializationError(str(exc)) from exc
        elif self._schema is not None:
            try:
                return schemaless_reader(  # type: ignore
                    BytesIO(value),
                    self._schema,
                    reader_schema=self._reader_schema,
                    return_record_name=self._return_record_name,
                    return_record_name_override=self._return_record_name_override,
                    return_named_type=self._return_named_type,
                    return_named_type_override=self._return_named_type_override,
                    handle_unicode_errors=self._handle_unicode_errors,
                )
            except EOFError as exc:
                raise SerializationError(str(exc)) from exc

        raise SerializationError("no schema found")
