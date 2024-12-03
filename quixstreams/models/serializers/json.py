import json
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Optional, Union

from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.json_schema import (
    JSONDeserializer as _JSONDeserializer,
)
from confluent_kafka.schema_registry.json_schema import (
    JSONSerializer as _JSONSerializer,
)
from confluent_kafka.serialization import SerializationError as _SerializationError
from jsonschema import Draft202012Validator, ValidationError

from quixstreams.utils.json import (
    dumps as default_dumps,
)
from quixstreams.utils.json import (
    loads as default_loads,
)

from .base import Deserializer, SerializationContext, Serializer
from .exceptions import SerializationError
from .schema_registry import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)

if TYPE_CHECKING:
    from jsonschema.validators import _Validator

__all__ = ("JSONSerializer", "JSONDeserializer")


class JSONSerializer(Serializer):
    def __init__(
        self,
        dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
        schema: Optional[Mapping] = None,
        validator: Optional["_Validator"] = None,
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
        schema_registry_serialization_config: Optional[
            SchemaRegistrySerializationConfig
        ] = None,
    ):
        """
        Serializer that returns data in json format.
        :param dumps: a function to serialize objects to json.
            Default - :py:func:`quixstreams.utils.json.dumps`
        :param schema: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/#jsonschema.validators.Draft202012Validator).
            Default - `None`
        :param validator: A jsonschema validator used to validate the data. Takes precedences over the schema.
            Default - `None`
        :param schema_registry_client_config: If provided, serialization is offloaded to Confluent's JSONSerializer.
            Default - `None`
        :param schema_registry_serialization_config: Additional configuration for Confluent's JSONSerializer.
            Default - `None`
            >***NOTE:*** `schema_registry_client_config` must also be set.
        """

        if schema and not validator:
            validator = Draft202012Validator(schema)

        super().__init__()
        self._dumps = dumps
        self._validator = validator

        if self._validator:
            self._validator.check_schema(self._validator.schema)

        self._schema_registry_serializer = None
        if schema_registry_client_config:
            if not schema:
                raise ValueError(
                    "If `schema_registry_serialization_config` is provided, "
                    "`schema` must also be set."
                )

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

            self._schema_registry_serializer = _JSONSerializer(
                schema_str=json.dumps(schema),
                schema_registry_client=SchemaRegistryClient(client_config),
                conf=serialization_config,
            )

    def __call__(self, value: Any, ctx: SerializationContext) -> Union[str, bytes]:
        if self._schema_registry_serializer is not None:
            try:
                return self._schema_registry_serializer(value, ctx)
            except (SchemaRegistryError, _SerializationError) as exc:
                raise SerializationError(str(exc)) from exc

        return self._to_json(value)

    def _to_json(self, value: Any):
        if self._validator:
            try:
                self._validator.validate(value)
            except ValidationError as exc:
                raise SerializationError(str(exc)) from exc

        try:
            return self._dumps(value)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc


class JSONDeserializer(Deserializer):
    def __init__(
        self,
        loads: Callable[[Union[bytes, bytearray]], Any] = default_loads,
        schema: Optional[Mapping] = None,
        validator: Optional["_Validator"] = None,
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    ):
        """
        Deserializer that parses data from JSON

        :param loads: function to parse json from bytes.
            Default - :py:func:`quixstreams.utils.json.loads`.
        :param schema: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/#jsonschema.validators.Draft202012Validator).
            Default - `None`
        :param validator: A jsonschema validator used to validate the data. Takes precedences over the schema.
            Default - `None`
        :param schema_registry_client_config: If provided, deserialization is offloaded to Confluent's JSONDeserializer.
            Default - `None`
        """

        if schema and not validator:
            validator = Draft202012Validator(schema)

        super().__init__()
        self._loads = loads
        self._validator = validator

        if self._validator:
            self._validator.check_schema(self._validator.schema)

        self._schema_registry_deserializer = None
        if schema_registry_client_config:
            if not schema:
                raise ValueError(
                    "If `schema_registry_serialization_config` is provided, "
                    "`schema` must also be set."
                )

            client_config = schema_registry_client_config.as_dict(
                plaintext_secrets=True,
            )
            self._schema_registry_deserializer = _JSONDeserializer(
                schema_str=json.dumps(schema),
                schema_registry_client=SchemaRegistryClient(client_config),
            )

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping]:
        if self._schema_registry_deserializer is not None:
            try:
                return self._schema_registry_deserializer(value, ctx)
            except (SchemaRegistryError, _SerializationError) as exc:
                raise SerializationError(str(exc)) from exc

        try:
            data = self._loads(value)
        except (ValueError, TypeError) as exc:
            raise SerializationError(str(exc)) from exc

        if self._validator:
            try:
                self._validator.validate(data)
            except ValidationError as exc:
                raise SerializationError(str(exc)) from exc

        return data
