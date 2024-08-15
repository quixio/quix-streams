import json
from typing import Callable, Union, Mapping, Optional, Any, Iterable

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import (
    JSONDeserializer as _JSONDeserializer,
    JSONSerializer as _JSONSerializer,
)
from jsonschema import ValidationError, Draft202012Validator
from jsonschema.protocols import Validator

from quixstreams.schema_registry import SchemaRegistryConfig
from quixstreams.utils.json import (
    dumps as default_dumps,
    loads as default_loads,
)
from .base import Serializer, Deserializer, SerializationContext
from .exceptions import SerializationError

__all__ = ("JSONSerializer", "JSONDeserializer")


class JSONSerializer(Serializer):
    def __init__(
        self,
        dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
        schema: Optional[Mapping] = None,
        validator: Optional[Validator] = None,
        schema_registry_config: Optional[SchemaRegistryConfig] = None,
    ):
        """
        Serializer that returns data in json format.
        :param dumps: a function to serialize objects to json.
            Default - :py:func:`quixstreams.utils.json.dumps`
        :param schema: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/#jsonschema.validators.Draft202012Validator).
            Default - `None`
        :param validator: A jsonschema validator used to validate the data. Takes precedences over the schema.
            Default - `None`
        """

        if schema and not validator:
            validator = Draft202012Validator(schema)

        super().__init__()
        self._dumps = dumps
        self._validator = validator

        if self._validator:
            self._validator.check_schema(self._validator.schema)

        self._schema_registry_serializer = None
        if schema_registry_config:
            if not schema:
                raise ValueError(
                    "`schema` is required if `schema_registry_config` is provided."
                )
            conf = schema_registry_config.as_dict(plaintext_secrets=True)
            self._schema_registry_serializer = _JSONSerializer(
                schema_str=json.dumps(schema),
                schema_registry_client=SchemaRegistryClient(conf),
            )

    def __call__(self, value: Any, ctx: SerializationContext) -> Union[str, bytes]:
        if self._schema_registry_serializer:
            return self._schema_registry_serializer(value, ctx.to_confluent_ctx())
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
        validator: Optional[Validator] = None,
        schema_registry_config: Optional[SchemaRegistryConfig] = None,
    ):
        """
        Deserializer that parses data from JSON

        :param loads: function to parse json from bytes.
            Default - :py:func:`quixstreams.utils.json.loads`.
        :param schema: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/#jsonschema.validators.Draft202012Validator).
            Default - `None`
        :param validator: A jsonschema validator used to validate the data. Takes precedences over the schema.
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
        if schema_registry_config:
            if not schema:
                raise ValueError(
                    "`schema` is required if `schema_registry_config` is provided."
                )
            conf = schema_registry_config.as_dict(plaintext_secrets=True)
            self._schema_registry_deserializer = _JSONDeserializer(
                schema_str=json.dumps(schema),
                schema_registry_client=SchemaRegistryClient(conf),
            )

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping]:
        if self._schema_registry_deserializer:
            return self._schema_registry_deserializer(value, ctx.to_confluent_ctx())

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
