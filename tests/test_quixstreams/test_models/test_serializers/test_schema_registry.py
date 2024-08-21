import json
from functools import partial
from io import BytesIO
from struct import pack, unpack
from typing import Any, Generator, Tuple, Union

import pytest
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    Schema,
    topic_subject_name_strategy,
)
from quixstreams.models import (
    Deserializer,
    Serializer,
    SerializationError,
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)

from quixstreams.models import JSONDeserializer, JSONSerializer
from quixstreams.models.serializers.avro import AvroDeserializer, AvroSerializer

from tests.conftest import SchemaRegistryContainer
from .constants import AVRO_TEST_SCHEMA, DUMMY_CONTEXT, JSONSCHEMA_TEST_SCHEMA

CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_MAGIC_SIZE = 5

SUBJECT = topic_subject_name_strategy(DUMMY_CONTEXT, None)


def _get_magic_byte_metadata(payload: bytes) -> Tuple[int, int]:
    return unpack(">bI", BytesIO(payload).read(CONFLUENT_MAGIC_SIZE))


def _set_magic_byte_metadata(payload: bytes, schema_id: int) -> bytes:
    return pack(">bI", CONFLUENT_MAGIC_BYTE, schema_id) + payload


@pytest.fixture()
def schema_registry_client_config(
    schema_registry_container: SchemaRegistryContainer,
) -> SchemaRegistryClientConfig:
    return SchemaRegistryClientConfig(
        url=schema_registry_container.schema_registry_address
    )


@pytest.fixture()
def schema_registry_client(
    schema_registry_client_config: SchemaRegistryClientConfig,
) -> SchemaRegistryClient:
    return SchemaRegistryClient(
        schema_registry_client_config.as_dict(plaintext_secrets=True)
    )


@pytest.fixture(autouse=True)
def _clear_schema_registry(
    schema_registry_client: SchemaRegistryClient,
) -> Generator[None, None, None]:
    # This will delete all schemas from the Schema Registry.
    # However, note that it will not reset the schema ID counter.
    # To restart schema IDs from 1, a container restart is required,
    # which would significantly increase testing times.
    yield
    while subjects := schema_registry_client.get_subjects():
        for subject in subjects:
            try:
                schema_registry_client.delete_subject(subject, permanent=True)
            except Exception as exc:
                if exc.error_code == 42206:
                    # One or more references exist to the schema. Skip for now.
                    continue
                raise


@pytest.fixture()
def _inject_schema_registry(
    request: pytest.FixtureRequest,
    schema_registry_client_config: SchemaRegistryClientConfig,
) -> Union[Deserializer, Serializer]:
    return request.param(schema_registry_client_config=schema_registry_client_config)


# This trick helps point multiple indirect attributes to a single fixture.
deserializer = serializer = _inject_schema_registry


@pytest.mark.parametrize(
    "serializer, deserializer, obj_to_serialize, serialized_data, deserialized_obj",
    [
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer),
            {"name": "foo", "id": 123},
            b"\x06foo\xf6\x01",
            {"name": "foo", "id": 123},
        ),
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer),
            {"name": "foo", "id": 0},
            b"\x06foo\x00",
            {"name": "foo", "id": 0},
        ),
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer, schema=AVRO_TEST_SCHEMA),
            {"name": "foo", "id": 123},
            b"\x06foo\xf6\x01",
            {"name": "foo", "id": 123},
        ),
        (
            partial(JSONSerializer, schema=JSONSCHEMA_TEST_SCHEMA),
            partial(JSONDeserializer, schema=JSONSCHEMA_TEST_SCHEMA),
            {"id": 10, "name": "foo"},
            b'{"id": 10, "name": "foo"}',
            {"id": 10, "name": "foo"},
        ),
    ],
    indirect=["serializer", "deserializer"],
)
def test_schema_registry_success(
    serializer: Serializer,
    deserializer: Deserializer,
    obj_to_serialize: Any,
    serialized_data: bytes,
    deserialized_obj: Any,
):
    serialized = serializer(obj_to_serialize, DUMMY_CONTEXT)

    magic, schema_id = _get_magic_byte_metadata(serialized)
    assert magic == CONFLUENT_MAGIC_BYTE
    assert isinstance(schema_id, int)
    assert serialized[CONFLUENT_MAGIC_SIZE:] == serialized_data

    assert deserializer(serialized, DUMMY_CONTEXT) == deserialized_obj


@pytest.mark.parametrize(
    "serializer, value",
    [
        (partial(AvroSerializer, AVRO_TEST_SCHEMA), {"foo": "foo", "id": 123}),
        (partial(AvroSerializer, AVRO_TEST_SCHEMA), {"id": 123}),
        (partial(JSONSerializer, schema=JSONSCHEMA_TEST_SCHEMA), {"id": 10}),
    ],
    indirect=["serializer"],
)
def test_schema_registry_serialize_error(serializer: Serializer, value: Any):
    with pytest.raises(SerializationError):
        serializer(value, DUMMY_CONTEXT)


@pytest.mark.parametrize(
    "deserializer, schema, data, exception_text",
    [
        (
            partial(AvroDeserializer),
            None,
            b"\x26foo\x00",
            r"Expecting data framing of length 6 bytes or more but total data size is \d+ bytes. "
            r"This message was not produced with a Confluent Schema Registry serializer",
        ),
        (
            partial(AvroDeserializer),
            Schema(schema_str=json.dumps(AVRO_TEST_SCHEMA), schema_type="AVRO"),
            b"\x26foo\x00",
            r"Expected \d+ bytes, read \d+",
        ),
        (
            partial(JSONDeserializer, schema=JSONSCHEMA_TEST_SCHEMA),
            None,
            b'{"id":10}',
            r"Unexpected magic byte \d+. This message was not produced with a Confluent Schema Registry serializer",
        ),
        (
            partial(JSONDeserializer, schema=JSONSCHEMA_TEST_SCHEMA),
            Schema(schema_str=json.dumps(JSONSCHEMA_TEST_SCHEMA), schema_type="JSON"),
            b'{"id":10}',
            "'name' is a required property",
        ),
    ],
    indirect=["deserializer"],
)
def test_schema_registry_deserialize_error(
    schema_registry_client: SchemaRegistryClient,
    deserializer: Serializer,
    schema: Schema,
    data: bytes,
    exception_text: str,
):
    if schema:
        schema_id = schema_registry_client.register_schema(
            subject_name=SUBJECT, schema=schema
        )
        data = _set_magic_byte_metadata(data, schema_id)

    with pytest.raises(SerializationError, match=exception_text):
        deserializer(data, DUMMY_CONTEXT)


def test_do_not_auto_register_schemas(
    schema_registry_client_config: SchemaRegistryClientConfig,
    schema_registry_client: SchemaRegistryClient,
):
    serializer = AvroSerializer(
        AVRO_TEST_SCHEMA,
        schema_registry_client_config=schema_registry_client_config,
        schema_registry_serialization_config=SchemaRegistrySerializationConfig(
            auto_register_schemas=False,
        ),
    )

    # First attempt fails because the schema is not registered
    with pytest.raises(SerializationError, match="Subject '.+' not found"):
        serializer({"name": "foo", "id": 123}, DUMMY_CONTEXT)

    schema_registry_client.register_schema(
        subject_name=SUBJECT,
        schema=Schema(schema_str=json.dumps(AVRO_TEST_SCHEMA), schema_type="AVRO"),
    )

    # Second attempt succeeds because the schema is registered
    serializer({"name": "foo", "id": 123}, DUMMY_CONTEXT)


def test_custom_subject_name_strategy(
    schema_registry_client_config: SchemaRegistryClientConfig,
    schema_registry_client: SchemaRegistryClient,
):
    serializer = AvroSerializer(
        AVRO_TEST_SCHEMA,
        schema_registry_client_config=schema_registry_client_config,
        schema_registry_serialization_config=SchemaRegistrySerializationConfig(
            subject_name_strategy=lambda *args: "custom-name"
        ),
    )

    serializer({"name": "foo", "id": 123}, DUMMY_CONTEXT)

    result = schema_registry_client.get_latest_version(subject_name="custom-name")
    assert json.loads(result.schema.schema_str) == AVRO_TEST_SCHEMA
