import json
import re
from functools import partial
from io import BytesIO
from struct import pack, unpack
from typing import Any, Set, Tuple

import pytest
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    topic_subject_name_strategy,
)

from quixstreams.models import (
    Deserializer,
    JSONDeserializer,
    JSONSerializer,
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
    SerializationError,
    Serializer,
)
from quixstreams.models.serializers.avro import AvroDeserializer, AvroSerializer
from quixstreams.models.serializers.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
)

from .constants import AVRO_TEST_SCHEMA, DUMMY_CONTEXT, JSONSCHEMA_TEST_SCHEMA
from .protobuf.nested_pb2 import Nested
from .protobuf.root_pb2 import Root
from .protobuf.utils import create_timestamp, get_schema_str

CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_MAGIC_SIZE = 5

SUBJECT = topic_subject_name_strategy(DUMMY_CONTEXT, None)


def _get_magic_byte_metadata(payload: bytes) -> Tuple[int, int]:
    return unpack(">bI", BytesIO(payload).read(CONFLUENT_MAGIC_SIZE))


def _set_magic_byte_metadata(payload: bytes, schema_id: int) -> bytes:
    return pack(">bI", CONFLUENT_MAGIC_BYTE, schema_id) + payload


@pytest.mark.parametrize(
    "serializer, deserializer, obj_to_serialize, serialized_data, deserialized_obj",
    [
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer),
            {"name": "foo", "id": 123},
            b"\x06foo\xf6\x01\x00",
            {"name": "foo", "id": 123, "nested": {"id": 0}},
        ),
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer),
            {"name": "foo", "id": 0},
            b"\x06foo\x00\x00",
            {"name": "foo", "id": 0, "nested": {"id": 0}},
        ),
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer),
            {"name": "foo", "id": 123, "nested": {"id": 123}},
            b"\x06foo\xf6\x01\xf6\x01",
            {"name": "foo", "id": 123, "nested": {"id": 123}},
        ),
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            partial(AvroDeserializer, schema=AVRO_TEST_SCHEMA),
            {"name": "foo", "id": 123, "nested": {"id": 123}},
            b"\x06foo\xf6\x01\xf6\x01",
            {"name": "foo", "id": 123, "nested": {"id": 123}},
        ),
        (
            partial(JSONSerializer, schema=JSONSCHEMA_TEST_SCHEMA),
            partial(JSONDeserializer, schema=JSONSCHEMA_TEST_SCHEMA),
            {"id": 10, "name": "foo"},
            b'{"id": 10, "name": "foo"}',
            {"id": 10, "name": "foo"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {},
            b"\x00",  # Confluent adds this extra byte in _encode_varints step
            {"name": "", "id": 0, "enum": "A"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {"id": 3},
            b"\x00\x10\x03",
            {"name": "", "id": 3, "enum": "A"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {"name": "foo"},
            b"\x00\n\x03foo",
            {"name": "foo", "id": 0, "enum": "A"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {"name": "foo", "id": 2},
            b"\x00\n\x03foo\x10\x02",
            {"name": "foo", "id": 2, "enum": "A"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            Root(name="foo", id=2),
            b"\x00\n\x03foo\x10\x02",
            {"name": "foo", "id": 2, "enum": "A"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {"name": "foo", "id": 2, "enum": "B"},
            b"\x00\n\x03foo\x10\x02\x18\x01",
            {"name": "foo", "id": 2, "enum": "B"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {"name": "foo", "id": 2, "enum": 1},
            b"\x00\n\x03foo\x10\x02\x18\x01",
            {"name": "foo", "id": 2, "enum": "B"},
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            {
                "name": "foo",
                "nested": {
                    "id": 10,
                    "time": "2000-01-02T12:34:56Z",
                },
            },
            b'\x00\n\x03foo"\n\x08\n\x12\x06\x08\xf0\x8b\xbd\xc3\x03',
            {
                "name": "foo",
                "id": 0,
                "enum": "A",
                "nested": {
                    "id": 10,
                    "time": "2000-01-02T12:34:56Z",
                },
            },
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root),
            Root(
                name="foo",
                nested=Nested(
                    id=10,
                    time=create_timestamp("2000-01-02T12:34:56Z"),
                ),
            ),
            b'\x00\n\x03foo"\n\x08\n\x12\x06\x08\xf0\x8b\xbd\xc3\x03',
            {
                "name": "foo",
                "id": 0,
                "enum": "A",
                "nested": {
                    "id": 10,
                    "time": "2000-01-02T12:34:56Z",
                },
            },
        ),
        (
            partial(ProtobufSerializer, Root),
            partial(ProtobufDeserializer, Root, to_dict=False),
            Root(
                name="foo",
                nested=Nested(
                    id=10,
                    time=create_timestamp("2000-01-02T12:34:56Z"),
                ),
            ),
            b'\x00\n\x03foo"\n\x08\n\x12\x06\x08\xf0\x8b\xbd\xc3\x03',
            Root(
                name="foo",
                nested=Nested(
                    id=10,
                    time=create_timestamp("2000-01-02T12:34:56Z"),
                ),
            ),
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
    "serializer, value, match",
    [
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            {"foo": "foo", "id": 123},
            "no value and no default for name",
        ),
        (
            partial(AvroSerializer, AVRO_TEST_SCHEMA),
            {"id": 123},
            "no value and no default for name",
        ),
        (
            partial(JSONSerializer, schema=JSONSCHEMA_TEST_SCHEMA),
            {"id": 10},
            "'name' is a required property",
        ),
        (
            partial(
                ProtobufSerializer,
                Root,
                schema_registry_serialization_config=SchemaRegistrySerializationConfig(
                    auto_register_schemas=False,
                ),
            ),
            {},
            re.escape(
                "Subject 'google/protobuf/timestamp.proto' not found. (HTTP status code 404, SR code 40401)"
            ),
        ),
    ],
    indirect=["serializer"],
)
def test_schema_registry_serialize_error(
    serializer: Serializer, value: Any, match: str
):
    with pytest.raises(SerializationError, match=match):
        serializer(value, DUMMY_CONTEXT)


@pytest.mark.parametrize(
    "deserializer, schema, data, match",
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
        (
            partial(ProtobufDeserializer, Root),
            None,
            b"\n\x03foo\x10\x02\x13",
            "Unknown magic byte. This message was not produced with a Confluent Schema Registry serializer",
        ),
        (
            partial(ProtobufDeserializer, Nested),
            Schema(schema_str=get_schema_str(Nested), schema_type="PROTOBUF"),
            b"\n\x03foo\x10\x02\x13",
            "Error parsing message",
        ),
        (
            partial(ProtobufDeserializer, Nested),
            Schema(schema_str=get_schema_str(Nested), schema_type="PROTOBUF"),
            b"\x01",
            "Invalid Protobuf msgidx array length",
        ),
    ],
    indirect=["deserializer"],
)
def test_schema_registry_deserialize_error(
    schema_registry_client: SchemaRegistryClient,
    deserializer: Serializer,
    schema: Schema,
    data: bytes,
    match: str,
):
    if schema:
        schema_id = schema_registry_client.register_schema(
            subject_name=SUBJECT, schema=schema
        )
        data = _set_magic_byte_metadata(data, schema_id)

    with pytest.raises(SerializationError, match=match):
        deserializer(data, DUMMY_CONTEXT)


@pytest.mark.parametrize(
    "serializer, schema, obj_to_serialize",
    [
        (
            partial(
                AvroSerializer,
                AVRO_TEST_SCHEMA,
                schema_registry_serialization_config=SchemaRegistrySerializationConfig(
                    auto_register_schemas=False,
                ),
            ),
            Schema(schema_str=json.dumps(AVRO_TEST_SCHEMA), schema_type="AVRO"),
            {"name": "foo", "id": 123},
        ),
        (
            partial(
                JSONSerializer,
                schema=JSONSCHEMA_TEST_SCHEMA,
                schema_registry_serialization_config=SchemaRegistrySerializationConfig(
                    auto_register_schemas=False,
                ),
            ),
            Schema(schema_str=json.dumps(JSONSCHEMA_TEST_SCHEMA), schema_type="JSON"),
            {"name": "foo", "id": 123},
        ),
        (
            partial(
                ProtobufSerializer,
                Nested,
                schema_registry_serialization_config=SchemaRegistrySerializationConfig(
                    auto_register_schemas=False,
                    skip_known_types=True,
                ),
            ),
            Schema(schema_str=get_schema_str(Nested), schema_type="PROTOBUF"),
            {"id": 123},
        ),
    ],
    indirect=["serializer"],
)
def test_do_not_auto_register_schemas(
    schema_registry_client: SchemaRegistryClient,
    serializer: Serializer,
    schema: Schema,
    obj_to_serialize: Any,
):
    # First attempt fails because the schema is not registered
    with pytest.raises(SerializationError, match="Subject '.+' not found"):
        serializer(obj_to_serialize, DUMMY_CONTEXT)

    schema_registry_client.register_schema(subject_name=SUBJECT, schema=schema)

    # Second attempt succeeds because the schema is registered
    serializer(obj_to_serialize, DUMMY_CONTEXT)


@pytest.mark.parametrize(
    "skip_known_types, subjects",
    [
        (False, {"google/protobuf/timestamp.proto", "nested.proto", "topic-value"}),
        (True, {"nested.proto", "topic-value"}),
    ],
)
def test_skip_known_types(
    schema_registry_client_config: SchemaRegistryClientConfig,
    schema_registry_client: SchemaRegistryClient,
    skip_known_types: bool,
    subjects: Set,
):
    serializer = ProtobufSerializer(
        Root,
        schema_registry_client_config=schema_registry_client_config,
        schema_registry_serialization_config=SchemaRegistrySerializationConfig(
            skip_known_types=skip_known_types,
        ),
    )

    serializer({}, DUMMY_CONTEXT)

    assert set(schema_registry_client.get_subjects()) == subjects


def test_custom_subject_name_strategies(
    schema_registry_client_config: SchemaRegistryClientConfig,
    schema_registry_client: SchemaRegistryClient,
):
    serializer = ProtobufSerializer(
        Root,
        schema_registry_client_config=schema_registry_client_config,
        schema_registry_serialization_config=SchemaRegistrySerializationConfig(
            skip_known_types=True,
            subject_name_strategy=lambda *args: "foo",
            reference_subject_name_strategy=lambda *args: "bar",
        ),
    )

    serializer({}, DUMMY_CONTEXT)

    assert set(schema_registry_client.get_subjects()) == {"foo", "bar"}
    root = schema_registry_client.get_latest_version(subject_name="foo")
    assert "message Root" in root.schema.schema_str
    nested = schema_registry_client.get_latest_version(subject_name="bar")
    assert "message Nested" in nested.schema.schema_str
