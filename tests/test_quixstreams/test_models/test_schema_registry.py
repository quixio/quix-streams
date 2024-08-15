from functools import partial
from io import BytesIO
from struct import unpack
from typing import Any, Generator, Union

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient

from quixstreams.schema_registry import SchemaRegistryConfig
from quixstreams.models import (
    Deserializer,
    Serializer,
    JSONDeserializer,
    JSONSerializer,
)
from quixstreams.models.serializers.avro import AvroDeserializer, AvroSerializer
from quixstreams.models.serializers.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
)

from .constants import AVRO_TEST_SCHEMA, DUMMY_CONTEXT, JSONSCHEMA_TEST_SCHEMA
from .protobuf.nested_pb2 import Nested
from .protobuf.root_pb2 import Root
from .protobuf.utils import create_timestamp


CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_MAGIC_SIZE = 5


def _get_magic_byte_metadata(payload: bytes, size: int):
    return unpack(">bI", BytesIO(payload).read(size))


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
    request: pytest.FixtureRequest, schema_registry_config: SchemaRegistryConfig
) -> Union[Deserializer, Serializer]:
    return request.param(schema_registry_config=schema_registry_config)


# This trick helps point multiple indirect attributes to a single fixture.
deserializer = serializer = _inject_schema_registry


@pytest.mark.parametrize(
    "serializer, deserializer, obj_to_serialize, serialized_data, deserialized_obj",
    [
        (
            partial(JSONSerializer, schema=JSONSCHEMA_TEST_SCHEMA),
            partial(JSONDeserializer, schema=JSONSCHEMA_TEST_SCHEMA),
            {"id": 10, "name": "foo"},
            b'{"id": 10, "name": "foo"}',
            {"id": 10, "name": "foo"},
        ),
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
def test_schema_registry(
    serializer: Serializer,
    deserializer: Deserializer,
    obj_to_serialize: Any,
    serialized_data: bytes,
    deserialized_obj: Any,
):
    serialized = serializer(obj_to_serialize, DUMMY_CONTEXT)

    magic, schema_id = _get_magic_byte_metadata(serialized, CONFLUENT_MAGIC_SIZE)
    assert magic == CONFLUENT_MAGIC_BYTE
    assert isinstance(schema_id, int)
    assert serialized[CONFLUENT_MAGIC_SIZE:] == serialized_data

    assert deserializer(serialized, DUMMY_CONTEXT) == deserialized_obj
