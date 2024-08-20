import pytest
import jsonschema

from quixstreams.models import (
    IntegerSerializer,
    DoubleSerializer,
    StringSerializer,
    BytesDeserializer,
    JSONDeserializer,
    JSONSerializer,
    BytesSerializer,
    Serializer,
    SerializationError,
    IntegerDeserializer,
    Deserializer,
    DoubleDeserializer,
    StringDeserializer,
)
from quixstreams.models.serializers.protobuf import (
    ProtobufSerializer,
    ProtobufDeserializer,
)

from quixstreams.models.serializers.avro import AvroDeserializer, AvroSerializer

from .constants import AVRO_TEST_SCHEMA, DUMMY_CONTEXT, JSONSCHEMA_TEST_SCHEMA
from .utils import int_to_bytes, float_to_bytes
from .protobuf.test_pb2 import Test


class TestSerializers:
    @pytest.mark.parametrize(
        "serializer, value, expected",
        [
            (IntegerSerializer(), 123, int_to_bytes(123)),
            (IntegerSerializer(), 123, int_to_bytes(123)),
            (DoubleSerializer(), 123, float_to_bytes(123)),
            (DoubleSerializer(), 123.123, float_to_bytes(123.123)),
            (StringSerializer(), "abc", b"abc"),
            (StringSerializer(codec="cp1251"), "abc", "abc".encode("cp1251")),
            (BytesSerializer(), b"abc", b"abc"),
            (JSONSerializer(), {"a": 123}, b'{"a":123}'),
            (JSONSerializer(), [1, 2, 3], b"[1,2,3]"),
            (
                JSONSerializer(schema=JSONSCHEMA_TEST_SCHEMA),
                {"id": 10, "name": "foo"},
                b'{"id":10,"name":"foo"}',
            ),
            (
                JSONSerializer(
                    validator=jsonschema.Draft202012Validator(JSONSCHEMA_TEST_SCHEMA)
                ),
                {"id": 10, "name": "foo"},
                b'{"id":10,"name":"foo"}',
            ),
            (
                AvroSerializer(AVRO_TEST_SCHEMA),
                {"name": "foo", "id": 123},
                b"\x06foo\xf6\x01",
            ),
            (AvroSerializer(AVRO_TEST_SCHEMA), {"name": "foo"}, b"\x06foo\x00"),
            (ProtobufSerializer(Test), {}, b""),
            (ProtobufSerializer(Test), {"id": 3}, b"\x10\x03"),
            (ProtobufSerializer(Test), {"name": "foo", "id": 2}, b"\n\x03foo\x10\x02"),
            (ProtobufSerializer(Test), Test(name="foo", id=2), b"\n\x03foo\x10\x02"),
            # Both values are supported for enum
            (
                ProtobufSerializer(Test),
                {"name": "foo", "id": 2, "enum": "B"},
                b"\n\x03foo\x10\x02\x18\x01",
            ),
            (
                ProtobufSerializer(Test),
                {"name": "foo", "id": 2, "enum": 1},
                b"\n\x03foo\x10\x02\x18\x01",
            ),
            (ProtobufSerializer(Test), {"name": "foo"}, b"\n\x03foo"),
        ],
    )
    def test_serialize_success(self, serializer: Serializer, value, expected):
        assert serializer(value, ctx=DUMMY_CONTEXT) == expected

    @pytest.mark.parametrize(
        "serializer, value",
        [
            (IntegerSerializer(), "abc"),
            (IntegerSerializer(), {"abc": "abc"}),
            (DoubleSerializer(), "abc"),
            (DoubleSerializer(), object()),
            (StringSerializer(), 123),
            (StringSerializer(), {"a": 123}),
            (JSONSerializer(), object()),
            (JSONSerializer(), complex(1, 2)),
            (
                JSONSerializer(schema=JSONSCHEMA_TEST_SCHEMA),
                {"id": 10},
            ),
            (
                JSONSerializer(
                    validator=jsonschema.Draft202012Validator(JSONSCHEMA_TEST_SCHEMA)
                ),
                {"id": 10},
            ),
            (AvroSerializer(AVRO_TEST_SCHEMA), {"foo": "foo", "id": 123}),
            (AvroSerializer(AVRO_TEST_SCHEMA), {"id": 123}),
            (AvroSerializer(AVRO_TEST_SCHEMA, strict=True), {"name": "foo"}),
            (ProtobufSerializer(Test), {"bar": 3}),
        ],
    )
    def test_serialize_error(self, serializer: Serializer, value):
        with pytest.raises(SerializationError):
            serializer(value, ctx=DUMMY_CONTEXT)

    def test_invalid_jsonschema(self):
        with pytest.raises(jsonschema.SchemaError):
            JSONSerializer(
                validator=jsonschema.Draft202012Validator({"type": "invalid"})
            )


class TestDeserializers:
    @pytest.mark.parametrize(
        "deserializer, value, expected",
        [
            (IntegerDeserializer(), int_to_bytes(123), 123),
            (DoubleDeserializer(), float_to_bytes(123), 123.0),
            (DoubleDeserializer(), float_to_bytes(123.123), 123.123),
            (StringDeserializer(), b"abc", "abc"),
            (StringDeserializer(codec="cp1251"), "abc".encode("cp1251"), "abc"),
            (StringDeserializer(codec="cp1251"), "abc".encode("cp1251"), "abc"),
            (BytesDeserializer(), b"123123", b"123123"),
            (JSONDeserializer(), b"123123", 123123),
            (JSONDeserializer(), b'{"a":"b"}', {"a": "b"}),
            (
                JSONDeserializer(schema=JSONSCHEMA_TEST_SCHEMA),
                b'{"id":10,"name":"foo"}',
                {"id": 10, "name": "foo"},
            ),
            (
                JSONDeserializer(
                    validator=jsonschema.Draft202012Validator(JSONSCHEMA_TEST_SCHEMA)
                ),
                b'{"id":10,"name":"foo"}',
                {"id": 10, "name": "foo"},
            ),
            (
                AvroDeserializer(AVRO_TEST_SCHEMA),
                b"\x06foo\xf6\x01",
                {"name": "foo", "id": 123},
            ),
            (
                AvroDeserializer(AVRO_TEST_SCHEMA),
                b"\x06foo\x00",
                {"name": "foo", "id": 0},
            ),
            (
                ProtobufDeserializer(Test),
                b"\n\x03foo\x10\x02",
                {"enum": "A", "name": "foo", "id": 2},
            ),
            (
                ProtobufDeserializer(Test, to_dict=False),
                b"\n\x03foo\x10\x02",
                Test(name="foo", id=2),
            ),
            (
                ProtobufDeserializer(Test, use_integers_for_enums=True),
                b"\n\x03foo\x10\x02",
                {"enum": 0, "name": "foo", "id": 2},
            ),
            (
                ProtobufDeserializer(Test),
                b"\n\x03foo",
                {
                    "enum": "A",
                    "name": "foo",
                    "id": 0,
                },
            ),
            (
                ProtobufDeserializer(Test),
                b"\x10\x03",
                {"enum": "A", "name": "", "id": 3},
            ),
            (ProtobufDeserializer(Test), b"", {"enum": "A", "name": "", "id": 0}),
        ],
    )
    def test_deserialize_no_column_name_success(
        self, deserializer: Deserializer, value, expected
    ):
        assert deserializer(value, ctx=DUMMY_CONTEXT) == expected

    @pytest.mark.parametrize(
        "deserializer, value",
        [
            (IntegerDeserializer(), b"abc"),
            (IntegerDeserializer(), b'{"abc": "abc"}'),
            (DoubleDeserializer(), b"abc"),
            (JSONDeserializer(), b"{"),
            (
                JSONDeserializer(schema=JSONSCHEMA_TEST_SCHEMA),
                b'{"id":10}',
            ),
            (
                JSONDeserializer(
                    validator=jsonschema.Draft202012Validator(JSONSCHEMA_TEST_SCHEMA)
                ),
                b'{"id":10}',
            ),
            (AvroDeserializer(AVRO_TEST_SCHEMA), b"\x26foo\x00"),
            (ProtobufDeserializer(Test), b"\n\x03foo\x10\x02\x13"),
        ],
    )
    def test_deserialize_error(self, deserializer: Deserializer, value):
        with pytest.raises(SerializationError):
            deserializer(value, ctx=DUMMY_CONTEXT)

    def test_invalid_jsonschema(self):
        with pytest.raises(jsonschema.SchemaError):
            JSONDeserializer(
                validator=jsonschema.Draft202012Validator({"type": "invalid"})
            )

        with pytest.raises(jsonschema.SchemaError):
            JSONDeserializer(schema={"type": "invalid"})
