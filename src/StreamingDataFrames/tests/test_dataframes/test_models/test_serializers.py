import pytest

from streamingdataframes.models import (
    IntegerSerializer,
    SerializationContext,
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
from .utils import int_to_bytes, float_to_bytes

dummy_context = SerializationContext(topic="topic")


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
            (JSONSerializer(), {"a": 123}, '{"a":123}'),
            (JSONSerializer(), [1, 2, 3], "[1,2,3]"),
        ],
    )
    def test_serialize_success(self, serializer: Serializer, value, expected):
        assert serializer(value, ctx=dummy_context) == expected

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
        ],
    )
    def test_serialize_error(self, serializer: Serializer, value):
        with pytest.raises(SerializationError):
            serializer(value, ctx=dummy_context)


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
        ],
    )
    def test_deserialize_no_column_name_success(
        self, deserializer: Deserializer, value, expected
    ):
        assert deserializer(value, ctx=dummy_context) == expected

    @pytest.mark.parametrize(
        "deserializer, value, expected",
        [
            (
                IntegerDeserializer("value"),
                int_to_bytes(123),
                {"value": 123},
            ),
            (DoubleDeserializer("value"), float_to_bytes(123), {"value": 123.0}),
            (DoubleDeserializer("value"), float_to_bytes(123.123), {"value": 123.123}),
            (StringDeserializer("value"), b"abc", {"value": "abc"}),
            (
                StringDeserializer("value", codec="cp1251"),
                "abc".encode("cp1251"),
                {"value": "abc"},
            ),
            (BytesDeserializer("value"), b"123123", {"value": b"123123"}),
            (JSONDeserializer("value"), b"123123", {"value": 123123}),
            (JSONDeserializer("value"), b'{"a":"b"}', {"value": {"a": "b"}}),
        ],
    )
    def test_deserialize_with_column_name_success(
        self, deserializer: Deserializer, value, expected
    ):
        assert deserializer(value, ctx=dummy_context) == expected

    @pytest.mark.parametrize(
        "deserializer, value",
        [
            (IntegerDeserializer(), b"abc"),
            (IntegerDeserializer(), b'{"abc": "abc"}'),
            (DoubleDeserializer(), b"abc"),
            (JSONDeserializer(), b"{"),
        ],
    )
    def test_deserialize_error(self, deserializer: Deserializer, value):
        with pytest.raises(SerializationError):
            deserializer(value, ctx=dummy_context)
