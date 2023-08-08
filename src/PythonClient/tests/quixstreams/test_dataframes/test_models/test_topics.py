import json
from typing import Optional, Any, Mapping

import pytest

from src.quixstreams.dataframes.models import Topic, Row, MessageTimestamp
from src.quixstreams.dataframes.models.serializers import (
    Deserializer,
    Serializer,
    IntegerDeserializer,
    DoubleDeserializer,
    JSONDeserializer,
    BytesSerializer,
    JSONSerializer,
    IntegerSerializer,
    BytesDeserializer,
    SerializationContext,
    DeserializerIsNotProvidedError,
    IgnoreValueError,
    SerializationError,
)
from src.quixstreams.dataframes.models.types import MessageKey, MessageHeaders
from .utils import ConfluentKafkaMessageStub, int_to_bytes, float_to_bytes


class JSONListDeserializer(JSONDeserializer):
    def split_values(self) -> bool:
        return True


class IgnoreDivisibleBy3Deserializer(IntegerDeserializer):
    def __call__(self, value: bytes, ctx: SerializationContext):
        deserialized = self._deserializer(value=value)
        if not deserialized % 3:
            raise IgnoreValueError("Ignore numbers divisible by 3")
        if self.column_name:
            return {self.column_name: deserialized}
        return deserialized


def row_factory(
    value: Optional[Mapping],
    topic: str = "test",
    partition: int = 0,
    offset: int = 0,
    size: int = 123,
    timestamp: MessageTimestamp = MessageTimestamp.create(
        timestamp_type=1, milliseconds=123
    ),
    key: Optional[MessageKey] = "key",
    headers: Optional[Mapping | MessageHeaders] = None,
    latency: Optional[float] = None,
    leader_epoch: Optional[int] = None,
) -> Row:
    return Row(
        key=key,
        value=value,
        headers=headers,
        topic=topic,
        partition=partition,
        offset=offset,
        size=size,
        timestamp=timestamp,
        latency=latency,
        leader_epoch=leader_epoch,
    )


class TestTopic:
    @pytest.mark.parametrize(
        "key_deserializer, value_deserializer, key, value, expected_key, expected_value",
        [
            (
                IntegerDeserializer(),
                IntegerDeserializer("column"),
                int_to_bytes(1),
                int_to_bytes(2),
                1,
                {"column": 2},
            ),
            (
                DoubleDeserializer(),
                JSONDeserializer(),
                float_to_bytes(1.1),
                json.dumps({"key": "value"}).encode(),
                1.1,
                {"key": "value"},
            ),
            (
                DoubleDeserializer(),
                JSONDeserializer(),
                float_to_bytes(1.1),
                json.dumps({"key": "value"}).encode(),
                1.1,
                {"key": "value"},
            ),
            (
                DoubleDeserializer(),
                JSONDeserializer(column_name="root"),
                float_to_bytes(1.1),
                json.dumps({"key": "value"}).encode(),
                1.1,
                {"root": {"key": "value"}},
            ),
        ],
    )
    def test_row_deserialize_success(
        self,
        key_deserializer: Deserializer,
        value_deserializer: Deserializer,
        key: Optional[bytes],
        value: Optional[bytes],
        expected_key: Any,
        expected_value: Any,
    ):
        topic = Topic(
            "topic",
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )
        message = ConfluentKafkaMessageStub(key=key, value=value)
        row = topic.row_deserialize(message=message)
        assert row
        assert row.topic == message.topic()
        assert row.partition == message.partition()
        assert row.offset == message.offset()
        assert row.key == expected_key
        assert row.value == expected_value
        assert row.headers == message.headers()
        assert row.timestamp.type == message.timestamp()[0]
        assert row.timestamp.milliseconds == message.timestamp()[1]
        assert row.latency == message.latency()
        assert row.leader_epoch == message.leader_epoch()

    @pytest.mark.parametrize(
        "value_deserializer, value",
        [
            # Value is primitive
            (DoubleDeserializer(), float_to_bytes(1.23)),
            # Value is a list
            (JSONDeserializer(), b'[{"a":"b"}]'),
            # Serializer is allowed to return a list, but each item is a primitive
            (JSONListDeserializer(), b"[1,2,3]"),
        ],
    )
    def test_row_deserialize_value_is_not_mapping_error(
        self,
        value_deserializer: Deserializer,
        value: Optional[bytes],
    ):
        topic = Topic(
            "topic",
            value_deserializer=value_deserializer,
        )
        message = ConfluentKafkaMessageStub(key=b"key", value=value)
        with pytest.raises(TypeError, match="Row value must be a Mapping"):
            topic.row_deserialize(message=message)

    def test_row_deserialize_ignorevalueerror_raised(self):
        topic = Topic(
            "topic",
            value_deserializer=IgnoreDivisibleBy3Deserializer(column_name="value"),
        )
        row = topic.row_deserialize(
            message=ConfluentKafkaMessageStub(key=b"key", value=int_to_bytes(4))
        )
        assert row
        assert row.value == {"value": 4}

        row = topic.row_deserialize(
            message=ConfluentKafkaMessageStub(key=b"key", value=int_to_bytes(3))
        )
        assert row is None

    def test_row_deserialize_split_values(self):
        topic = Topic(
            "topic",
            value_deserializer=JSONListDeserializer(),
        )
        value = b'[{"a":"b"}, {"c":123}]'
        message = ConfluentKafkaMessageStub(key=b"key", value=value)
        rows = topic.row_deserialize(message=message)
        assert isinstance(rows, list)
        assert len(rows) == 2
        assert rows[0].value == {"a": "b"}
        assert rows[1].value == {"c": 123}

        assert rows[0].key == rows[1].key
        assert rows[0].topic == rows[1].topic
        assert rows[0].partition == rows[1].partition
        assert rows[0].offset == rows[1].offset

    @pytest.mark.parametrize(
        "key_deserializer, value_deserializer",
        [
            (None, None),
            (BytesDeserializer(), None),
            (None, BytesDeserializer()),
        ],
    )
    def test_row_deserialize_deserializer_isnot_provided_error(
        self, key_deserializer, value_deserializer
    ):
        topic = Topic(
            "topic",
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )
        with pytest.raises(DeserializerIsNotProvidedError):
            topic.row_deserialize(
                message=ConfluentKafkaMessageStub(key=b"key", value=b"123")
            )

    @pytest.mark.parametrize(
        "key_serializer, value_serializer, key, value, expected_key, expected_value",
        [
            (
                BytesSerializer(),
                BytesSerializer(),
                b"key",
                b"value",
                b"key",
                b"value",
            ),
            (
                BytesSerializer(),
                JSONSerializer(),
                b"key",
                {"field": "value"},
                b"key",
                '{"field": "value"}',
            ),
        ],
    )
    def test_row_serialize_success(
        self,
        key_serializer: Serializer,
        value_serializer: Serializer,
        key: Any,
        value: Any,
        expected_key: Optional[bytes],
        expected_value: Optional[bytes],
    ):
        topic = Topic(
            "topic", key_serializer=key_serializer, value_serializer=value_serializer
        )
        row = row_factory(key=key, value=value)
        message = topic.row_serialize(row=row)
        assert message.key == expected_key
        assert message.value == expected_value
        assert message.headers == row.headers

    @pytest.mark.parametrize(
        "key_serializer, value_serializer, key, value",
        [
            (
                BytesSerializer(),
                IntegerSerializer(),
                b"key",
                "value",
            ),
            (
                IntegerSerializer(),
                JSONSerializer(),
                "key",
                b"value",
            ),
        ],
    )
    def test_row_serialize_error(
        self,
        key_serializer: Serializer,
        value_serializer: Serializer,
        key: Any,
        value: Any,
    ):
        topic = Topic(
            "topic", key_serializer=key_serializer, value_serializer=value_serializer
        )

        row = row_factory(key=key, value=value)
        with pytest.raises(SerializationError):
            topic.row_serialize(row=row)
