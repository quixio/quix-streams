import json
from typing import Any, Callable, List, Optional

import pytest

from quixstreams.models import (
    KafkaHeaders,
    StringSerializer,
    TimestampType,
    Topic,
    TopicConfig,
)
from quixstreams.models.serializers import (
    DESERIALIZERS,
    SERIALIZERS,
    BytesDeserializer,
    BytesSerializer,
    Deserializer,
    DoubleDeserializer,
    IgnoreMessage,
    IntegerDeserializer,
    IntegerSerializer,
    JSONDeserializer,
    JSONSerializer,
    SerializationContext,
    SerializationError,
    Serializer,
)
from quixstreams.models.topics.exceptions import TopicConfigurationError
from tests.utils import ConfluentKafkaMessageStub

from ..utils import float_to_bytes, int_to_bytes


class JSONListDeserializer(JSONDeserializer):
    def split_values(self) -> bool:
        return True


class IgnoreDivisibleBy3Deserializer(IntegerDeserializer):
    def __call__(self, value: bytes, ctx: SerializationContext):
        deserialized = self._deserializer(value=value)
        if not deserialized % 3:
            raise IgnoreMessage("Ignore numbers divisible by 3")
        return deserialized


class TestTopic:
    @pytest.mark.parametrize(
        "key_deserializer, value_deserializer, key, value, expected_key, expected_value",
        [
            (
                IntegerDeserializer(),
                IntegerDeserializer(),
                int_to_bytes(1),
                int_to_bytes(2),
                1,
                2,
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
                JSONDeserializer(),
                float_to_bytes(1.1),
                json.dumps({"key": "value"}).encode(),
                1.1,
                {"key": "value"},
            ),
            (
                BytesDeserializer(),
                DoubleDeserializer(),
                b"key",
                float_to_bytes(1.23),
                b"key",
                1.23,
            ),
            (
                BytesDeserializer(),
                JSONDeserializer(),
                b"key",
                b'[{"a":"b"}]',
                b"key",
                [{"a": "b"}],
            ),
            (
                BytesDeserializer(),
                JSONDeserializer(),
                b"key",
                b"[1,2,3]",
                b"key",
                [1, 2, 3],
            ),
            (
                JSONDeserializer(),
                JSONDeserializer(),
                None,
                b"[1,2,3]",
                None,
                [1, 2, 3],
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
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
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
        assert row.timestamp == message.timestamp()[1]
        assert row.leader_epoch == message.leader_epoch()

    @pytest.mark.parametrize(
        "key_deserializer, value_deserializer, key, value, expected_key, expected_value",
        [
            (
                BytesDeserializer(),
                JSONListDeserializer(),
                b"key",
                b"[1,2,3]",
                b"key",
                [1, 2, 3],
            ),
            (
                BytesDeserializer(),
                JSONListDeserializer(),
                b"key",
                b'[{"a":"b"}]',
                b"key",
                [{"a": "b"}],
            ),
        ],
    )
    def test_row_list_deserialize_success(
        self,
        key_deserializer: Deserializer,
        value_deserializer: Deserializer,
        key: Optional[bytes],
        value: Optional[bytes],
        expected_key: Any,
        expected_value: Any,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )
        message = ConfluentKafkaMessageStub(key=key, value=value)
        rows = topic.row_deserialize(message=message)

        assert rows
        assert isinstance(rows, list)
        assert [r.value for r in rows] == expected_value
        for row in rows:
            assert row.topic == message.topic()
            assert row.partition == message.partition()
            assert row.offset == message.offset()
            assert row.key == expected_key
            assert row.headers == message.headers()
            assert row.timestamp == message.timestamp()[1]
            assert row.leader_epoch == message.leader_epoch()

    def test_row_deserialize_ignorevalueerror_raised(self, topic_manager_topic_factory):
        topic = topic_manager_topic_factory(
            value_deserializer=IgnoreDivisibleBy3Deserializer(),
        )
        row = topic.row_deserialize(
            message=ConfluentKafkaMessageStub(key=b"key", value=int_to_bytes(4))
        )
        assert row
        assert row.value == 4

        row = topic.row_deserialize(
            message=ConfluentKafkaMessageStub(key=b"key", value=int_to_bytes(3))
        )
        assert row is None

    def test_row_deserialize_split_values(self, topic_manager_topic_factory):
        topic = topic_manager_topic_factory(
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
        "value_deserializer, value, headers, timestamp_extractor, expected_timestamps",
        [
            (
                JSONDeserializer(),
                json.dumps({"some": "thing", "ts": 123}).encode(),
                None,
                lambda v, *_: v["ts"],
                [123],
            ),
            (
                JSONListDeserializer(),
                json.dumps([{"ts": 123}, {"ts": 456}]).encode(),
                None,
                lambda v, *_: v["ts"],
                [123, 456],
            ),
            (
                JSONListDeserializer(),
                json.dumps([{"ts": 456}]).encode(),
                [("header_ts", 333)],
                lambda v, headers, *_: headers[0][1],
                [333],
            ),
            (
                JSONListDeserializer(),
                json.dumps([{"ts": 456}]).encode(),
                None,
                lambda v, headers, ts, *_: ts + 1,  # 123 is default ts in tests
                [124],
            ),
            (
                JSONListDeserializer(),
                json.dumps([{"ts": 456}]).encode(),
                None,
                lambda v, headers, ts, ts_type: (
                    101 if ts_type == TimestampType.TIMESTAMP_CREATE_TIME else 0
                ),
                [101],
            ),
        ],
    )
    def test_row_deserialize_timestamp_extractor(
        self,
        value_deserializer: Deserializer,
        value: Optional[bytes],
        headers: KafkaHeaders,
        timestamp_extractor: Callable,
        expected_timestamps: List[int],
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            value_deserializer=value_deserializer,
            timestamp_extractor=timestamp_extractor,
        )
        message = ConfluentKafkaMessageStub(value=value, headers=headers)
        row_or_rows = topic.row_deserialize(message=message)

        rows = row_or_rows if isinstance(row_or_rows, list) else [row_or_rows]

        for index, row in enumerate(rows):
            assert row.timestamp == expected_timestamps[index]

    @pytest.mark.parametrize(
        "key_serializer, value_serializer, key, value, new_key, expected_key, "
        "expected_value",
        [
            (
                BytesSerializer(),
                BytesSerializer(),
                b"key",
                b"value",
                b"new_key",
                b"new_key",
                b"value",
            ),
            (
                BytesSerializer(),
                BytesSerializer(),
                b"key",
                b"value",
                None,
                None,
                b"value",
            ),
            (
                StringSerializer(),
                JSONSerializer(),
                "key",
                {"field": "value"},
                "new_key",
                b"new_key",
                b'{"field":"value"}',
            ),
            (
                JSONSerializer(),
                JSONSerializer(),
                "key",
                {"field": "value"},
                None,
                None,
                b'{"field":"value"}',
            ),
        ],
    )
    def test_row_serialize(
        self,
        key_serializer: Serializer,
        value_serializer: Serializer,
        key: Any,
        value: Any,
        new_key: Any,
        expected_key: Optional[bytes],
        expected_value: Optional[bytes],
        row_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        row = row_factory(key=key, value=value)
        message = topic.row_serialize(row=row, key=new_key)
        assert message.key == expected_key
        assert message.value == expected_value
        assert not message.headers

    def test_row_serialize_extra_headers(
        self, row_factory, topic_manager_topic_factory
    ):
        class BytesSerializerWithHeaders(BytesSerializer):
            extra_headers = {"header": b"value"}

        key_serializer = BytesSerializer()
        value_serializer = BytesSerializerWithHeaders()

        topic = topic_manager_topic_factory(
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        row = row_factory(key=b"key", value=b"value")  # noqa
        message = topic.row_serialize(row=row, key=row.key)
        assert message.key == b"key"
        assert message.value == b"value"
        assert message.headers == list(value_serializer.extra_headers.items())

    @pytest.mark.parametrize(
        "headers, headers_extra, expected_headers",
        [
            (None, {}, []),
            ([], {}, []),
            ([("key", b"value")], {}, [("key", b"value")]),
            ([("key", b"value")], {"key": b"value2"}, [("key", b"value2")]),
            (
                [("key", b"value")],
                {"key2": b"value2"},
                [("key", b"value"), ("key2", b"value2")],
            ),
        ],
    )
    def test_row_serialize_extra_headers_with_original_headers(
        self,
        headers,
        headers_extra,
        expected_headers,
        row_factory,
        topic_manager_topic_factory,
    ):
        class BytesSerializerWithHeaders(BytesSerializer):
            extra_headers = headers_extra

        key_serializer = BytesSerializer()
        value_serializer = BytesSerializerWithHeaders()

        topic = topic_manager_topic_factory(
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        row = row_factory(key=b"key", value=b"value", headers=headers)  # noqa
        message = topic.row_serialize(row=row, key=row.key)
        assert message.key == b"key"
        assert message.value == b"value"
        assert message.headers == expected_headers

    def test_serialize(self, topic_json_serdes_factory, topic_manager_topic_factory):
        topic = topic_manager_topic_factory(
            key_serializer="str", value_serializer="json"
        )
        message = topic.serialize(
            key="woo",
            value={"a": ["cool", "json"]},
            headers={"header": "value"},
            timestamp_ms=1234567890,
        )
        assert message.key == b"woo"
        assert message.value == b'{"a":["cool","json"]}'
        assert message.headers == {"header": "value"}
        assert message.timestamp == 1234567890

    def test_serialize_no_key(
        self, topic_json_serdes_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer="json")
        message = topic.serialize(
            value={"a": ["cool", "json"]},
            headers={"header": "value"},
            timestamp_ms=1234567890,
        )
        assert message.key is None
        assert message.value == b'{"a":["cool","json"]}'
        assert message.headers == {"header": "value"}
        assert message.timestamp == 1234567890

    @pytest.mark.skip(
        "string serializer currently allows NoneTypes, probably shouldn't?"
    )
    def test_serialize_key_missing(
        self, topic_json_serdes_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(
            key_serializer="string", value_serializer="json"
        )
        with pytest.raises(SerializationError):
            topic.serialize(
                value={"a": ["cool", "json"]},
                headers={"header": "value"},
                timestamp_ms=1234567890,
            )

    @pytest.mark.skip("skip until we do more type checking with serializers")
    def test_serialize_serialization_error(
        self, topic_json_serdes_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(
            key_serializer="bytes", value_serializer="json"
        )
        with pytest.raises(SerializationError):
            topic.serialize(
                key="woo",
                value={"a": ["cool", "json"]},
                headers={"header": "value"},
                timestamp_ms=1234567890,
            )

    @pytest.mark.parametrize(
        "deserializers, input_kv, expected_kv",
        [
            (
                {"key_deserializer": "str", "value_deserializer": "json"},
                {"key": b"woo", "value": b'{"a":["cool","json"]}'},
                {"key": "woo", "value": {"a": ["cool", "json"]}},
            ),
            (
                {"key_deserializer": "bytes", "value_deserializer": "int"},
                {"key": None, "value": int_to_bytes(12345)},
                {"key": None, "value": 12345},
            ),
            (
                {"key_deserializer": "str", "value_deserializer": "json"},
                {"key": None, "value": None},
                {"key": None, "value": None},
            ),
        ],
    )
    def test_deserialize(
        self, topic_manager_topic_factory, deserializers, input_kv, expected_kv
    ):
        topic = topic_manager_topic_factory(**deserializers)
        message = topic.deserialize(
            ConfluentKafkaMessageStub(
                **input_kv,
                headers=[("header", b"value")],
                timestamp=(1000, 2000),
            )
        )
        assert {"key": message.key, "value": message.value} == expected_kv
        assert message.headers == [("header", b"value")]
        assert message.timestamp == 2000

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
        row_factory: pytest.fixture,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            key_serializer=key_serializer, value_serializer=value_serializer
        )

        row = row_factory(key=key, value=value)
        with pytest.raises(SerializationError):
            topic.row_serialize(row=row, key=row.key)

    @pytest.mark.parametrize(
        "serializer_str, expected_type", [(k, v) for k, v in SERIALIZERS.items()]
    )
    def test__get_serializer_strings(
        self, serializer_str, expected_type, topic_manager_topic_factory
    ):
        assert isinstance(
            topic_manager_topic_factory(key_serializer=serializer_str)._key_serializer,
            expected_type,
        )

    def test__get_serializer_strings_invalid(self, topic_manager_topic_factory):
        with pytest.raises(ValueError):
            topic_manager_topic_factory(key_serializer="fail_me_bro")

    @pytest.mark.parametrize(
        "deserializer_str, expected_type", [(k, v) for k, v in DESERIALIZERS.items()]
    )
    def test__get_deserializer_strings(
        self, deserializer_str, expected_type, topic_manager_topic_factory
    ):
        assert isinstance(
            topic_manager_topic_factory(
                key_deserializer=deserializer_str
            )._key_deserializer,
            expected_type,
        )

    def test__get_deserializer_strings_invalid(self, topic_manager_topic_factory):
        with pytest.raises(ValueError):
            topic_manager_topic_factory(key_deserializer="fail_me_bro")

    def test_topic_real_config_success(self):
        topic = Topic(name="test")
        config = TopicConfig(
            num_partitions=3, replication_factor=5, extra_config={"a": "b"}
        )
        topic.broker_config = config

        assert topic.broker_config == config

    def test_topic_real_config_not_set_fails(self):
        topic = Topic(name="test")
        with pytest.raises(
            TopicConfigurationError, match="The broker topic configuration is missing"
        ):
            topic.broker_config

    @pytest.mark.parametrize(
        "serializer, exc_match",
        [
            ("abc", "Unknown serializer option"),
            (object(), "Serializer must be either one of"),
        ],
    )
    def test_invalid_serializer(self, serializer, exc_match):
        with pytest.raises(ValueError, match=exc_match):
            Topic(name="test", key_serializer=serializer)
        with pytest.raises(ValueError, match=exc_match):
            Topic(name="test", value_serializer=serializer)

    @pytest.mark.parametrize(
        "deserializer, exc_match",
        [
            ("abc", "Unknown deserializer option"),
            (object(), "Deserializer must be either one of"),
        ],
    )
    def test_invalid_deserializer(self, deserializer, exc_match):
        with pytest.raises(ValueError, match=exc_match):
            Topic(name="test", key_deserializer=deserializer)
        with pytest.raises(ValueError, match=exc_match):
            Topic(name="test", value_deserializer=deserializer)
