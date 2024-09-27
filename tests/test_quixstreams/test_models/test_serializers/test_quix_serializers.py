import base64
import gzip
import json

import pytest

from quixstreams.models.serializers import (
    SerializationError,
    SerializationContext,
    IgnoreMessage,
    MessageField,
)
from quixstreams.models.serializers.quix import (
    QuixDeserializer,
    QuixTimeseriesSerializer,
    QuixEventsSerializer,
    QCodecId,
    QModelKey,
    Q_SPLITMESSAGEID_NAME,
)


class TestQuixDeserializersValidation:
    @pytest.mark.parametrize(
        "headers, error",
        [
            # Empty headers
            ([], f'"{QCodecId.HEADER_NAME}" header is missing or empty'),
            # Codec header is provided but empty
            (
                [(QCodecId.HEADER_NAME, b"")],
                f'"{QCodecId.HEADER_NAME}" header is missing or empty',
            ),
            # ModelKey header is not provided
            (
                [(QCodecId.HEADER_NAME, b"JT")],
                f'"{QModelKey.HEADER_NAME}" header is missing or empty',
            ),
            # ModelKey header is provided but empty
            (
                [(QCodecId.HEADER_NAME, b"JT"), (QModelKey.HEADER_NAME, b"")],
                f'"{QModelKey.HEADER_NAME}" header is missing or empty',
            ),
            # Codec header value is incorrect
            (
                [(QCodecId.HEADER_NAME, b"BLABLA"), (QModelKey.HEADER_NAME, b"BLABLA")],
                f'Unsupported "{QCodecId.HEADER_NAME}" value "BLABLA"',
            ),
            # GZIP prefix is provided but codec header value is incorrect
            (
                [
                    (QCodecId.HEADER_NAME, f"{QCodecId.GZIP_PREFIX}BLABLA".encode()),
                    (QModelKey.HEADER_NAME, b"BLABLA"),
                ],
                f'Unsupported "{QCodecId.HEADER_NAME}" value "BLABLA"',
            ),
            # ModelKey header value is incorrect
            (
                [(QCodecId.HEADER_NAME, b"JT"), (QModelKey.HEADER_NAME, b"BLABLA")],
                f'Unsupported "{QModelKey.HEADER_NAME}" value "BLABLA"',
            ),
            # Message is a part of the multi-message split
            (
                [
                    (Q_SPLITMESSAGEID_NAME, b"blabla"),
                ],
                f'Detected "{Q_SPLITMESSAGEID_NAME}" header but message splitting '
                f"is not supported",
            ),
        ],
    )
    def test_deserialize_missing_or_invalid_headers_fails(
        self,
        headers,
        error,
    ):
        deserializer = QuixDeserializer()
        with pytest.raises(SerializationError, match=error):
            list(
                deserializer(
                    value=b"",
                    ctx=SerializationContext(
                        topic="topic", field=MessageField.VALUE, headers=headers
                    ),
                )
            )

    def test_deserialize_message_not_json_fails(self):
        deserializer = QuixDeserializer()
        with pytest.raises(
            SerializationError,
            match="Input must be bytes, bytearray, memoryview, or str",
        ):
            list(
                deserializer(  # noqa
                    value=123,
                    ctx=SerializationContext(
                        topic="test",
                        field=MessageField.VALUE,
                        headers=[
                            ("__Q_ModelKey", b"TimeseriesData"),
                            ("__Q_CodecId", b"JT"),
                        ],
                    ),
                )
            )

    @pytest.mark.parametrize("model_key", QModelKey.KEYS_TO_IGNORE)
    def test_deserialize_message_is_ignored(self, model_key, quix_timeseries_factory):
        # We don't care about a particular message structure in this test
        message = quix_timeseries_factory(model_key=model_key)
        deserializer = QuixDeserializer()
        with pytest.raises(IgnoreMessage):
            list(
                deserializer(
                    value=message.value(),
                    ctx=SerializationContext(
                        topic=message.topic(),
                        field=MessageField.VALUE,
                        headers=message.headers(),
                    ),
                )
            )

    def test_deserialize_legacy_multi_part_message_is_ignored(
        self, quix_timeseries_factory
    ):
        message = quix_timeseries_factory(as_legacy=True)
        deserializer = QuixDeserializer()
        with pytest.raises(SerializationError):
            list(
                deserializer(
                    value=b"<1234/10/1>" + message.value(),
                    ctx=SerializationContext(
                        topic=message.topic(),
                        field=MessageField.VALUE,
                        headers=message.headers(),
                    ),
                )
            )


class TestQuixDeserializer:
    @pytest.mark.parametrize("as_legacy", [False, True])
    def test_deserialize_timeseries_success(self, quix_timeseries_factory, as_legacy):
        message = quix_timeseries_factory(
            binary={"param1": [b"1", None], "param2": [None, b"1"]},
            strings={"param3": [1, None], "param4": [None, 1.1]},
            numeric={"param5": ["1", None], "param6": [None, "a"], "param7": ["", ""]},
            tags={"tag1": ["value1", "value2"], "tag2": ["value3", "value4"]},
            timestamps=[1234567890, 1234567891],
            as_legacy=as_legacy,
        )

        expected = [
            {
                "param1": b"1",
                "param2": None,
                "param3": 1,
                "param4": None,
                "param5": "1",
                "param6": None,
                "param7": "",
                "Tags": {"tag1": "value1", "tag2": "value3"},
                "Timestamp": 1234567890,
            },
            {
                "param1": None,
                "param2": b"1",
                "param3": None,
                "param4": 1.1,
                "param5": None,
                "param6": "a",
                "param7": "",
                "Tags": {"tag1": "value2", "tag2": "value4"},
                "Timestamp": 1234567891,
            },
        ]

        deserializer = QuixDeserializer()
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(
                    topic=message.topic(),
                    field=MessageField.VALUE,
                    headers=message.headers(),
                ),
            )
        )
        assert len(rows) == len(expected)
        for item, row in zip(expected, rows):
            for key in item:
                assert item[key] == row[key]

    def test_deserialize_timeseries_gzip_success(self, quix_timeseries_factory):
        message = quix_timeseries_factory(
            binary={"param1": [b"1", None], "param2": [None, b"1"]},
            strings={"param3": [1, None], "param4": [None, 1.1]},
            numeric={"param5": ["1", None], "param6": [None, "a"], "param7": ["", ""]},
            tags={"tag1": ["value1", "value2"], "tag2": ["value3", "value4"]},
            timestamps=[1234567890, 1234567891],
            as_legacy=False,
            codec_id=QCodecId.GZIP_PREFIX + QCodecId.JSON_TYPED,
        )
        value_zipped = gzip.compress(message.value())

        expected = [
            {
                "param1": b"1",
                "param2": None,
                "param3": 1,
                "param4": None,
                "param5": "1",
                "param6": None,
                "param7": "",
                "Tags": {"tag1": "value1", "tag2": "value3"},
                "Timestamp": 1234567890,
            },
            {
                "param1": None,
                "param2": b"1",
                "param3": None,
                "param4": 1.1,
                "param5": None,
                "param6": "a",
                "param7": "",
                "Tags": {"tag1": "value2", "tag2": "value4"},
                "Timestamp": 1234567891,
            },
        ]

        deserializer = QuixDeserializer()
        rows = list(
            deserializer(
                value=value_zipped,
                ctx=SerializationContext(
                    topic=message.topic(),
                    field=MessageField.VALUE,
                    headers=message.headers(),
                ),
            )
        )
        assert len(rows) == len(expected)
        for item, row in zip(expected, rows):
            for key in item:
                assert item[key] == row[key]

    @pytest.mark.parametrize("as_legacy", [False, True])
    def test_deserialize_timeseries_timestamp_field_clash(
        self, quix_timeseries_factory, as_legacy
    ):
        message = quix_timeseries_factory(
            numeric={"param5": ["1", None], "Timestamp": [1, 2]},
            as_legacy=as_legacy,
        )

        error_string = (
            "There is a competing 'Timestamp' field name present at "
            "NumericValues.Timestamp: you must rename your field "
            "or specify a field to use for the 'Timestamps' parameter."
        )
        deserializer = QuixDeserializer()
        with pytest.raises(SerializationError, match=error_string):
            list(
                deserializer(
                    value=message.value(),
                    ctx=SerializationContext(
                        topic=message.topic(),
                        field=MessageField.VALUE,
                        headers=message.headers(),
                    ),
                )
            )

    @pytest.mark.parametrize("as_legacy", [False, True])
    def test_deserialize_eventdata_success(
        self, quix_eventdata_factory, quix_eventdata_params_factory, as_legacy
    ):
        event_params = quix_eventdata_params_factory(
            id="test", value={"blabla": 123}, tags={"tag1": "1"}, timestamp=1234567890
        )
        message = quix_eventdata_factory(params=event_params, as_legacy=as_legacy)

        deserializer = QuixDeserializer()
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(
                    topic="test", field=MessageField.VALUE, headers=message.headers()
                ),
            )
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["Timestamp"] == event_params.timestamp
        assert row["Id"] == event_params.id
        assert row["Value"] == event_params.value
        assert row["Tags"] == event_params.tags

    @pytest.mark.parametrize("as_legacy", [False, True])
    def test_deserialize_eventdata_list_success(
        self,
        quix_eventdata_list_factory,
        quix_eventdata_params_factory,
        as_legacy,
    ):
        event_params = [
            quix_eventdata_params_factory(
                id="test",
                value={"blabla": 123},
                tags={"tag1": "1"},
                timestamp=1234567890,
            ),
            quix_eventdata_params_factory(
                id="test2",
                value={"blabla2": 1234},
                tags={"tag2": "2"},
                timestamp=1234567891,
            ),
        ]
        message = quix_eventdata_list_factory(params=event_params, as_legacy=as_legacy)

        deserializer = QuixDeserializer()
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(
                    topic="test", field=MessageField.VALUE, headers=message.headers()
                ),
            )
        )
        assert len(rows) == 2
        for row, params in zip(rows, event_params):
            assert row["Timestamp"] == params.timestamp
            assert row["Id"] == params.id
            assert row["Value"] == params.value
            assert row["Tags"] == params.tags


class TestQuixTimeseriesSerializer:
    def test_serialize_dict_success(self):
        serializer = QuixTimeseriesSerializer(as_legacy=False)
        value = {
            "int": 1,
            "float": 1.0,
            "str": "abc",
            "bytes": b"123",
            "bytearray": bytearray(b"Hi"),
            "Tags": {"tag1": "tag1", "tag2": "tag2"},
            "Timestamp": 1234567890,
        }
        serialized = serializer(
            value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
        )

        expected = {
            "Timestamps": [1234567890],
            "BinaryValues": {
                "bytes": [base64.b64encode(value["bytes"]).decode("ascii")],
                "bytearray": [base64.b64encode(value["bytearray"]).decode("ascii")],
            },
            "StringValues": {
                "str": [
                    value["str"],
                ]
            },
            "NumericValues": {
                "int": [value["int"]],
                "float": [value["float"]],
            },
            "TagValues": {
                "tag1": [value["Tags"]["tag1"]],
                "tag2": [value["Tags"]["tag2"]],
            },
        }
        assert json.loads(serialized) == expected

    def test_legacy_serialize_dict_success(self):
        serializer = QuixTimeseriesSerializer(as_legacy=True)
        value = {
            "int": 1,
            "float": 1.0,
            "str": "abc",
            "bytes": b"123",
            "bytearray": bytearray(b"Hi"),
            "Tags": {"tag1": "tag1", "tag2": "tag2"},
            "Timestamp": 1234567890,
        }
        serialized = serializer(
            value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
        )
        expected_value = {
            "BinaryValues": {
                "bytes": [base64.b64encode(value["bytes"]).decode("ascii")],
                "bytearray": [base64.b64encode(value["bytearray"]).decode("ascii")],
            },
            "StringValues": {
                "str": [
                    value["str"],
                ]
            },
            "NumericValues": {
                "int": [value["int"]],
                "float": [value["float"]],
            },
            "Timestamps": [1234567890],
            "TagValues": {
                "tag1": [value["Tags"]["tag1"]],
                "tag2": [value["Tags"]["tag2"]],
            },
        }
        c = "JT"
        k = "ParameterData"
        s = len("{" + f'"C":"{c}","K":"{k}","V":')
        expected_value_bytes = json.dumps(
            expected_value, separators=(",", ":")
        ).encode()
        expected = {
            "C": c,
            "K": k,
            "V": expected_value,
            "S": s,
            "E": s + len(expected_value_bytes),
        }
        assert json.loads(serialized) == expected
        assert serialized[expected["S"] : expected["E"]] == expected_value_bytes

    @pytest.mark.parametrize("as_legacy", [True, False])
    def test_serialize_missing_timestamp(self, as_legacy):
        serializer = QuixTimeseriesSerializer(as_legacy=as_legacy)
        value = {
            "int": 1,
            "float": 1.0,
            "str": "abc",
            "bytes": b"123",
            "bytearray": bytearray(b"Hi"),
            "Tags": {"tag1": "tag1", "tag2": "tag2"},
        }

        with pytest.raises(
            SerializationError,
            match="Missing required Quix field: 'Timestamp'",
        ):
            serializer(
                value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
            )

    @pytest.mark.parametrize(
        "value",
        [
            {},  # empty dict
            {"a": None, "b": None},  # all values are None
        ],
    )
    def test_serialize_dict_empty_or_none_as_legacy_false(self, value):
        serializer = QuixTimeseriesSerializer(as_legacy=False)
        serialized = serializer(
            value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
        )
        expected = {
            "Timestamps": [],
            "BinaryValues": {},
            "NumericValues": {},
            "StringValues": {},
            "TagValues": {},
        }
        assert json.loads(serialized) == expected

    @pytest.mark.parametrize(
        "value",
        [
            {},  # empty dict
            {"a": None, "b": None},  # all values are None
        ],
    )
    def test_serialize_dict_empty_or_none_as_legacy_true(self, value):
        serializer = QuixTimeseriesSerializer(as_legacy=True)
        serialized = serializer(
            value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
        )
        expected_value = {
            "BinaryValues": {},
            "StringValues": {},
            "NumericValues": {},
            "Timestamps": [],
            "TagValues": {},
        }

        c = "JT"
        k = "ParameterData"
        s = len("{" + f'"C":"{c}","K":"{k}","V":')
        expected_value_str = json.dumps(expected_value, separators=(",", ":")).encode()
        expected = {
            "C": c,
            "K": k,
            "V": expected_value,
            "S": s,
            "E": s + len(expected_value_str),
        }
        assert json.loads(serialized) == expected
        assert serialized[expected["S"] : expected["E"]] == expected_value_str

    @pytest.mark.parametrize("as_legacy", [True, False])
    @pytest.mark.parametrize("value", ["", 0, True, [], ()])
    def test_serialize_not_mapping(self, value, as_legacy):
        serializer = QuixTimeseriesSerializer(as_legacy=as_legacy)
        with pytest.raises(SerializationError, match="Expected Mapping"):
            serializer(
                value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
            )  # noqa

    @pytest.mark.parametrize("as_legacy", [True, False])
    @pytest.mark.parametrize("item", [True, [], (), object()])
    def test_serialize_item_of_unsupported_type(self, item, as_legacy):
        serializer = QuixTimeseriesSerializer(as_legacy=as_legacy)
        with pytest.raises(
            SerializationError, match='Item with key "item" has unsupported type'
        ):
            serializer(
                {"item": item},
                ctx=SerializationContext(topic="test", field=MessageField.VALUE),
            )


class TestQuixEventsSerializer:
    def test_serialize_success(self):
        serializer = QuixEventsSerializer(as_legacy=False)
        value = {
            "Id": "id",
            "Value": "value",
            "Tags": {"tag1": "tag1"},
            "Timestamp": 1234567890,
        }
        expected = {
            "Id": "id",
            "Value": "value",
            "Tags": {"tag1": "tag1"},
            "Timestamp": 1234567890,
        }
        ctx = SerializationContext(topic="test", field=MessageField.VALUE)
        assert json.loads(serializer(value, ctx=ctx)) == expected

    def test_legacy_serialize_success(self):
        serializer = QuixEventsSerializer(as_legacy=True)
        value = {
            "Id": "id",
            "Value": "value",
            "Tags": {"tag1": "tag1"},
            "Timestamp": 1234567890,
        }
        expected_value = {
            "Id": "id",
            "Value": "value",
            "Tags": {"tag1": "tag1"},
            "Timestamp": 1234567890,
        }
        c = "JT"
        k = "EventData"
        s = len("{" + f'"C":"{c}","K":"{k}","V":')
        expected_value_bytes = json.dumps(
            expected_value, separators=(",", ":")
        ).encode()
        expected = {
            "C": c,
            "K": k,
            "V": expected_value,
            "S": s,
            "E": s + len(expected_value_bytes),
        }
        ctx = SerializationContext(topic="test", field=MessageField.VALUE)
        serialized = serializer(value, ctx=ctx)
        assert json.loads(serialized) == expected
        assert serialized[expected["S"] : expected["E"]] == expected_value_bytes

    @pytest.mark.parametrize("as_legacy", [True, False])
    def test_serialize_missing_timestamp(self, as_legacy):
        serializer = QuixEventsSerializer(as_legacy=as_legacy)
        value = {
            "Id": "id",
            "Value": "value",
            "Tags": {"tag1": "tag1"},
        }
        with pytest.raises(
            SerializationError,
            match="Missing required Quix field: 'Timestamp'",
        ):
            serializer(
                value, ctx=SerializationContext(topic="test", field=MessageField.VALUE)
            )

    @pytest.mark.parametrize("as_legacy", [True, False])
    @pytest.mark.parametrize("value", [0, "", object(), [], (), set()])
    def test_serialize_not_a_mapping(self, value, as_legacy):
        serializer = QuixEventsSerializer(as_legacy=as_legacy)
        with pytest.raises(SerializationError, match="Expected Mapping"):
            serializer(
                value, ctx=SerializationContext("test", field=MessageField.VALUE)
            )  # noqa

    @pytest.mark.parametrize("as_legacy", [True, False])
    def test_serialize_id_isnot_string(self, as_legacy):
        serializer = QuixEventsSerializer(as_legacy=as_legacy)
        with pytest.raises(
            SerializationError, match='Field "Id" is expected to be of type "str"'
        ):
            serializer(
                {"Id": 0, "Value": "abc"},
                ctx=SerializationContext("test", field=MessageField.VALUE),
            )

    @pytest.mark.parametrize("as_legacy", [True, False])
    def test_serialize_value_isnot_string(self, as_legacy):
        serializer = QuixEventsSerializer(as_legacy=as_legacy)
        with pytest.raises(
            SerializationError, match='Field "Value" is expected to be of type "str"'
        ):
            serializer(
                {"Id": "id", "Value": 1},
                ctx=SerializationContext("test", field=MessageField.VALUE),
            )

    @pytest.mark.parametrize("as_legacy", [True, False])
    def test_serialize_tags_isnot_dict(self, as_legacy):
        serializer = QuixEventsSerializer(as_legacy=as_legacy)
        with pytest.raises(
            SerializationError, match='Field "Tags" is expected to be of type "dict"'
        ):
            serializer(
                {"Id": "id", "Value": "value", "Tags": 1},
                ctx=SerializationContext("test", field=MessageField.VALUE),
            )
