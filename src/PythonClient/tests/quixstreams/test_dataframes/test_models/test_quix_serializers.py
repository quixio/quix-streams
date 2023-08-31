import base64
import json
import time

import pytest

from src.quixstreams.dataframes.models.serializers import (
    SerializationError,
    SerializationContext,
    IgnoreMessage,
)
from src.quixstreams.dataframes.models.serializers.quix import (
    QuixTimeseriesDeserializer,
    QuixEventsDeserializer,
    QuixTimeseriesSerializer,
    QuixEventsSerializer,
    QCodecId,
    QModelKey,
    Q_SPLITMESSAGEID_NAME,
)


class TestQuixDeserializersValidation:
    @pytest.mark.parametrize(
        "deserializer", [QuixTimeseriesDeserializer(), QuixEventsDeserializer()]
    )
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
        self, headers, error, deserializer
    ):
        with pytest.raises(SerializationError, match=error):
            list(
                deserializer(
                    value=b"", ctx=SerializationContext(topic="topic", headers=headers)
                )
            )

    @pytest.mark.parametrize(
        "deserializer", [QuixTimeseriesDeserializer(), QuixEventsDeserializer()]
    )
    def test_deserialize_message_not_json_fails(self, deserializer):
        with pytest.raises(
            SerializationError,
            match="the JSON object must be str, bytes or bytearray, not int",
        ):
            list(
                deserializer(  # noqa
                    value=123,
                    ctx=SerializationContext(
                        topic="test",
                        headers=[
                            ("__Q_ModelKey", b"TimeseriesData"),
                            ("__Q_CodecId", b"JT"),
                        ],
                    ),
                )
            )

    @pytest.mark.parametrize(
        "deserializer", [QuixTimeseriesDeserializer(), QuixEventsDeserializer()]
    )
    @pytest.mark.parametrize("model_key", QModelKey.KEYS_TO_IGNORE)
    def test_deserialize_message_is_ignored(
        self, model_key, deserializer, quix_timeseries_factory
    ):
        # We don't care about a particular message structure in this test
        message = quix_timeseries_factory(model_key=model_key)
        with pytest.raises(IgnoreMessage):
            list(
                deserializer(
                    value=message.value(),
                    ctx=SerializationContext(
                        topic=message.topic(), headers=message.headers()
                    ),
                )
            )


class TestQuixTimeseriesDeserializer:
    def test_deserialize_timeseries_success(self, quix_timeseries_factory):
        message = quix_timeseries_factory(
            binary={"param1": [b"1", None], "param2": [None, b"1"]},
            strings={"param3": [1, None], "param4": [None, 1.1]},
            numeric={"param5": ["1", None], "param6": [None, "a"], "param7": ["", ""]},
            tags={"tag1": ["value1", "value2"], "tag2": ["value3", "value4"]},
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
            },
        ]

        deserializer = QuixTimeseriesDeserializer()
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(
                    topic=message.topic(),
                    headers=message.headers(),
                ),
            )
        )
        assert len(rows) == len(expected)
        for item, row in zip(expected, rows):
            assert "__Q_Timestamp" in row
            assert row["__Q_Timestamp"]
            for key in item:
                assert item[key] == row[key]

    def test_deserialize_timeseries_with_column_name_success(
        self, quix_timeseries_factory
    ):
        message = quix_timeseries_factory(
            binary={"param1": [b"1", None], "param2": [None, b"1"]},
            strings={"param3": [1, None], "param4": [None, 1.1]},
            numeric={"param5": ["1", None], "param6": [None, "a"], "param7": ["", ""]},
            tags={"tag1": ["value1", "value2"], "tag2": ["value3", "value4"]},
        )

        expected = [
            {
                "root": {
                    "param1": b"1",
                    "param2": None,
                    "param3": 1,
                    "param4": None,
                    "param5": "1",
                    "param6": None,
                    "param7": "",
                    "Tags": {"tag1": "value1", "tag2": "value3"},
                }
            },
            {
                "root": {
                    "param1": None,
                    "param2": b"1",
                    "param3": None,
                    "param4": 1.1,
                    "param5": None,
                    "param6": "a",
                    "param7": "",
                    "Tags": {"tag1": "value2", "tag2": "value4"},
                }
            },
        ]

        deserializer = QuixTimeseriesDeserializer(column_name="root")
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(
                    topic=message.topic(),
                    headers=message.headers(),
                ),
            )
        )
        assert len(rows) == len(expected)
        for item, row in zip(expected, rows):
            assert "root" in row
            value = row["root"]
            item = row["root"]
            assert "__Q_Timestamp" in value
            assert value["__Q_Timestamp"]
            for key in item:
                assert item[key] == value[key]


class TestQuixTimeseriesSerializer:
    def test_serialize_dict_success(self):
        serializer = QuixTimeseriesSerializer()
        value = {
            "int": 1,
            "float": 1.0,
            "str": "abc",
            "bytes": b"123",
            "bytearray": bytearray(b"Hi"),
            "Tags": {"tag1": "tag1", "tag2": "tag2"},
        }
        timestamp_ns = time.time_ns()
        serialized = serializer(
            value, timestamp_ns=timestamp_ns, ctx=SerializationContext(topic="test")
        )

        expected = {
            "Timestamps": [timestamp_ns],
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

    @pytest.mark.parametrize(
        "value",
        [
            {},  # empty dict
            {"a": None, "b": None},  # all values are None
        ],
    )
    def test_serialize_dict_empty_or_none(self, value):
        serializer = QuixTimeseriesSerializer()
        timestamp_ns = time.time_ns()
        serialized = serializer(
            value, ctx=SerializationContext(topic="test"), timestamp_ns=timestamp_ns
        )
        expected = {
            "Timestamps": [],
            "BinaryValues": {},
            "NumericValues": {},
            "StringValues": {},
            "TagValues": {},
        }
        assert json.loads(serialized) == expected

    @pytest.mark.parametrize("value", ["", 0, True, [], ()])
    def test_serialize_not_mapping(self, value):
        serializer = QuixTimeseriesSerializer()
        with pytest.raises(SerializationError, match="Expected Mapping"):
            serializer(value, ctx=SerializationContext(topic="test"))  # noqa

    @pytest.mark.parametrize("item", [True, [], (), object()])
    def test_serialize_item_of_unsupported_type(self, item):
        serializer = QuixTimeseriesSerializer()
        with pytest.raises(
            SerializationError, match='Item with key "item" has unsupported type'
        ):
            serializer({"item": item}, ctx=SerializationContext(topic="test"))


class TestQuixEventsDeserializer:
    def test_deserialize_eventdata_success(
        self, quix_eventdata_factory, quix_eventdata_params_factory
    ):
        event_params = quix_eventdata_params_factory(
            id="test",
            value={"blabla": 123},
            tags={"tag1": "1"},
        )
        message = quix_eventdata_factory(params=event_params)

        deserializer = QuixEventsDeserializer()
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(topic="test", headers=message.headers()),
            )
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["__Q_Timestamp"]
        assert row["Id"] == event_params.id
        assert row["Value"] == event_params.value
        assert row["Tags"] == event_params.tags

    def test_deserialize_eventdata_list_success(
        self,
        quix_eventdata_list_factory,
        quix_eventdata_params_factory,
    ):
        event_params = [
            quix_eventdata_params_factory(
                id="test",
                value={"blabla": 123},
                tags={"tag1": "1"},
            ),
            quix_eventdata_params_factory(
                id="test2",
                value={"blabla2": 1234},
                tags={"tag2": "2"},
            ),
        ]
        message = quix_eventdata_list_factory(params=event_params)

        deserializer = QuixEventsDeserializer()
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(topic="test", headers=message.headers()),
            )
        )
        assert len(rows) == 2
        for row, params in zip(rows, event_params):
            assert row["__Q_Timestamp"]
            assert row["Id"] == params.id
            assert row["Value"] == params.value
            assert row["Tags"] == params.tags

    def test_deserialize_event_data_with_column(
        self,
        quix_eventdata_list_factory,
        quix_eventdata_params_factory,
    ):
        event_params = [
            quix_eventdata_params_factory(
                id="test",
                value={"blabla": 123},
                tags={"tag1": "1"},
            ),
            quix_eventdata_params_factory(
                id="test2",
                value={"blabla2": 1234},
                tags={"tag2": "2"},
            ),
        ]
        message = quix_eventdata_list_factory(params=event_params)

        deserializer = QuixEventsDeserializer(column_name="root")
        rows = list(
            deserializer(
                value=message.value(),
                ctx=SerializationContext(topic="test", headers=message.headers()),
            )
        )
        assert len(rows) == 2
        for row, params in zip(rows, event_params):
            assert "root" in row
            row = row["root"]
            assert row["__Q_Timestamp"]
            assert row["Id"] == params.id
            assert row["Value"] == params.value
            assert row["Tags"] == params.tags


class TestQuixEventsSerializer:
    def test_serialize_success(self):
        serializer = QuixEventsSerializer()
        value = {"Id": "id", "Value": "value", "Tags": {"tag1": "tag1"}}
        timestamp_ns = time.time_ns()
        expected = {
            "Id": "id",
            "Value": "value",
            "Tags": {"tag1": "tag1"},
            "Timestamp": timestamp_ns,
        }
        ctx = SerializationContext("test")
        assert (
            json.loads(serializer(value, timestamp_ns=timestamp_ns, ctx=ctx))
            == expected
        )

    @pytest.mark.parametrize("value", [0, "", object(), [], (), set()])
    def test_serialize_not_a_mapping(self, value):
        serializer = QuixEventsSerializer()
        with pytest.raises(SerializationError, match="Expected Mapping"):
            serializer(value, ctx=SerializationContext("test"))  # noqa

    def test_serialize_id_isnot_string(self):
        serializer = QuixEventsSerializer()
        with pytest.raises(
            SerializationError, match='Field "Id" is expected to be of type "str"'
        ):
            serializer({"Id": 0, "Value": "abc"}, ctx=SerializationContext("test"))

    def test_serialize_value_isnot_string(self):
        serializer = QuixEventsSerializer()
        with pytest.raises(
            SerializationError, match='Field "Value" is expected to be of type "str"'
        ):
            serializer({"Id": "id", "Value": 1}, ctx=SerializationContext("test"))

    def test_serialize_tags_isnot_dict(self):
        serializer = QuixEventsSerializer()
        with pytest.raises(
            SerializationError, match='Field "Tags" is expected to be of type "dict"'
        ):
            serializer(
                {"Id": "id", "Value": "value", "Tags": 1},
                ctx=SerializationContext("test"),
            )
