import logging
import operator
import uuid
import warnings
from collections import namedtuple
from datetime import timedelta
from typing import Any
from unittest import mock

import pytest

from quixstreams import State
from quixstreams.dataframe.exceptions import (
    GroupByDuplicate,
    GroupByNestingLimit,
    InvalidOperation,
    TopicPartitionsMismatch,
)
from quixstreams.dataframe.registry import DataFrameRegistry
from quixstreams.dataframe.windows.base import WindowResult
from quixstreams.models import TopicConfig
from tests.utils import DummySink

RecordStub = namedtuple("RecordStub", ("value", "key", "timestamp"))


class TestStreamingDataFrame:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (1, 2),
            ("input", "return"),
            ([0, 1, 2], "return"),
            ({"key": "value"}, None),
        ],
    )
    def test_apply(self, dataframe_factory, value, expected):
        sdf = dataframe_factory()

        def _apply(value_: dict):
            assert value_ == value
            return expected

        sdf = sdf.apply(_apply)
        key, timestamp, headers = b"key", 0, []
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_apply_with_metadata(self, dataframe_factory):
        sdf = dataframe_factory()
        value = 1
        key, timestamp, headers = b"key", 0, []
        expected = (2, key, timestamp, headers)

        sdf = sdf.apply(
            lambda value_, _key, _timestamp, _headers: value + 1, metadata=True
        )

        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[0]
            == expected
        )

    @pytest.mark.parametrize(
        "value, mutation, expected",
        [
            ([0, 1, 2], lambda v: v.append(3), [0, 1, 2, 3]),
            ({"a": "b"}, lambda v: operator.setitem(v, "x", "y"), {"a": "b", "x": "y"}),
        ],
    )
    def test_update(self, dataframe_factory, value, mutation, expected):
        sdf = dataframe_factory()
        sdf = sdf.update(mutation)
        key, timestamp, headers = b"key", 0, []
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_update_with_metadata(self, dataframe_factory):
        sdf = dataframe_factory()
        value = [1]
        key, timestamp, headers = b"key", 0, []
        expected = ([1, 2], key, timestamp, headers)

        sdf = sdf.update(
            lambda value_, _key, _timestamp, _headers: value_.append(2), metadata=True
        )
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[0]
            == expected
        )

    def test_apply_multiple(self, dataframe_factory):
        sdf = dataframe_factory()
        value = 1
        expected = 4
        key, timestamp, headers = b"key", 0, []
        sdf = sdf.apply(lambda v: v + 1).apply(lambda v: v + 2)
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_apply_update_multiple(self, dataframe_factory):
        sdf = dataframe_factory()
        value = {"x": 1}
        expected = {"x": 3, "y": 3}
        key, timestamp, headers = b"key", 0, []
        sdf = (
            sdf.apply(lambda v: {"x": v["x"] + 1})
            .update(lambda v: operator.setitem(v, "y", 3))
            .apply(lambda v: {**v, "x": v["x"] + 1})
        )
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_setitem_primitive(self, dataframe_factory):
        value = {"x": 1}
        key, timestamp, headers = b"key", 0, []
        expected = {"x": 2}
        sdf = dataframe_factory()
        sdf["x"] = 2
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_setitem_series(self, dataframe_factory):
        value = {"x": 1, "y": 2}
        key, timestamp, headers = b"key", 0, []
        expected = {"x": 2, "y": 2}
        sdf = dataframe_factory()
        sdf["x"] = sdf["y"]

        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_setitem_series_apply(self, dataframe_factory):
        value = {"x": 1}
        key, timestamp, headers = b"key", 0, []
        expected = {"x": 1, "y": 2}
        sdf = dataframe_factory()
        sdf["y"] = sdf["x"].apply(lambda v: v + 1)
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_setitem_series_with_operations(self, dataframe_factory):
        value = {"x": 1, "y": 2}
        key, timestamp, headers = b"key", 0, []
        expected = {"x": 1, "y": 2, "z": 5}
        sdf = dataframe_factory()
        sdf["z"] = (sdf["x"] + sdf["y"]).apply(lambda v: v + 1) + 1
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_setitem_another_dataframe_apply(self, dataframe_factory):
        value = {"x": 1}
        key, timestamp, headers = b"key", 0, []
        expected = {"x": 1, "y": 2}
        sdf = dataframe_factory()
        sdf["y"] = sdf.apply(lambda v: v["x"] + 1)
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_column_subset(self, dataframe_factory):
        value = {"x": 1, "y": 2, "z": 3}
        key, timestamp, headers = b"key", 0, []
        expected = {"x": 1, "y": 2}
        sdf = dataframe_factory()
        sdf = sdf[["x", "y"]]
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_column_subset_and_apply(self, dataframe_factory):
        value = {"x": 1, "y": 2, "z": 3}
        key, timestamp, headers = b"key", 0, []
        expected = 2
        sdf = dataframe_factory()
        sdf = sdf[["x", "y"]]
        sdf = sdf.apply(lambda v: v["y"])
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            ({"x": 1, "y": 2}, b"key", 0, None, [({"x": 1, "y": 2}, b"key", 0, None)]),
            ({"x": 0, "y": 2}, b"key", 0, None, []),
        ],
    )
    def test_filter_with_series(
        self, dataframe_factory, value, key, timestamp, headers, expected
    ):
        sdf = dataframe_factory()
        sdf = sdf[sdf["x"] > 0]
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            ({"x": 1, "y": 2}, b"key", 0, None, [({"x": 1, "y": 2}, b"key", 0, None)]),
            ({"x": 0, "y": 2}, b"key", 0, None, []),
        ],
    )
    def test_filter_with_series_apply(
        self, dataframe_factory, value, key, timestamp, headers, expected
    ):
        sdf = dataframe_factory()
        sdf = sdf[sdf["x"].apply(lambda v: v > 0)]
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            ({"x": 1, "y": 2}, b"key", 0, None, [({"x": 1, "y": 2}, b"key", 0, None)]),
            ({"x": 0, "y": 2}, b"key", 0, None, []),
        ],
    )
    def test_filter_with_multiple_series(
        self, dataframe_factory, value, key, timestamp, headers, expected
    ):
        sdf = dataframe_factory()
        sdf = sdf[(sdf["x"] > 0) & (sdf["y"] > 0)]
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            ({"x": 1, "y": 2}, b"key", 0, None, [({"x": 1, "y": 2}, b"key", 0, None)]),
            ({"x": 0, "y": 2}, b"key", 0, None, []),
        ],
    )
    def test_filter_with_another_sdf_apply(
        self, dataframe_factory, value, key, timestamp, headers, expected
    ):
        sdf = dataframe_factory()
        sdf = sdf[sdf.apply(lambda v: v["x"] > 0)]
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    def test_filter_with_another_sdf_with_filters_fails(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf2 = sdf[sdf["x"] > 1].apply(lambda v: v["x"] > 0)
        with pytest.raises(ValueError, match="Filter functions are not allowed"):
            _ = sdf[sdf2]

    def test_filter_with_another_sdf_with_update_fails(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf2 = sdf.apply(lambda v: v).update(lambda v: operator.setitem(v, "x", 2))
        with pytest.raises(ValueError, match="Update functions are not allowed"):
            _ = sdf[sdf2]

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            ({"x": 1, "y": 2}, b"key", 0, None, [({"x": 1, "y": 2}, b"key", 0, None)]),
            ({"x": 0, "y": 2}, b"key", 0, None, []),
        ],
    )
    def test_filter_with_function(
        self, dataframe_factory, value, key, timestamp, headers, expected
    ):
        sdf = dataframe_factory()
        sdf = sdf.filter(lambda v: v["x"] > 0)
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            ({"x": 1, "y": 2}, b"key", 0, None, [({"x": 1, "y": 2}, b"key", 0, None)]),
            ({"x": 0, "y": 2}, b"key", 0, None, []),
        ],
    )
    def test_filter_with_metadata(
        self, dataframe_factory, value, key, timestamp, headers, expected
    ):
        sdf = dataframe_factory()
        sdf = sdf.filter(
            lambda value_, _key, _timestamp, _headers: value_["x"] > 0, metadata=True
        )
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    def test_contains_on_existing_column(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf["has_column"] = sdf.contains("x")
        value = {"x": 1}
        expected = {"x": 1, "has_column": True}
        key, timestamp, headers = b"key", 0, []
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_contains_on_missing_column(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf["has_column"] = sdf.contains("wrong_column")
        value = {"x": 1}
        expected = {"x": 1, "has_column": False}
        key, timestamp, headers = b"key", 0, []
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    def test_contains_as_filter(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf = sdf[sdf.contains("x")]

        valid_value = {"x": 1, "y": 2}
        key, timestamp, headers = b"key", 0, []
        assert sdf.test(
            value=valid_value, key=key, timestamp=timestamp, headers=headers
        ) == [(valid_value, key, timestamp, headers)]
        invalid_value = {"y": 2}
        assert (
            sdf.test(value=invalid_value, key=key, timestamp=timestamp, headers=headers)
            == []
        )

    def test_cannot_use_logical_and(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(InvalidOperation):
            sdf["truth"] = sdf[sdf.apply(lambda x: x["a"] > 0)] and sdf[["b"]]

    def test_cannot_use_logical_or(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(InvalidOperation):
            sdf["truth"] = sdf[sdf.apply(lambda x: x["a"] > 0)] or sdf[["b"]]

    def test_set_timestamp(self, dataframe_factory):
        value, key, timestamp, headers = 1, "key", 0, None
        expected = (1, "key", 100, headers)
        sdf = dataframe_factory()

        sdf = sdf.set_timestamp(
            lambda value_, key_, timestamp_, headers_: timestamp_ + 100
        )

        result = sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[0]
        assert result == expected

    @pytest.mark.parametrize(
        "original_headers, new_headers",
        [
            (None, None),
            ([], []),
            ([], [("key", b"value")]),
            ([[("key", b"value")]], [("key2", b"value2")]),
        ],
    )
    def test_set_headers(self, original_headers, new_headers, dataframe_factory):
        value, key, timestamp = 1, "key", 0
        expected = (1, "key", 0, new_headers)
        sdf = dataframe_factory()

        sdf = sdf.set_headers(lambda value_, key_, timestamp_, headers_: new_headers)

        result = sdf.test(
            value=value, key=key, timestamp=timestamp, headers=original_headers
        )[0]
        assert result == expected

    @pytest.mark.parametrize(
        "metadata,expected",
        [
            (False, str({"value": {"x": 1}})),
            (
                True,
                str({"value": {"x": 1}, "key": b"key", "timestamp": 0, "headers": []}),
            ),
        ],
    )
    def test_print(self, dataframe_factory, metadata, expected, get_output):
        sdf = dataframe_factory()
        sdf.print(metadata=metadata)

        value = {"x": 1}
        key, timestamp, headers = b"key", 0, []
        sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)

        assert expected in get_output()

    def test_print_table(self, dataframe_factory, get_output):
        sdf = dataframe_factory()
        sdf.print_table(
            size=1, title="test", metadata=True, live_slowdown=0.0, columns=["x"]
        )
        sdf.test(value={"x": 1, "y": 2}, key=b"key", timestamp=12345, headers=[])
        sdf._processing_context.printer.print()
        assert get_output() == (
            "test                       \n"
            "┏━━━━━━━━┳━━━━━━━━━━━━┳━━━┓\n"
            "┃ _key   ┃ _timestamp ┃ x ┃\n"
            "┡━━━━━━━━╇━━━━━━━━━━━━╇━━━┩\n"
            "│ b'key' │ 12345      │ 1 │\n"
            "└────────┴────────────┴───┘\n"
        )

    def test_print_table_empty_warning(self, dataframe_factory):
        sdf = dataframe_factory()
        with mock.patch.object(warnings, "warn") as warn:
            sdf.print_table(columns=[], metadata=False)
            warn.assert_called_once()

    def test_print_table_non_dict_value(self, dataframe_factory, get_output):
        sdf = dataframe_factory()
        sdf.print_table(size=1, metadata=False)
        sdf.test(value=1, key=b"key", timestamp=12345)
        sdf._processing_context.printer.print()
        assert get_output() == (
            "┏━━━┓\n"
            "┃ 0 ┃\n"
            "┡━━━┩\n"
            "│ 1 │\n"
            "└───┘\n"
        )  # fmt: skip

    @pytest.mark.parametrize(
        ["columns", "mapping", "value", "expected"],
        [
            (["x"], {}, 1, 1),  # value is not a dict, fill ignored
            (["x"], {}, {"x": 1}, {"x": 1}),
            (["x"], {}, {}, {"x": None}),
            (["x"], {}, 1, 1),
            (["x", "y"], {}, {"x": 1, "y": 2}, {"x": 1, "y": 2}),
            (["x", "y"], {}, {"x": 1}, {"x": 1, "y": None}),
            (["x", "y"], {}, {}, {"x": None, "y": None}),
            ([], {"x": 1, "y": 2}, {"x": 3, "y": 4}, {"x": 3, "y": 4}),
            ([], {"x": 1, "y": 2}, {"x": 3}, {"x": 3, "y": 2}),
            ([], {"x": 1, "y": 2}, {}, {"x": 1, "y": 2}),
            ([], {"x": 1, "y": 2}, {"x": None}, {"x": 1, "y": 2}),
            ([], {"x": 1, "y": None}, {"x": 2}, {"x": 2, "y": None}),
            (["x"], {"y": 2}, {"y": None}, {"x": None, "y": 2}),
            # overlapping column names, kwargs take precedence
            (["x"], {"x": 2}, {"x": None}, {"x": 2}),
        ],
    )
    def test_fill(self, dataframe_factory, columns, mapping, value, expected):
        sdf = dataframe_factory()
        sdf.fill(*columns, **mapping)
        assert sdf.test(value=value)[0][0] == expected

    def test_fill_no_columns_or_mapping(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(
            ValueError, match="No columns or mapping provided to fill()."
        ):
            sdf.fill()

    @pytest.mark.parametrize(
        "columns, expected",
        [
            ("col_a", {"col_b": 2, "col_c": 3}),
            (["col_a"], {"col_b": 2, "col_c": 3}),
            (["col_a", "col_b"], {"col_c": 3}),
        ],
    )
    def test_drop(self, dataframe_factory, columns, expected):
        value = {"col_a": 1, "col_b": 2, "col_c": 3}
        key, timestamp, headers = b"key", 0, []
        sdf = dataframe_factory()
        sdf.drop(columns)
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)

    @pytest.mark.parametrize("columns", [["col_a", 3], b"col_d", {"col_a"}])
    def test_drop_invalid_columns(self, dataframe_factory, columns):
        sdf = dataframe_factory()
        with pytest.raises(TypeError):
            sdf.drop(columns)

    def test_drop_empty_list(self, dataframe_factory):
        """
        Dropping an empty list is ignored entirely.
        """
        sdf = dataframe_factory()
        pre_drop_stream = sdf.stream.full_tree()
        sdf = sdf.drop([])
        post_drop_stream = sdf.stream.full_tree()
        assert pre_drop_stream == post_drop_stream

    def test_drop_missing_columns_errors_raise(self, dataframe_factory):
        value = {"col_a": 1}
        key, timestamp, headers = b"key", 0, []
        sdf = dataframe_factory()
        sdf.drop(["col_b", "col_c"], errors="raise")
        with pytest.raises(KeyError):
            assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)

    def test_drop_missing_columns_errors_ignore(self, dataframe_factory):
        value = {"col_a": 1}
        expected = {"col_a": 1}
        key, timestamp, headers = b"key", 0, []
        sdf = dataframe_factory()
        sdf.drop(["col_b", "col_c"], errors="ignore")
        assert sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[
            0
        ] == (expected, key, timestamp, headers)


class TestStreamingDataFrameApplyExpand:
    def test_apply_expand(self, dataframe_factory):
        sdf = dataframe_factory()
        value = 1
        key, timestamp, headers = b"key", 0, []
        expected = [(1, key, timestamp, headers), (2, key, timestamp, headers)]
        sdf = sdf.apply(lambda v: [v, v + 1], expand=True)
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    def test_apply_expand_filter(self, dataframe_factory):
        value = 1
        key, timestamp, headers = b"key", 0, []
        expected = [(1, key, timestamp, headers)]
        sdf = dataframe_factory()
        sdf = sdf.apply(lambda v: [v, v + 1], expand=True)
        sdf = sdf[sdf.apply(lambda v: v != 2)]
        result = sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
        assert result == expected

    def test_apply_expand_update(self, dataframe_factory):
        value = {"x": 1}
        key, timestamp, headers = b"key", 0, []
        expected = [
            ({"x": 2}, key, timestamp, headers),
            ({"x": 2}, key, timestamp, headers),
        ]
        sdf = dataframe_factory()
        sdf = sdf.apply(lambda v: [v, v], expand=True)
        sdf["x"] = 2
        assert (
            sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
            == expected
        )

    def test_apply_expand_as_filter_not_allowed(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(ValueError, match="Expand functions are not allowed"):
            sdf["x"] = sdf.apply(lambda v: [v, v], expand=True)

    def test_setitem_expand_not_allowed(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(ValueError, match="Expand functions are not allowed"):
            _ = sdf[sdf.apply(lambda v: [v, v], expand=True)]


class TestStreamingDataFrameToTopic:
    @pytest.mark.parametrize("reassign", [True, False])
    def test_to_topic(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
        message_context_factory,
        reassign,
    ):
        topic = topic_manager_topic_factory(
            key_serializer="str",
            key_deserializer="str",
            value_serializer="json",
            value_deserializer="json",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory(producer=producer)
        if reassign:
            sdf = sdf.to_topic(topic)
        else:
            sdf.to_topic(topic)

        value = {"x": 1, "y": 2}
        key, timestamp = "key", 10
        ctx = message_context_factory()

        with producer:
            sdf.test(value=value, key=key, timestamp=timestamp, ctx=ctx)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == topic.name
        assert consumed_row.key == key
        assert consumed_row.value == value
        assert consumed_row.timestamp == timestamp

    def test_to_topic_apply_expand(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic = topic_manager_topic_factory(
            key_serializer="str",
            key_deserializer="str",
            value_serializer="json",
            value_deserializer="json",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory(producer=producer)

        sdf = sdf.apply(lambda v: [v, v], expand=True).to_topic(topic)

        value = {"x": 1, "y": 2}
        key, timestamp = "key", 10
        ctx = message_context_factory()

        with producer:
            sdf.test(value=value, key=key, timestamp=timestamp, ctx=ctx)

        consumed = []
        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            for _ in range(2):
                row = consumer.poll_row(timeout=5.0)
                consumed.append(row)

        assert len(consumed) == 2
        for row in consumed:
            assert row.topic == topic.name
            assert row.key == key
            assert row.value == value
            assert row.timestamp == timestamp

    @pytest.mark.parametrize(
        "value, key_func, expected_key",
        [
            ({"x": 1}, lambda value: value["x"], 1),
            ({"x": 0}, lambda value: value["x"], 0),
        ],
    )
    def test_to_topic_custom_key(
        self,
        value,
        key_func,
        expected_key,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic = topic_manager_topic_factory(
            value_serializer="json",
            value_deserializer="json",
            key_serializer="int",
            key_deserializer="int",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory(producer=producer)

        # Use value["x"] as a new key
        sdf = sdf.to_topic(topic, key=key_func)
        key, timestamp = 10, 10
        ctx = message_context_factory()

        with producer:
            sdf.test(value=value, key=key, timestamp=timestamp, ctx=ctx)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == topic.name
        assert consumed_row.value == value
        assert consumed_row.key == expected_key

    def test_to_topic_multiple_topics_out(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic_0 = topic_manager_topic_factory(
            value_serializer="json", value_deserializer="json"
        )
        topic_1 = topic_manager_topic_factory(
            value_serializer="json", value_deserializer="json"
        )
        producer = row_producer_factory()

        sdf = dataframe_factory(producer=producer)
        sdf = sdf.to_topic(topic_0).to_topic(topic_1)

        value = {"x": 1, "y": 2}
        key, timestamp = b"key", 10
        ctx = message_context_factory()

        with producer:
            sdf.test(value=value, key=key, timestamp=timestamp, ctx=ctx)

        consumed_rows = []
        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic_0, topic_1])
            while len(consumed_rows) < 2:
                consumed_rows.append(consumer.poll_row(timeout=5.0))

        assert len(consumed_rows) == 2
        assert {row.topic for row in consumed_rows} == {
            t.name for t in [topic_0, topic_1]
        }
        for consumed_row in consumed_rows:
            assert consumed_row.key == key
            assert consumed_row.value == value


class TestStreamingDataframeStateful:
    def test_apply_stateful(
        self,
        dataframe_factory,
        state_manager,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic = topic_manager_topic_factory()

        def stateful_func(value_: dict, state: State) -> int:
            current_max = state.get("max")
            if current_max is None:
                current_max = value_["number"]
            else:
                current_max = max(current_max, value_["number"])
            state.set("max", current_max)
            return current_max

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.apply(stateful_func, stateful=True)

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        values = [
            {"number": 1},
            {"number": 10},
            {"number": 3},
        ]
        key, timestamp, headers = b"test", 0, []
        result = None
        ctx = message_context_factory(topic=topic.name)
        for value in values:
            result = sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )[0]

        assert result == (10, key, timestamp, headers)

    def test_update_stateful(
        self,
        dataframe_factory,
        state_manager,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic = topic_manager_topic_factory()

        def stateful_func(value_: dict, state: State):
            current_max = state.get("max")
            if current_max is None:
                current_max = value_["number"]
            else:
                current_max = max(current_max, value_["number"])
            state.set("max", current_max)
            value_["max"] = current_max

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.update(stateful_func, stateful=True)

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        result = None
        values = [
            {"number": 1},
            {"number": 10},
            {"number": 3},
        ]
        key, timestamp, headers = b"test", 0, None
        ctx = message_context_factory(topic=topic.name)
        for value in values:
            result = sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )[0]

        assert result == ({"number": 3, "max": 10}, key, timestamp, headers)

    def test_filter_stateful(
        self,
        dataframe_factory,
        state_manager,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic = topic_manager_topic_factory()

        def stateful_func(value_: dict, state: State):
            current_max = state.get("max")
            if current_max is None:
                current_max = value_["number"]
            else:
                current_max = max(current_max, value_["number"])
            state.set("max", current_max)
            value_["max"] = current_max

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.update(stateful_func, stateful=True)
        sdf = sdf.filter(lambda v, state: state.get("max") >= 3, stateful=True)

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        values = [
            {"number": 1},
            {"number": 1},
            {"number": 3},
        ]
        key, timestamp, headers = b"test", 0, None
        ctx = message_context_factory(topic=topic.name)
        results = []
        for value in values:
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 1
        assert results[0] == ({"number": 3, "max": 3}, key, timestamp, headers)

    def test_filter_with_another_sdf_apply_stateful(
        self,
        dataframe_factory,
        state_manager,
        topic_manager_topic_factory,
        message_context_factory,
    ):
        topic = topic_manager_topic_factory()

        def stateful_func(value_: dict, state: State):
            current_max = state.get("max")
            if current_max is None:
                current_max = value_["number"]
            else:
                current_max = max(current_max, value_["number"])
            state.set("max", current_max)
            value_["max"] = current_max

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.update(stateful_func, stateful=True)
        sdf = sdf[sdf.apply(lambda v, state: state.get("max") >= 3, stateful=True)]

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        values = [
            {"number": 1},
            {"number": 1},
            {"number": 3},
        ]
        key, timestamp, headers = b"test", 0, None
        ctx = message_context_factory(topic=topic.name)
        results = []
        for value in values:
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )
        assert len(results) == 1
        assert results[0] == ({"max": 3, "number": 3}, key, timestamp, headers)


class TestStreamingDataFrameTumblingWindow:
    def test_tumbling_window_define_from_milliseconds(
        self, dataframe_factory, state_manager
    ):
        sdf = dataframe_factory(state_manager=state_manager)
        window_definition = sdf.tumbling_window(duration_ms=2000, grace_ms=1000)
        assert window_definition.duration_ms == 2000
        assert window_definition.grace_ms == 1000

    @pytest.mark.parametrize(
        "duration_delta, grace_delta, duration_ms, grace_ms",
        [
            (timedelta(seconds=10), timedelta(seconds=0), 10_000, 0),
            (timedelta(seconds=10), timedelta(seconds=1), 10_000, 1000),
            (timedelta(milliseconds=10.1), timedelta(milliseconds=1.1), 10, 1),
            (timedelta(milliseconds=10.9), timedelta(milliseconds=1.9), 11, 2),
        ],
    )
    def test_tumbling_window_define_from_timedelta(
        self,
        duration_delta,
        grace_delta,
        duration_ms,
        grace_ms,
        dataframe_factory,
        state_manager,
    ):
        sdf = dataframe_factory(state_manager=state_manager)
        window_definition = sdf.tumbling_window(
            duration_ms=duration_delta, grace_ms=grace_delta
        )
        assert window_definition.duration_ms == duration_ms
        assert window_definition.grace_ms == grace_ms

    def test_tumbling_window_current(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = (
            sdf.tumbling_window(
                duration_ms=timedelta(seconds=10), grace_ms=timedelta(seconds=1)
            )
            .sum()
            .current()
        )

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )

        records = [
            # Message early in the window
            RecordStub(1, "key", 1000),
            # Message towards the end of the window
            RecordStub(2, "key", 9000),
            # Should start a new window
            RecordStub(3, "key", 20010),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )
        assert len(results) == 3
        assert results == [
            (WindowResult(value=1, start=0, end=10000), records[0].key, 0, None),
            (WindowResult(value=3, start=0, end=10000), records[1].key, 0, None),
            (
                WindowResult(value=3, start=20000, end=30000),
                records[2].key,
                20000,
                None,
            ),
        ]

    @pytest.mark.parametrize("should_log", [True, False])
    def test_tumbling_window_current_out_of_order_late(
        self,
        should_log,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
        caplog,
    ):
        """
        Test that window with "latest" doesn't output the result if incoming timestamp
        is late
        """
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)

        on_late_called = False

        def on_late(
            value: Any,
            key: Any,
            timestamp_ms: int,
            late_by_ms: int,
            start: int,
            end: int,
            store_name: str,
            topic: str,
            partition: int,
            offset: int,
        ) -> bool:
            nonlocal on_late_called
            on_late_called = True
            return should_log

        sdf = (
            sdf.tumbling_window(duration_ms=10, grace_ms=0, on_late=on_late)
            .sum()
            .current()
        )

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0, 10)
            RecordStub(1, "test", 1),
            # Create window [20,30)
            RecordStub(2, "test", 20),
            # Late message - it belongs to window [0,10) but this window
            # is already closed. This message should be skipped from processing
            RecordStub(3, "test", 19),
        ]
        headers = [("key", b"value")]

        results = []
        with caplog.at_level(logging.WARNING, logger="quixstreams"):
            for value, key, timestamp in records:
                ctx = message_context_factory(topic=topic.name)
                result = sdf.test(
                    value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
                )
                results += result

        assert on_late_called
        warning_logs = [
            r
            for r in caplog.records
            if r.levelname == "WARNING"
            and "Skipping window processing for the closed window" in r.message
        ]

        assert warning_logs if should_log else not warning_logs

        assert len(results) == 2
        assert results == [
            (WindowResult(value=1, start=0, end=10), records[0].key, 0, None),
            (WindowResult(value=2, start=20, end=30), records[1].key, 20, None),
        ]

    def test_tumbling_window_final(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.tumbling_window(duration_ms=10, grace_ms=0).sum().final()

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0, 10)
            RecordStub(1, "test", 1),
            # Update window [0, 10)
            RecordStub(1, "test", 2),
            # Create window [20,30). Window [0, 10) is expired now.
            RecordStub(2, "test", 20),
            # Create window [30, 40). Window [20, 30) is expired now.
            RecordStub(3, "test", 39),
            # Update window [30, 40). Nothing should be returned.
            RecordStub(4, "test", 38),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 2
        assert results == [
            (WindowResult(value=2, start=0, end=10), records[2].key, 0, None),
            (WindowResult(value=2, start=20, end=30), records[3].key, 20, None),
        ]

    def test_tumbling_window_final_invalid_strategy(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)

        with pytest.raises(TypeError):
            sdf = (
                sdf.tumbling_window(duration_ms=10, grace_ms=0)
                .sum()
                .final(closing_strategy="foo")
            )

    def test_tumbling_window_none_key_messages(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.tumbling_window(duration_ms=10).sum().current()

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0,10)
            RecordStub(1, "test", 1),
            # Message with None key, expected to be ignored
            RecordStub(10, None, 100),
            # Update window [0,10)
            RecordStub(2, "test", 2),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 2
        # Ensure that the windows are returned with correct values and order
        assert results == [
            (WindowResult(value=1, start=0, end=10), records[0].key, 0, None),
            (WindowResult(value=3, start=0, end=10), records[2].key, 0, None),
        ]

    def test_tumbling_window_two_windows(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        """
        Test that tumbling windows work correctly when executed one after the other
        """
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = (
            sdf.tumbling_window(duration_ms=timedelta(seconds=10))
            .sum()
            .current()
            .apply(lambda value_: value_["value"])
            .tumbling_window(duration_ms=timedelta(seconds=30))
            .sum()
            .current()
        )

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )

        records = [
            # Message early in the window
            RecordStub(1, "key", 1000),
            # Message towards the end of the window
            RecordStub(2, "key", 9000),
            # Should start a new window
            RecordStub(3, "key", 90010),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )
        assert len(results) == 3
        assert results == [
            (WindowResult(value=1, start=0, end=30000), records[0].key, 0, None),
            (WindowResult(value=4, start=0, end=30000), records[1].key, 0, None),
            (
                WindowResult(value=3, start=90000, end=120000),
                records[2].key,
                90000,
                None,
            ),
        ]


class TestStreamingDataFrameHoppingWindow:
    def test_hopping_window_define_from_milliseconds(
        self, dataframe_factory, state_manager
    ):
        sdf = dataframe_factory(state_manager=state_manager)
        window_definition = sdf.hopping_window(
            duration_ms=2000, grace_ms=1000, step_ms=1000
        )
        assert window_definition.duration_ms == 2000
        assert window_definition.grace_ms == 1000
        assert window_definition.step_ms == 1000

    @pytest.mark.parametrize(
        "duration_delta, step_delta, grace_delta, duration_ms, step_ms, grace_ms",
        [
            (
                timedelta(seconds=10),
                timedelta(seconds=1),
                timedelta(seconds=0),
                10_000,
                1000,
                0,
            ),
            (
                timedelta(seconds=10),
                timedelta(seconds=1),
                timedelta(seconds=1),
                10_000,
                1000,
                1000,
            ),
            (
                timedelta(milliseconds=10.1),
                timedelta(milliseconds=1.1),
                timedelta(milliseconds=1.1),
                10,
                1,
                1,
            ),
            (
                timedelta(milliseconds=10.9),
                timedelta(milliseconds=1.9),
                timedelta(milliseconds=1.9),
                11,
                2,
                2,
            ),
        ],
    )
    def test_hopping_window_define_from_timedelta(
        self,
        duration_delta,
        step_delta,
        grace_delta,
        duration_ms,
        step_ms,
        grace_ms,
        dataframe_factory,
        state_manager,
    ):
        sdf = dataframe_factory(state_manager=state_manager)
        window_definition = sdf.hopping_window(
            duration_ms=duration_delta, grace_ms=grace_delta, step_ms=step_delta
        )
        assert window_definition.duration_ms == duration_ms
        assert window_definition.grace_ms == grace_ms
        assert window_definition.step_ms == step_ms

    def test_hopping_window_current(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.hopping_window(duration_ms=10, step_ms=5).sum().current()

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0,10)
            RecordStub(1, "test", 1),
            # Update window [0,10) and create window [5,15)
            RecordStub(2, "test", 7),
            # Update window [5,15) and create window [10,20)
            RecordStub(3, "test", 10),
            # Create windows [30, 40) and [35, 45)
            RecordStub(4, "test", 35),
            # Update windows [30, 40) and [35, 45)
            RecordStub(5, "test", 35),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 9

        # Ensure that the windows are returned with correct values and order
        assert results == [
            (WindowResult(value=1, start=0, end=10), records[0].key, 0, None),
            (WindowResult(value=3, start=0, end=10), records[1].key, 0, None),
            (WindowResult(value=2, start=5, end=15), records[1].key, 5, None),
            (WindowResult(value=5, start=5, end=15), records[2].key, 5, None),
            (WindowResult(value=3, start=10, end=20), records[2].key, 10, None),
            (WindowResult(value=4, start=30, end=40), records[3].key, 30, None),
            (WindowResult(value=4, start=35, end=45), records[3].key, 35, None),
            (WindowResult(value=9, start=30, end=40), records[4].key, 30, None),
            (WindowResult(value=9, start=35, end=45), records[4].key, 35, None),
        ]

    def test_hopping_window_current_out_of_order_late(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.hopping_window(duration_ms=10, step_ms=5).sum().current()

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0,10)
            RecordStub(1, "test", 1),
            # Update window [0,10) and create window [5,15)
            RecordStub(2, "test", 7),
            # Create windows [30, 40) and [35, 45)
            RecordStub(4, "test", 35),
            # Timestamp "10" is late and should not be processed
            RecordStub(3, "test", 26),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 5
        # Ensure that the windows are returned with correct values and order
        assert results == [
            (WindowResult(value=1, start=0, end=10), records[0].key, 0, None),
            (WindowResult(value=3, start=0, end=10), records[1].key, 0, None),
            (WindowResult(value=2, start=5, end=15), records[1].key, 5, None),
            (WindowResult(value=4, start=30, end=40), records[2].key, 30, None),
            (WindowResult(value=4, start=35, end=45), records[2].key, 35, None),
        ]

    def test_hopping_window_final(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.hopping_window(duration_ms=10, step_ms=5).sum().final()

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )

        records = [
            # Create window [0,10)
            RecordStub(1, "test", 1),
            # Update window [0,10) and create window [5,15)
            RecordStub(2, "test", 7),
            # Update window [5,15) and create window [10,20)
            RecordStub(3, "test", 10),
            # Create windows [30, 40) and [35, 45).
            # Windows [0,10), [5,15) and [10,20) should be expired
            RecordStub(4, "test", 35),
            # Update windows [30, 40) and [35, 45)
            RecordStub(5, "test", 35),
        ]
        headers = [("key", b"value")]

        results = []

        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 3
        # Ensure that the windows are returned with correct values and order
        assert results == [
            (WindowResult(value=3, start=0, end=10), records[2].key, 0, None),
            (WindowResult(value=5, start=5, end=15), records[3].key, 5, None),
            (WindowResult(value=3, start=10, end=20), records[3].key, 10, None),
        ]

    def test_hopping_window_final_invalid_strategy(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)

        with pytest.raises(TypeError):
            sdf = (
                sdf.hopping_window(duration_ms=10, step_ms=5)
                .sum()
                .final(closing_strategy="foo")
            )

    def test_hopping_window_none_key_messages(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.hopping_window(duration_ms=10, step_ms=5).sum().current()

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0,10)
            RecordStub(1, "test", 1),
            # Message with None key, expected to be ignored
            RecordStub(10, None, 100),
            # Update window [0,10)
            RecordStub(2, "test", 2),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )

        assert len(results) == 2
        # Ensure that the windows are returned with correct values and order
        assert results == [
            (WindowResult(value=1, start=0, end=10), records[0].key, 0, None),
            (WindowResult(value=3, start=0, end=10), records[2].key, 0, None),
        ]


class TestStreamingDataFrameSlidingWindow:
    def test_sliding_window_current(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(name="test")

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = (
            sdf.sliding_window(
                duration_ms=timedelta(seconds=10), grace_ms=timedelta(seconds=1)
            )
            .sum()
            .current()
        )

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )

        records = [
            RecordStub(1, "key", 1000),
            RecordStub(2, "key", 9000),
            RecordStub(3, "key", 20010),
        ]
        headers = [("key", b"value")]

        results = []
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            results += sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )
        assert len(results) == 3
        assert results == [
            (WindowResult(value=1, start=0, end=1000), records[0].key, 0, None),
            (WindowResult(value=3, start=0, end=9000), records[1].key, 0, None),
            (
                WindowResult(value=3, start=10010, end=20010),
                records[2].key,
                10010,
                None,
            ),
        ]

    @pytest.mark.parametrize("should_log", [True, False])
    def test_sliding_window_out_of_order_late(
        self,
        should_log,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
        caplog,
    ):
        """
        Test that late timestamps trigger the `on_late` callback and log the warning
        if the callback returns True.
        """
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)

        on_late_called = False

        def on_late(
            value: Any,
            key: Any,
            timestamp_ms: int,
            late_by_ms: int,
            start: int,
            end: int,
            store_name: str,
            topic: str,
            partition: int,
            offset: int,
        ) -> bool:
            nonlocal on_late_called
            on_late_called = True
            return should_log

        sdf = (
            sdf.sliding_window(duration_ms=10, grace_ms=0, on_late=on_late)
            .sum()
            .current()
        )

        state_manager.on_partition_assign(
            stream_id=topic.name, partition=0, committed_offsets={topic.name: -1001}
        )
        records = [
            # Create window [0, 1]
            RecordStub(1, "test", 1),
            # Create window [10,20]
            RecordStub(2, "test", 20),
            # Late message - it belongs to window [0,5] but this window
            # is already closed. This message should be skipped from processing
            RecordStub(3, "test", 5),
        ]
        headers = [("key", b"value")]

        results = []
        with caplog.at_level(logging.WARNING, logger="quixstreams"):
            for value, key, timestamp in records:
                ctx = message_context_factory(topic=topic.name)
                result = sdf.test(
                    value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
                )
                results += result

        assert on_late_called
        warning_logs = [
            r
            for r in caplog.records
            if r.levelname == "WARNING"
            and "Skipping window processing for the closed window" in r.message
        ]

        assert warning_logs if should_log else not warning_logs

        assert len(results) == 2
        assert results == [
            (WindowResult(value=1, start=0, end=1), records[0].key, 0, None),
            (WindowResult(value=2, start=10, end=20), records[1].key, 10, None),
        ]


class TestStreamingDataFrameGroupBy:
    def test_group_by_column(
        self,
        dataframe_factory,
        topic_manager_factory,
        row_producer_factory,
        row_consumer_factory,
        message_context_factory,
    ):
        """GroupBy can accept a string (column name) as its grouping method."""
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        producer = row_producer_factory()

        col = "column_A"
        orig_key = "original_key"
        orig_timestamp_ms = 1000
        new_key = "woo"
        value = {col: new_key}
        col_update = "updated_col"
        headers = [("key", b"value")]

        sdf_registry = DataFrameRegistry()
        sdf = dataframe_factory(
            topic, topic_manager=topic_manager, producer=producer, registry=sdf_registry
        )
        sdf = sdf.group_by(col)
        sdf[col] = col_update

        groupby_topic = sdf.topics[0]
        assert sdf_registry.consumer_topics == [topic, groupby_topic]
        assert groupby_topic.name.startswith("repartition__")

        with producer:
            pre_groupby_branch_result = sdf.test(
                value=value,
                topic=topic,
                key=orig_key,
                timestamp=orig_timestamp_ms,
                headers=headers,
                ctx=message_context_factory(topic=topic.name),
            )

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([groupby_topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == groupby_topic.name
        assert consumed_row.key == new_key
        assert consumed_row.timestamp == orig_timestamp_ms
        assert consumed_row.value == value
        assert consumed_row.headers == headers
        assert pre_groupby_branch_result[0] == (
            value,
            orig_key,
            orig_timestamp_ms,
            headers,
        )

        # Check that the value is updated after record passed the groupby
        post_groupby_branch_result = sdf.test(
            value=value,
            key=new_key,
            timestamp=orig_timestamp_ms,
            headers=headers,
            ctx=message_context_factory(topic=groupby_topic.name),
        )
        assert post_groupby_branch_result[0] == (
            {col: col_update},
            new_key,
            orig_timestamp_ms,
            headers,
        )

    def test_group_by_column_with_name(
        self,
        dataframe_factory,
        topic_manager_factory,
        row_producer_factory,
        row_consumer_factory,
        message_context_factory,
    ):
        """
        GroupBy can accept a string (column name) as its grouping method and use
        a custom name for it (instead of the column name)
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        producer = row_producer_factory()

        col = "column_A"
        op_name = "get_col_A"
        orig_key = "original_key"
        orig_timestamp_ms = 1000
        new_key = "woo"
        value = {col: new_key}
        col_update = "updated_col"
        headers = [("key", b"value")]

        sdf_registry = DataFrameRegistry()
        sdf = dataframe_factory(
            topic, topic_manager=topic_manager, producer=producer, registry=sdf_registry
        )
        sdf = sdf.group_by(col, name=op_name)
        sdf[col] = col_update

        groupby_topic = sdf.topics[0]
        assert sdf_registry.consumer_topics == [topic, groupby_topic]
        assert groupby_topic.name.startswith("repartition__")

        with producer:
            pre_groupby_branch_result = sdf.test(
                value=value,
                topic=topic,
                key=orig_key,
                timestamp=orig_timestamp_ms,
                headers=headers,
                ctx=message_context_factory(topic=topic.name),
            )

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([groupby_topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == groupby_topic.name
        assert consumed_row.key == new_key
        assert consumed_row.timestamp == orig_timestamp_ms
        assert consumed_row.value == value
        assert consumed_row.headers == headers
        assert pre_groupby_branch_result[0] == (
            value,
            orig_key,
            orig_timestamp_ms,
            headers,
        )

        # Check that the value is updated after record passed the groupby
        post_groupby_branch_result = sdf.test(
            value=value,
            key=new_key,
            timestamp=orig_timestamp_ms,
            headers=headers,
            ctx=message_context_factory(topic=groupby_topic.name),
        )
        assert post_groupby_branch_result[0] == (
            {col: col_update},
            new_key,
            orig_timestamp_ms,
            headers,
        )

    def test_group_by_func(
        self,
        dataframe_factory,
        topic_manager_factory,
        row_producer_factory,
        row_consumer_factory,
        message_context_factory,
    ):
        """
        GroupBy can accept a Callable as its grouping method (requires a name too).
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        producer = row_producer_factory()

        col = "column_A"
        op_name = "get_col_A"
        orig_key = "original_key"
        orig_timestamp_ms = 1000

        new_key = "woo"
        value = {col: new_key}
        col_update = "updated_col"
        headers = [("key", b"value")]

        sdf_registry = DataFrameRegistry()
        sdf = dataframe_factory(
            topic, topic_manager=topic_manager, producer=producer, registry=sdf_registry
        )
        sdf = sdf.group_by(lambda v: v[col], name=op_name)
        sdf[col] = col_update

        groupby_topic = sdf.topics[0]
        assert sdf_registry.consumer_topics == [topic, groupby_topic]

        with producer:
            pre_groupby_branch_result = sdf.test(
                value=value,
                topic=topic,
                key=orig_key,
                timestamp=orig_timestamp_ms,
                headers=headers,
                ctx=message_context_factory(topic=topic.name),
            )

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([groupby_topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == groupby_topic.name
        assert consumed_row.key == new_key
        assert consumed_row.timestamp == orig_timestamp_ms
        assert consumed_row.value == value
        assert consumed_row.headers == headers
        assert pre_groupby_branch_result[0] == (
            value,
            orig_key,
            orig_timestamp_ms,
            headers,
        )

        # Check that the value is updated after record passed the groupby
        post_groupby_branch_result = sdf.test(
            value=value,
            key=new_key,
            timestamp=orig_timestamp_ms,
            headers=headers,
            ctx=message_context_factory(topic=groupby_topic.name),
        )
        assert post_groupby_branch_result[0] == (
            {col: col_update},
            new_key,
            orig_timestamp_ms,
            headers,
        )

    def test_group_by_func_name_missing(self, dataframe_factory, topic_manager_factory):
        """Using a Callable for groupby requires giving a name"""
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)

        with pytest.raises(ValueError):
            sdf.group_by(lambda v: "do_stuff")

    def test_group_by_key_empty_fails(self, dataframe_factory, topic_manager_factory):
        """Using a Callable for groupby requires giving a name"""
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)

        with pytest.raises(ValueError, match='Parameter "key" cannot be empty'):
            sdf.group_by(key="")

    def test_group_by_invalid_key_func(self, dataframe_factory, topic_manager_factory):
        """GroupBy can only use a string (column name) or Callable to group with"""
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)

        with pytest.raises(ValueError):
            sdf.group_by({"um": "what is this"})

    def test_group_by_limit_exceeded(self, dataframe_factory, topic_manager_factory):
        """
        Only 1 GroupBy depth per SDF (no nesting of them).
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)
        sdf = sdf.group_by("col_a")

        with pytest.raises(GroupByNestingLimit):
            sdf.group_by("col_b")

    def test_group_by_name_clash(self, dataframe_factory, topic_manager_factory):
        """
        Each groupby operation per SDF instance (or, what appears to end users
        as a "single" SDF instance) should be uniquely named.

        Most likely to encounter this if group by is used with the same column name.
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)
        sdf.group_by("col_a")

        with pytest.raises(GroupByDuplicate):
            sdf.group_by("col_a")

    def test_sink_cannot_be_added_to(self, dataframe_factory, topic_manager_factory):
        """
        A sink cannot be added to or branched.
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)
        assert len(sdf.stream.children) == 0
        sdf.sink(DummySink())
        # sink operation was added
        assert len(sdf.stream.children) == 1
        sdf_sink_node = list(sdf.stream.children)[0]
        # do random stuff
        sdf.apply(lambda x: x).update(lambda x: x)
        sdf = sdf.apply(lambda x: x)
        sdf.update(lambda x: x).apply(lambda x: x)
        sdf.update(lambda x: x)
        # no children should be added to the sink operation
        assert not sdf_sink_node.children


def add_n(n):
    return lambda value: value + n


def add_n_col(n, col="v"):
    return lambda value: value[col] + n


def less_than(n):
    return lambda v: v < n


def div_n(n):
    return lambda value: value // n


def add_n_df(n):
    def wrapper(value):
        return {**value, "v": value["v"] + n}

    return wrapper


class TestStreamingDataFrameBranching:
    def test_basic_branching(self, dataframe_factory):
        sdf = dataframe_factory().apply(lambda v: v + 1)
        sdf.apply(lambda v: v + 2)
        sdf.apply(lambda v: v + 3)
        sdf = sdf.apply(lambda v: v + 100)

        key, timestamp, headers = b"key", 0, []
        value = 0
        expected = [
            (3, key, timestamp, headers),
            (4, key, timestamp, headers),
            (101, key, timestamp, headers),
        ]
        results = sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)
        assert results == expected

    def test_multiple_branches(self, dataframe_factory):
        """
        INPUT: 0
        └── SDF_1 = (add 120, div 2) -> 60
            ├── SDF_2 = (div 3) -> 20
            │   ├── SDF_3 = (add 10, add 3  ) -> 33
            │   ├── SDF_4 = (    add 24     ) -> 44
            │   └── SDF_2 = (    add 2      ) -> 22
            └── SDF_1 = (add 40) -> 100
                ├── SDF_5 = ( div 2, add 5  ) -> 55
                └── SDF_1 = (div 100, add 10) -> 11
        """

        sdf = dataframe_factory().apply(add_n(120)).apply(div_n(2))  # 60
        sdf_2 = sdf.apply(div_n(3))  # 20
        sdf_3 = sdf_2.apply(add_n(10)).apply(add_n(3))  # 33  # noqa: F841
        sdf_4 = sdf_2.apply(add_n(24))  # 44  # noqa: F841
        sdf_2 = sdf_2.apply(add_n(2))  # 22
        sdf = sdf.apply(add_n(40))  # 100
        sdf_5 = sdf.apply(div_n(2)).apply(add_n(5))  # 55  # noqa: F841
        sdf = sdf.apply(div_n(100)).apply(add_n(10))  # 11

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [
            (33, *extras),
            (44, *extras),
            (22, *extras),
            (55, *extras),
            (11, *extras),
        ]
        results = sdf.test(value=0, **_extras)

        assert results == expected

    def test_multiple_branches_skip_assigns(self, dataframe_factory):
        """
        INPUT: 0
        └── SDF_1 = (add 120, div 2) -> 60
            ├── SDF_2 = (div 3) -> 20
            │   ├── (add 10, add 3  ) -> 33
            │   ├── (    add 24     ) -> 44
            │   └── (    add 2      ) -> 22
            └── SDF_1 = (add 40) -> 100
                ├── ( div 2, add 5  ) -> 55
                └── (div 100, add 10) -> 11
        """

        sdf = dataframe_factory().apply(add_n(120)).apply(div_n(2))  # 60
        sdf_2 = sdf.apply(div_n(3))  # 20
        sdf_2.apply(add_n(10)).apply(add_n(3))  # 33
        sdf_2.apply(add_n(24))  # 44
        sdf_2.apply(add_n(2))  # 22
        sdf = sdf.apply(add_n(40))  # 100
        sdf.apply(div_n(2)).apply(add_n(5))  # 55
        sdf.apply(div_n(100)).apply(add_n(10))  # 11

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [
            (33, *extras),
            (44, *extras),
            (22, *extras),
            (55, *extras),
            (11, *extras),
        ]
        results = sdf.test(value=0, **_extras)

        assert results == expected

    def test_filter(self, dataframe_factory):
        sdf = dataframe_factory().apply(add_n(10))
        sdf2 = sdf.apply(add_n(5)).filter(less_than(0)).apply(add_n(200))  # noqa: F841
        sdf3 = sdf.apply(add_n(7)).filter(less_than(20)).apply(add_n(4))  # noqa: F841
        sdf = sdf.apply(add_n(30)).filter(less_than(50))
        sdf4 = sdf.apply(add_n(60))  # noqa: F841
        sdf.apply(add_n(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [(21, *extras), (100, *extras), (840, *extras)]
        results = sdf.test(value=0, **_extras)

        # each operation is only called once (no redundant processing)
        assert results == expected

    def test_filter_using_sdf_apply_and_col_select(self, dataframe_factory):
        sdf = dataframe_factory().apply(add_n(10))
        sdf2 = sdf[sdf.apply(less_than(0))].apply(add_n(200))  # noqa: F841
        sdf3 = sdf[sdf.apply(add_n(8)).apply(less_than(20))].apply(add_n(33))  # noqa: F841
        sdf = sdf[sdf.apply(add_n(30)).apply(less_than(50))].apply(add_n(77))
        sdf4 = sdf.apply(add_n(60))  # noqa: F841
        sdf = sdf.apply(add_n(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [(43, *extras), (147, *extras), (887, *extras)]
        results = sdf.test(value=0, **_extras)

        assert results == expected

    def test_filter_using_columns(self, dataframe_factory):
        sdf = dataframe_factory().apply(add_n_df(10))
        sdf2 = sdf[sdf["v"] < 0].apply(add_n_df(200))  # noqa: F841
        sdf3 = sdf[sdf["v"].apply(add_n(1)).apply(add_n(7)) < 20].apply(add_n_df(33))  # noqa: F841
        sdf = sdf[sdf["v"].apply(add_n(5)).apply(add_n(25)) < 50].apply(add_n_df(77))
        sdf4 = sdf.apply(add_n_df(60))  # noqa: F841
        sdf = sdf.apply(add_n_df(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [({"v": 43}, *extras), ({"v": 147}, *extras), ({"v": 887}, *extras)]
        results = sdf.test(value={"v": 0}, **_extras)

        assert results == expected

    def test_store_series_filter_as_var_and_use(self, dataframe_factory):
        """
        NOTE: This is NOT dependent on branching functionality, but branching may
        encourage these sorts of operations (it does NOT copy data).

        NOTE: this kind of operation is only guaranteed to be correct when the stored
        series is IMMEDIATELY used. If any operations manipulate any of the references
        used within the series, those manipulations will persist/apply for the stored
        result as well.

        Basically, storing a series is like using a function with mutable args.
        """
        sdf = dataframe_factory().apply(add_n_df(10))
        sdf_filter = sdf["v"].apply(add_n(1)).apply(add_n(7)) < 20  # NOT a split
        sdf2 = sdf[sdf_filter].apply(add_n_df(33))  # noqa: F841
        sdf = sdf.apply(add_n_df(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [({"v": 43}, *extras), ({"v": 810}, *extras)]
        results = sdf.test(value={"v": 0}, **_extras)

        assert results == expected

    def test_store_series_result_as_var_and_use(self, dataframe_factory):
        """
        NOTE: This is NOT dependent on branching functionality, but branching may
        encourage these sorts of operations (it does NOT copy data).

        NOTE: this kind of operation is only guaranteed to be correct when the stored
        series is IMMEDIATELY used. If any operations manipulate any of the references
        used within the series, those manipulations will persist/apply for the stored
        result as well.

        Basically, storing a series is like using a function with mutable args.
        """
        sdf = dataframe_factory().apply(add_n_df(10))
        sdf_sum = sdf["v"].apply(add_n(1)) + 8  # NOT a split (no data cloning)
        sdf2 = sdf[sdf["v"] + sdf_sum < 30].apply(add_n_df(33))  # noqa: F841
        sdf = sdf.apply(add_n_df(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [({"v": 43}, *extras), ({"v": 810}, *extras)]
        results = sdf.test(value={"v": 0}, **_extras)

        assert results == expected

    def test_store_sdf_filter_as_var_and_use(self, dataframe_factory):
        """
        NOTE: This is NOT dependent on branching functionality, but branching may
        encourage these sorts of operations.

        NOTE: This WILL copy data, so it is basically a more inefficient way of doing
        a filter using series.

        NOTE: this kind of operation is only guaranteed to be correct when the stored
        series is IMMEDIATELY used. If any operations manipulate any of the references
        used within the series, those manipulations will persist/apply for the stored
        result as well.

        Basically, storing a sdf is like using a function with mutable args.
        """
        sdf = dataframe_factory().apply(add_n(10))
        sdf_filter = sdf.apply(less_than(20))
        sdf = sdf[sdf_filter].apply(add_n(33))
        sdf = sdf.apply(add_n(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [(843, *extras)]
        results = sdf.test(value=0, **_extras)

        assert results == expected

    def test_store_sdf_as_var_and_use_in_split(self, dataframe_factory):
        """
        NOTE: This WILL copy data, so it is basically a more inefficient way of doing
        a filter using StreamingSeries.

        NOTE: this kind of operation is only guaranteed to be correct when the stored
        series is IMMEDIATELY used. If any operations manipulate any of the references
        used within the series, those manipulations will persist/apply for the stored
        result as well.

        Basically, storing a sdf is like using a function with mutable args.
        """
        sdf = dataframe_factory().apply(add_n(10))
        sdf_filter = sdf.apply(less_than(20))
        sdf2 = sdf[sdf_filter].apply(add_n(33))  # noqa: F841
        sdf = sdf.apply(add_n(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [(43, *extras), (810, *extras)]
        results = sdf.test(value=0, **_extras)

        assert results == expected

    def test_reuse_sdf_as_filter_fails(self, dataframe_factory):
        """
        Attempting to reuse a filtering SDF will fail.
        """
        sdf = dataframe_factory().apply(add_n(10))
        sdf_filter = sdf.apply(less_than(20))
        sdf2 = sdf[sdf_filter].apply(add_n(100))  # noqa: F841

        with pytest.raises(
            InvalidOperation,
            match="Cannot use a filtering or column-setter SDF more than once",
        ):
            sdf[sdf_filter].apply(add_n(200))

    def test_sdf_as_filter_with_added_operation_fails(self, dataframe_factory):
        """
        Using a filtering SDF with further operations added (post-assignment) fails.

        Why:
        The additional operation is actually included in the filter SDF, which is
        unintuitive even if you understand how SDF works internally.
        """
        sdf = dataframe_factory().apply(add_n(10))
        sdf_filter = sdf.apply(less_than(20))
        sdf = sdf.apply(add_n(50))  # "additional" operation

        with pytest.raises(
            InvalidOperation,
            match="Cannot assign or filter using a branched SDF",
        ):
            sdf[sdf_filter].apply(add_n(100))

    def test_sdf_as_filter_in_another_split_fails(self, dataframe_factory):
        """
        Using a filtering SDF with further operations added (post-assignment) fails.

        This case uses a "split" SDF to filter with (though functionally it's somewhat
        similar to non-split), with a unique side effect.

        Why:
        The additional operation on the filtering SDF becomes orphaned, producing no
        result where one would likely be expected.
        """
        sdf = dataframe_factory().apply(add_n(10))
        sdf2 = sdf.apply(add_n(5))
        sdf_filter = sdf2.apply(less_than(20))
        sdf2 = sdf2.apply(add_n(25))  # this would become orphaned

        with pytest.raises(
            InvalidOperation,
            match="Cannot assign or filter using a branched SDF",
        ):
            sdf[sdf_filter].apply(add_n(100))

    def test_update(self, dataframe_factory):
        """
        "Update" functions work with split behavior.
        """

        def mul_n(n):
            def wrapper(value):
                value["v"] *= n

            return wrapper

        sdf = dataframe_factory().apply(add_n_df(1))
        sdf2 = sdf.apply(add_n_df(2)).update(mul_n(2))  # noqa: F841
        sdf3 = sdf.apply(add_n_df(3))
        sdf3.update(mul_n(3))
        sdf = sdf.update(mul_n(4)).apply(add_n_df(100))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [({"v": 6}, *extras), ({"v": 12}, *extras), ({"v": 104}, *extras)]
        results = sdf.test(value={"v": 0}, **_extras)

        assert results == expected

    def test_set_timestamp(self, dataframe_factory):
        """
        "Transform" functions work with split behavior.
        """

        def set_ts(n):
            return lambda value, key, timestamp, headers: timestamp + n

        sdf = dataframe_factory().apply(add_n(1))
        sdf2 = sdf.apply(add_n(2)).set_timestamp(set_ts(3)).set_timestamp(set_ts(5))  # noqa: F841
        sdf3 = sdf.apply(add_n(3))  # noqa: F841
        sdf = sdf.set_timestamp(set_ts(4)).apply(add_n(7))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [(3, b"key", 8, []), (4, *extras), (8, b"key", 4, [])]
        results = sdf.test(value=0, **_extras)

        assert results == expected

    def test_column_assign_reuse_fails(self, dataframe_factory):
        sdf = dataframe_factory().apply(add_n_df(10))
        new_v = sdf.apply(add_n_df(1)).apply(add_n_col(7))
        sdf["new_v0"] = new_v

        with pytest.raises(
            InvalidOperation,
            match="Cannot use a filtering or column-setter SDF more than once",
        ):
            sdf["new_v1"] = new_v

    def test_sdf_as_column_setter_in_another_split_fails(self, dataframe_factory):
        """
        Using a filtering SDF with further operations added (post-assignment) fails.

        This case uses a "split" SDF to filter with (though functionally it's somewhat
        similar to non-split), with a unique side effect.

        Why:
        The additional operation on the filtering SDF becomes orphaned, producing no
        result where one would likely be expected.
        """
        sdf = dataframe_factory().apply(add_n_df(10))
        sdf2 = sdf.apply(add_n_df(5))
        new_val = sdf2.apply(add_n_col(30))
        sdf2 = sdf2.apply(add_n_df(25))  # this would become orphaned

        with pytest.raises(
            InvalidOperation,
            match="Cannot assign or filter using a branched SDF",
        ):
            sdf["n_new"] = new_val

    def test_sdf_as_column_setter_with_added_operation_fails(self, dataframe_factory):
        """
        Using a filtering SDF with further operations added (post-assignment) fails.

        Why:
        The additional operation is actually included in the filter SDF, which is
        unintuitive even if you understand how SDF works internally.
        """
        sdf = dataframe_factory().apply(add_n_df(10))
        new_val = sdf.apply(add_n_col(30))
        sdf = sdf.apply(add_n_df(50))  # "additional" operation

        with pytest.raises(
            InvalidOperation,
            match="Cannot assign or filter using a branched SDF",
        ):
            sdf["n_new"] = new_val

    def test_column_setter(self, dataframe_factory):
        sdf = dataframe_factory().apply(add_n_df(10))
        sdf2 = sdf.apply(add_n_df(5))
        sdf2["v"] = sdf2.apply(add_n_col(1))
        sdf3 = sdf.apply(add_n_df(7))
        sdf3["v"] = sdf3.apply(add_n_col(20))
        sdf["v"] = sdf.apply(add_n_df(25)).apply(add_n_col(55))
        sdf4 = sdf.apply(add_n_df(100))  # noqa: F841
        sdf["v"] = sdf.apply(add_n_col(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [({"v": v}, *extras) for v in [16, 37, 190, 890]]
        results = sdf.test(value={"v": 0}, **_extras)
        assert results == expected

    def test_store_sdf_setter_as_var_and_use(self, dataframe_factory):
        """
        NOTE: This is NOT dependent on branching functionality, but branching may
        encourage these sorts of operations.
        """
        sdf = dataframe_factory().apply(add_n_df(10))
        new_val = sdf.apply(add_n_col(20))
        sdf = sdf[new_val].apply(add_n_df(33))
        sdf = sdf.apply(add_n_df(800))

        _extras = {"key": b"key", "timestamp": 0, "headers": []}
        extras = list(_extras.values())
        expected = [({"v": 843}, *extras)]
        results = sdf.test(value={"v": 0}, **_extras)

        assert results == expected

    def test_column_setting_from_another_sdf_series_fails(self, dataframe_factory):
        """
        Attempting to set a column based on another SDF series operation fails,
        as series are only intended to be applied to the same SDF.
        :param dataframe_factory:
        :return:
        """

        sdf = dataframe_factory().apply(add_n_df(10))
        sdf2 = sdf.apply(add_n_df(500))

        with pytest.raises(InvalidOperation, match="Column-setting"):
            sdf["new_col"] = sdf2["v"] + 3

    def test_column_operations_different_sdfs_fails(self, dataframe_factory):
        """
        Attempting series operations involving multiple SDFs fails,
        as series are only intended to be applied to the same SDF.
        :param dataframe_factory:
        :return:
        """

        sdf = dataframe_factory().apply(add_n_df(10))
        sdf2 = sdf.apply(add_n_df(500))
        sdf3 = sdf.apply(add_n_df(800))

        with pytest.raises(InvalidOperation, match="All column operations"):
            sdf["new_col"] = sdf2["v"] + sdf3["v"]


class TestStreamingDataFrameConcat:
    def test_concat_different_topics_success(
        self, topic_manager_factory, dataframe_factory
    ):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(
            str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        topic2 = topic_manager.topic(
            str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=2, replication_factor=1),
        )

        sdf1 = dataframe_factory(topic=topic1)
        sdf2 = dataframe_factory(topic=topic2)

        sdf_concatenated = sdf1.concat(sdf2).apply(lambda v: v + 1)
        assert sdf_concatenated.test(
            value=1, key=b"key1", timestamp=1, topic=topic1
        ) == [(2, b"key1", 1, None)]
        assert sdf_concatenated.test(
            value=2, key=b"key2", timestamp=1, topic=topic2
        ) == [(3, b"key2", 1, None)]

    def test_concat_same_topic_success(self, topic_manager_factory, dataframe_factory):
        """
        Test that `StreamingDataFrame.concat()` can merge two branches of the same SDF
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))

        sdf = dataframe_factory(topic)
        sdf_branch1 = sdf.apply(lambda v: v + 1)
        sdf_branch2 = sdf.apply(lambda v: v + 2)

        # Branching is not exclusive, and it duplicates data in this case.
        # Check that we receive the results from both branches
        sdf_concatenated = sdf_branch1.concat(sdf_branch2)
        assert sdf_concatenated.test(value=1, key=b"key1", timestamp=1) == [
            (2, b"key1", 1, None),
            (3, b"key1", 1, None),
        ]

    def test_concat_stateful_success(
        self,
        topic_manager_factory,
        dataframe_factory,
        state_manager,
        message_context_factory,
    ):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(str(uuid.uuid4()))
        topic2 = topic_manager.topic(str(uuid.uuid4()))

        def accumulate(value: dict, state: State) -> []:
            items = state.get("items", [])
            items.append(value)
            state.set("items", items)
            return items

        sdf1 = dataframe_factory(topic1, state_manager=state_manager)
        sdf2 = dataframe_factory(topic2, state_manager=state_manager)
        sdf_concatenated = sdf1.concat(sdf2).apply(accumulate, stateful=True)

        state_manager.on_partition_assign(
            stream_id=sdf_concatenated.stream_id,
            partition=0,
            committed_offsets={},
        )

        key, timestamp, headers = b"key", 0, None

        assert sdf_concatenated.test(
            1,
            key=key,
            timestamp=timestamp,
            headers=headers,
            topic=topic1,
            ctx=message_context_factory(topic=topic1.name),
        ) == [([1], key, timestamp, headers)]

        assert sdf_concatenated.test(
            2,
            key=key,
            timestamp=timestamp,
            headers=headers,
            topic=topic2,
            ctx=message_context_factory(topic=topic2.name),
        ) == [([1, 2], key, timestamp, headers)]

    def test_concat_stateful_mismatching_partitions_fails(
        self, topic_manager_factory, dataframe_factory, state_manager
    ):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(
            str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        topic2 = topic_manager.topic(
            str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=2, replication_factor=1),
        )

        sdf1 = dataframe_factory(topic1, state_manager=state_manager)
        sdf2 = dataframe_factory(topic2, state_manager=state_manager)
        with pytest.raises(
            TopicPartitionsMismatch,
            match="The underlying topics must have the same number of partitions to use State",
        ):
            sdf1.concat(sdf2).update(lambda v, state: None, stateful=True)
