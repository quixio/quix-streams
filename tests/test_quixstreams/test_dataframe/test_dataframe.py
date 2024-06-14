import operator
import uuid
from collections import namedtuple
from datetime import timedelta

import pytest

from quixstreams import State
from quixstreams.dataframe.exceptions import InvalidOperation, GroupByLimitExceeded
from quixstreams.dataframe.windows import WindowResult

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

        sdf = sdf.set_timestamp(lambda value_, timestamp_: timestamp_ + 100)

        result = sdf.test(value=value, key=key, timestamp=timestamp, headers=headers)[0]
        assert result == expected


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
    def test_to_topic(
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
        sdf = sdf.to_topic(topic)

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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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

    def test_tumbling_window_current_out_of_order_late(
        self,
        dataframe_factory,
        state_manager,
        message_context_factory,
        topic_manager_topic_factory,
    ):
        """
        Test that window with "latest" doesn't output the result if incoming timestamp
        is late
        """
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = sdf.tumbling_window(duration_ms=10, grace_ms=0).sum().current()

        state_manager.on_partition_assign(
            topic=topic.name, partition=0, committed_offset=-1001
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
        for value, key, timestamp in records:
            ctx = message_context_factory(topic=topic.name)
            result = sdf.test(
                value=value, key=key, timestamp=timestamp, headers=headers, ctx=ctx
            )
            results += result

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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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
            topic=topic.name, partition=0, committed_offset=-1001
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

        sdf = dataframe_factory(topic, topic_manager=topic_manager, producer=producer)
        sdf = sdf.group_by(col)
        sdf[col] = col_update

        groupby_topic = sdf.topic
        assert sdf.topics_to_subscribe == [topic, sdf.topic]
        assert (
            groupby_topic.name == topic_manager.repartition_topic(col, topic.name).name
        )

        topic_manager.create_all_topics()
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

        sdf = dataframe_factory(topic, topic_manager=topic_manager, producer=producer)
        sdf = sdf.group_by(col, name=op_name)
        sdf[col] = col_update

        groupby_topic = sdf.topic
        assert sdf.topics_to_subscribe == [topic, sdf.topic]
        assert (
            groupby_topic.name
            == topic_manager.repartition_topic(op_name, topic.name).name
        )

        topic_manager.create_all_topics()
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

        sdf = dataframe_factory(topic, topic_manager=topic_manager, producer=producer)
        sdf = sdf.group_by(lambda v: v[col], name=op_name)
        sdf[col] = col_update

        groupby_topic = sdf.topic
        assert [topic, sdf.topic] == sdf.topics_to_subscribe
        assert (
            groupby_topic.name
            == topic_manager.repartition_topic(op_name, topic.name).name
        )

        topic_manager.create_all_topics()
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

        with pytest.raises(TypeError):
            sdf.group_by({"um": "what is this"})

    def test_group_by_limit_exceeded(self, dataframe_factory, topic_manager_factory):
        """
        Only 1 GroupBy operation per SDF instance (or, what appears to end users
        as a "single" SDF instance).
        """
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(str(uuid.uuid4()))
        sdf = dataframe_factory(topic, topic_manager=topic_manager)
        sdf = sdf.group_by("col_a")

        with pytest.raises(GroupByLimitExceeded):
            sdf.group_by("col_b")
