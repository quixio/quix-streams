import operator
from datetime import timedelta

import pytest

from quixstreams import MessageContext, State
from quixstreams.core.stream import Filtered
from quixstreams.dataframe.exceptions import InvalidOperation
from quixstreams.dataframe.windows import WindowResult
from quixstreams.models import MessageTimestamp, Topic
from tests.utils import TopicPartitionStub


class TestStreamingDataFrame:
    @pytest.mark.parametrize(
        "value, expected",
        [(1, 2), ("input", "return"), ([0, 1, 2], "return"), ({"key": "value"}, None)],
    )
    def test_apply(self, dataframe_factory, value, expected):
        sdf = dataframe_factory()

        def _apply(value_: dict):
            assert value_ == value
            return expected

        sdf = sdf.apply(_apply)
        assert sdf.test(value) == expected

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
        assert sdf.test(value) == expected

    def test_apply_multiple(self, dataframe_factory):
        sdf = dataframe_factory()
        value = 1
        expected = 4
        sdf = sdf.apply(lambda v: v + 1).apply(lambda v: v + 2)
        assert sdf.test(value) == expected

    def test_apply_update_multiple(self, dataframe_factory):
        sdf = dataframe_factory()
        value = {"x": 1}
        expected = {"x": 3, "y": 3}
        sdf = (
            sdf.apply(lambda v: {"x": v["x"] + 1})
            .update(lambda v: operator.setitem(v, "y", 3))
            .apply(lambda v: {**v, "x": v["x"] + 1})
        )
        assert sdf.test(value) == expected

    def test_setitem_primitive(self, dataframe_factory):
        value = {"x": 1}
        expected = {"x": 2}
        sdf = dataframe_factory()
        sdf["x"] = 2
        assert sdf.test(value) == expected

    def test_setitem_series(self, dataframe_factory):
        value = {"x": 1, "y": 2}
        expected = {"x": 2, "y": 2}
        sdf = dataframe_factory()
        sdf["x"] = sdf["y"]
        assert sdf.test(value) == expected

    def test_setitem_series_apply(self, dataframe_factory):
        value = {"x": 1}
        expected = {"x": 1, "y": 2}
        sdf = dataframe_factory()
        sdf["y"] = sdf["x"].apply(lambda v: v + 1)
        assert sdf.test(value) == expected

    def test_setitem_series_with_operations(self, dataframe_factory):
        value = {"x": 1, "y": 2}
        expected = {"x": 1, "y": 2, "z": 5}
        sdf = dataframe_factory()
        sdf["z"] = (sdf["x"] + sdf["y"]).apply(lambda v: v + 1) + 1
        assert sdf.test(value) == expected

    def test_setitem_another_dataframe_apply(self, dataframe_factory):
        value = {"x": 1}
        expected = {"x": 1, "y": 2}
        sdf = dataframe_factory()
        sdf["y"] = sdf.apply(lambda v: v["x"] + 1)
        assert sdf.test(value) == expected

    def test_column_subset(self, dataframe_factory):
        value = {"x": 1, "y": 2, "z": 3}
        expected = {"x": 1, "y": 2}
        sdf = dataframe_factory()
        sdf = sdf[["x", "y"]]
        assert sdf.test(value) == expected

    def test_column_subset_and_apply(self, dataframe_factory):
        value = {"x": 1, "y": 2, "z": 3}
        expected = 2
        sdf = dataframe_factory()
        sdf = sdf[["x", "y"]]
        sdf = sdf.apply(lambda v: v["y"])
        assert sdf.test(value) == expected

    @pytest.mark.parametrize(
        "value, filtered",
        [
            ({"x": 1, "y": 2}, False),
            ({"x": 0, "y": 2}, True),
        ],
    )
    def test_filter_with_series(self, dataframe_factory, value, filtered):
        sdf = dataframe_factory()
        sdf = sdf[sdf["x"] > 0]

        if filtered:
            with pytest.raises(Filtered):
                assert sdf.test(value)
        else:
            assert sdf.test(value) == value

    @pytest.mark.parametrize(
        "value, filtered",
        [
            ({"x": 1, "y": 2}, False),
            ({"x": 0, "y": 2}, True),
        ],
    )
    def test_filter_with_series_apply(self, dataframe_factory, value, filtered):
        sdf = dataframe_factory()
        sdf = sdf[sdf["x"].apply(lambda v: v > 0)]

        if filtered:
            with pytest.raises(Filtered):
                assert sdf.test(value)
        else:
            assert sdf.test(value) == value

    @pytest.mark.parametrize(
        "value, filtered",
        [
            ({"x": 1, "y": 2}, False),
            ({"x": 0, "y": 2}, True),
        ],
    )
    def test_filter_with_multiple_series(self, dataframe_factory, value, filtered):
        sdf = dataframe_factory()
        sdf = sdf[(sdf["x"] > 0) & (sdf["y"] > 0)]

        if filtered:
            with pytest.raises(Filtered):
                assert sdf.test(value)
        else:
            assert sdf.test(value) == value

    @pytest.mark.parametrize(
        "value, filtered",
        [
            ({"x": 1, "y": 2}, False),
            ({"x": 0, "y": 2}, True),
        ],
    )
    def test_filter_with_another_sdf_apply(self, dataframe_factory, value, filtered):
        sdf = dataframe_factory()
        sdf = sdf[sdf.apply(lambda v: v["x"] > 0)]

        if filtered:
            with pytest.raises(Filtered):
                assert sdf.test(value)
        else:
            assert sdf.test(value) == value

    def test_filter_with_another_sdf_with_filters_fails(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf2 = sdf[sdf["x"] > 1].apply(lambda v: v["x"] > 0)
        with pytest.raises(ValueError, match="Filter functions are not allowed"):
            sdf = sdf[sdf2]

    def test_filter_with_another_sdf_with_update_fails(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf2 = sdf.apply(lambda v: v).update(lambda v: operator.setitem(v, "x", 2))
        with pytest.raises(ValueError, match="Update functions are not allowed"):
            sdf = sdf[sdf2]

    @pytest.mark.parametrize(
        "value, filtered",
        [
            ({"x": 1, "y": 2}, False),
            ({"x": 0, "y": 2}, True),
        ],
    )
    def test_filter_with_function(self, dataframe_factory, value, filtered):
        sdf = dataframe_factory()
        sdf = sdf.filter(lambda v: v["x"] > 0)

        if filtered:
            with pytest.raises(Filtered):
                assert sdf.test(value)
        else:
            assert sdf.test(value) == value

    def test_contains_on_existing_column(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf["has_column"] = sdf.contains("x")
        assert sdf.test({"x": 1}) == {"x": 1, "has_column": True}

    def test_contains_on_missing_column(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf["has_column"] = sdf.contains("wrong_column")

        assert sdf.test({"x": 1}) == {"x": 1, "has_column": False}

    def test_contains_as_filter(self, dataframe_factory):
        sdf = dataframe_factory()
        sdf = sdf[sdf.contains("x")]

        valid_value = {"x": 1, "y": 2}
        assert sdf.test(valid_value) == valid_value

        invalid_value = {"y": 2}
        with pytest.raises(Filtered):
            sdf.test(invalid_value)

    def test_cannot_use_logical_and(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(InvalidOperation):
            sdf["truth"] = sdf[sdf.apply(lambda x: x["a"] > 0)] and sdf[["b"]]

    def test_cannot_use_logical_or(self, dataframe_factory):
        sdf = dataframe_factory()
        with pytest.raises(InvalidOperation):
            sdf["truth"] = sdf[sdf.apply(lambda x: x["a"] > 0)] or sdf[["b"]]


class TestStreamingDataFrameApplyExpand:
    @pytest.mark.parametrize(
        "value, expected",
        [(1, [1, 1]), ({"key": "value"}, [{"key": "value"}, {"key": "value"}])],
    )
    def test_apply_expand(self, dataframe_factory, value, expected):
        sdf = dataframe_factory()
        sdf = sdf.apply(lambda v: [v, v], expand=True)
        result = sdf.test(value)
        assert result == expected

    def test_apply_expand_filter(self, dataframe_factory):
        value = 1
        expected = [1]
        sdf = dataframe_factory()
        sdf = sdf.apply(lambda v: [v, v + 1], expand=True)
        sdf = sdf[sdf.apply(lambda v: v != 2)]
        result = sdf.test(value)
        assert result == expected

    def test_apply_expand_update(self, dataframe_factory):
        value = {"x": 1}
        expected = [{"x": 2}, {"x": 2}]
        sdf = dataframe_factory()
        sdf = sdf.apply(lambda v: [v, v], expand=True)
        sdf["x"] = 2
        result = sdf.test(value)
        assert result == expected

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
    ):
        topic = topic_manager_topic_factory(
            key_deserializer="str",
            value_serializer="json",
            value_deserializer="json",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory()
        sdf.producer = producer
        sdf = sdf.to_topic(topic)

        value = {"x": 1, "y": 2}
        ctx = MessageContext(
            key="test",
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )

        with producer:
            sdf.test(value, ctx=ctx)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == topic.name
        assert consumed_row.key == ctx.key
        assert consumed_row.value == value

    def test_to_topic_apply_expand(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            key_deserializer="str",
            value_serializer="json",
            value_deserializer="json",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory()
        sdf.producer = producer

        sdf = sdf.apply(lambda v: [v, v], expand=True).to_topic(topic)

        value = {"x": 1, "y": 2}
        ctx = MessageContext(
            key="test",
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )

        with producer:
            sdf.test(value, ctx=ctx)

        consumed = []
        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            for _ in range(2):
                row = consumer.poll_row(timeout=5.0)
                consumed.append(row)

        assert len(consumed) == 2
        for row in consumed:
            assert row.topic == topic.name
            assert row.key == ctx.key
            assert row.value == value

    def test_to_topic_custom_key(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(
            value_serializer="json",
            value_deserializer="json",
            key_serializer="int",
            key_deserializer="int",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory()
        sdf.producer = producer

        # Use value["x"] as a new key
        sdf = sdf.to_topic(topic, key=lambda v: v["x"])

        value = {"x": 1, "y": 2}
        ctx = MessageContext(
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )

        with producer:
            sdf.test(value, ctx=ctx)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == topic.name
        assert consumed_row.value == value
        assert consumed_row.key == value["x"]

    def test_to_topic_multiple_topics_out(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        topic_manager_topic_factory,
    ):
        topic_0 = topic_manager_topic_factory(
            value_serializer="json",
            value_deserializer="json",
        )
        topic_1 = topic_manager_topic_factory(
            value_serializer="json",
            value_deserializer="json",
        )
        producer = row_producer_factory()

        sdf = dataframe_factory()
        sdf.producer = producer

        sdf = sdf.to_topic(topic_0).to_topic(topic_1)

        value = {"x": 1, "y": 2}
        ctx = MessageContext(
            key=b"test",
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )

        with producer:
            sdf.test(value, ctx=ctx)

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
            assert consumed_row.key == ctx.key
            assert consumed_row.value == value

    def test_to_topic_no_producer_assigned(
        self, dataframe_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory()

        sdf = dataframe_factory()
        sdf = sdf.to_topic(topic)

        value = {"x": "1", "y": "2"}
        ctx = MessageContext(
            key=b"test",
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )

        with pytest.raises(
            RuntimeError, match="Producer instance has not been provided"
        ):
            sdf.test(value, ctx=ctx)


class TestStreamingDataframeStateful:
    def test_apply_stateful(
        self, dataframe_factory, state_manager, topic_manager_topic_factory
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        values = [
            {"number": 1},
            {"number": 10},
            {"number": 3},
        ]
        result = None
        ctx = MessageContext(
            key=b"test",
            topic=topic.name,
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )
        for value in values:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                result = sdf.test(value, ctx)

        assert result == 10

    def test_update_stateful(
        self, dataframe_factory, state_manager, topic_manager_topic_factory
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        result = None
        values = [
            {"number": 1},
            {"number": 10},
            {"number": 3},
        ]
        ctx = MessageContext(
            key=b"test",
            topic=topic.name,
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )
        for value in values:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                result = sdf.test(value, ctx)

        assert result is not None
        assert result["max"] == 10

    def test_filter_stateful(
        self, dataframe_factory, state_manager, topic_manager_topic_factory
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        values = [
            {"number": 1},
            {"number": 1},
            {"number": 3},
        ]
        ctx = MessageContext(
            key=b"test",
            topic=topic.name,
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )
        results = []
        for value in values:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                try:
                    results.append(sdf.test(value, ctx))
                except Filtered:
                    pass
        assert len(results) == 1
        assert results[0]["max"] == 3

    def test_filter_with_another_sdf_apply_stateful(
        self, dataframe_factory, state_manager, topic_manager_topic_factory
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        values = [
            {"number": 1},
            {"number": 1},
            {"number": 3},
        ]
        ctx = MessageContext(
            key=b"test",
            topic=topic.name,
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )
        results = []
        for value in values:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                try:
                    results.append(sdf.test(value, ctx))
                except Filtered:
                    pass
        assert len(results) == 1
        assert results[0]["max"] == 3


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
        topic = topic_manager_topic_factory(
            name="test",
        )

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf = (
            sdf.tumbling_window(
                duration_ms=timedelta(seconds=10), grace_ms=timedelta(seconds=1)
            )
            .sum()
            .current()
        )

        state_manager.on_partition_assign(
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Message early in the window
            (1, message_context_factory(key="test", timestamp_ms=1000)),
            # Message towards the end of the window
            (2, message_context_factory(key="test", timestamp_ms=9000)),
            # Should start a new window
            (3, message_context_factory(key="test", timestamp_ms=20010)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                results += sdf.test(value=value, ctx=ctx)
        assert len(results) == 3
        assert results == [
            WindowResult(value=1, start=0, end=10000),
            WindowResult(value=3, start=0, end=10000),
            WindowResult(value=3, start=20000, end=30000),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0, 10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Create window [20,30)
            (2, message_context_factory(key="test", timestamp_ms=20)),
            # Late message - it belongs to window [0,10) but this window
            # is already closed. This message should be skipped from processing
            (3, message_context_factory(key="test", timestamp_ms=9)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                result = sdf.test(value=value, ctx=ctx)
                results += result

        assert len(results) == 2
        assert results == [
            WindowResult(value=1, start=0, end=10),
            WindowResult(value=2, start=20, end=30),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0, 10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Update window [0, 10)
            (1, message_context_factory(key="test", timestamp_ms=2)),
            # Create window [20,30). Window [0, 10) is expired now.
            (2, message_context_factory(key="test", timestamp_ms=20)),
            # Create window [30, 40). Window [20, 30) is expired now.
            (3, message_context_factory(key="test", timestamp_ms=39)),
            # Update window [30, 40). Nothing should be returned.
            (4, message_context_factory(key="test", timestamp_ms=38)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                result = sdf.test(value=value, ctx=ctx)
                results += result

        assert len(results) == 2
        assert results == [
            WindowResult(value=2, start=0, end=10),
            WindowResult(value=2, start=20, end=30),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0,10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Message with None key, expected to be ignored
            (10, message_context_factory(key=None, timestamp_ms=100)),
            # Update window [0,10)
            (2, message_context_factory(key="test", timestamp_ms=2)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                results += sdf.test(value=value, ctx=ctx)

        assert len(results) == 2
        # Ensure that the windows are returned with correct values and order
        assert results == [
            WindowResult(value=1, start=0, end=10),
            WindowResult(value=3, start=0, end=10),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0,10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Update window [0,10) and create window [5,15)
            (2, message_context_factory(key="test", timestamp_ms=7)),
            # Update window [5,15) and create window [10,20)
            (3, message_context_factory(key="test", timestamp_ms=10)),
            # Create windows [30, 40) and [35, 45)
            (4, message_context_factory(key="test", timestamp_ms=35)),
            # Update windows [30, 40) and [35, 45)
            (5, message_context_factory(key="test", timestamp_ms=35)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                results += sdf.test(value=value, ctx=ctx)

        assert len(results) == 9
        # Ensure that the windows are returned with correct values and order
        assert results == [
            WindowResult(value=1, start=0, end=10),
            WindowResult(value=3, start=0, end=10),
            WindowResult(value=2, start=5, end=15),
            WindowResult(value=5, start=5, end=15),
            WindowResult(value=3, start=10, end=20),
            WindowResult(value=4, start=30, end=40),
            WindowResult(value=4, start=35, end=45),
            WindowResult(value=9, start=30, end=40),
            WindowResult(value=9, start=35, end=45),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0,10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Update window [0,10) and create window [5,15)
            (2, message_context_factory(key="test", timestamp_ms=7)),
            # Create windows [30, 40) and [35, 45)
            (4, message_context_factory(key="test", timestamp_ms=35)),
            # Timestamp "10" is late and should not be processed
            (3, message_context_factory(key="test", timestamp_ms=10)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                results += sdf.test(value=value, ctx=ctx)

        assert len(results) == 5
        # Ensure that the windows are returned with correct values and order
        assert results == [
            WindowResult(value=1, start=0, end=10),
            WindowResult(value=3, start=0, end=10),
            WindowResult(value=2, start=5, end=15),
            WindowResult(value=4, start=30, end=40),
            WindowResult(value=4, start=35, end=45),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0,10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Update window [0,10) and create window [5,15)
            (2, message_context_factory(key="test", timestamp_ms=7)),
            # Update window [5,15) and create window [10,20)
            (3, message_context_factory(key="test", timestamp_ms=10)),
            # Create windows [30, 40) and [35, 45).
            # Windows [0,10), [5,15) and [10,20) should be expired
            (4, message_context_factory(key="test", timestamp_ms=35)),
            # Update windows [30, 40) and [35, 45)
            (5, message_context_factory(key="test", timestamp_ms=35)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                results += sdf.test(value=value, ctx=ctx)

        assert len(results) == 3
        # Ensure that the windows are returned with correct values and order
        assert results == [
            WindowResult(value=3, start=0, end=10),
            WindowResult(value=5, start=5, end=15),
            WindowResult(value=3, start=10, end=20),
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
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        messages = [
            # Create window [0,10)
            (1, message_context_factory(key="test", timestamp_ms=1)),
            # Message with None key, expected to be ignored
            (10, message_context_factory(key=None, timestamp_ms=100)),
            # Update window [0,10)
            (2, message_context_factory(key="test", timestamp_ms=2)),
        ]

        results = []
        for value, ctx in messages:
            with state_manager.start_store_transaction(
                topic=ctx.topic, partition=ctx.partition, offset=ctx.offset
            ):
                results += sdf.test(value=value, ctx=ctx)

        assert len(results) == 2
        # Ensure that the windows are returned with correct values and order
        assert results == [
            WindowResult(value=1, start=0, end=10),
            WindowResult(value=3, start=0, end=10),
        ]
