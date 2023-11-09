import pytest
from tests.utils import TopicPartitionStub

from quixstreams.dataframe.exceptions import InvalidApplyResultType
from quixstreams.dataframe.pipeline import Pipeline
from quixstreams.models import MessageContext
from quixstreams.models.topics import Topic
from quixstreams.state import State


class TestDataframe:
    def test_dataframe(self, dataframe_factory):
        dataframe = dataframe_factory()
        assert isinstance(dataframe._pipeline, Pipeline)
        assert dataframe._pipeline.id == dataframe.id


class TestDataframeProcess:
    def test_apply(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        row = row_factory({"x": 1, "y": 2}, key="key")

        def _apply(value: dict, ctx: MessageContext):
            assert ctx.key == "key"
            assert value == {"x": 1, "y": 2}
            return {
                "x": 3,
                "y": 4,
            }

        dataframe.apply(_apply)
        assert dataframe.process(row).value == {"x": 3, "y": 4}

    def test_apply_no_return_value(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(lambda row, ctx: row.update({"y": 2}))
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "y": 2}).value

    def test_apply_invalid_return_type(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(lambda row, ctx: False)
        row = row_factory({"x": 1, "y": 2})
        with pytest.raises(InvalidApplyResultType):
            dataframe.process(row)

    def test_apply_fluent(self, dataframe_factory, row_factory, row_plus_n_func):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(row_plus_n_func(n=1)).apply(row_plus_n_func(n=2))
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row_factory({"x": 4, "y": 5}).value

    def test_apply_sequential(self, dataframe_factory, row_factory, row_plus_n_func):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(row_plus_n_func(n=1))
        dataframe = dataframe.apply(row_plus_n_func(n=2))
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row_factory({"x": 4, "y": 5}).value

    def test_setitem_primitive(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe["new"] = 1
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "new": 1}).value

    def test_setitem_column_only(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe["new"] = dataframe["x"]
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "new": 1}).value

    def test_setitem_column_with_function(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe["new"] = dataframe["x"].apply(lambda v, ctx: v + 5)
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "new": 6}).value

    def test_setitem_column_with_operations(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe["new"] = (
            dataframe["x"] + dataframe["y"].apply(lambda v, ctx: v + 5) + 1
        )
        row = row_factory({"x": 1, "y": 2})
        expected = row_factory({"x": 1, "y": 2, "new": 9})
        assert dataframe.process(row).value == expected.value

    def test_setitem_from_a_nested_column(
        self, dataframe_factory, row_factory, row_plus_n_func
    ):
        dataframe = dataframe_factory()
        dataframe["a"] = dataframe["x"]["y"]
        row = row_factory({"x": {"y": 1, "z": "banana"}})
        expected = row_factory({"x": {"y": 1, "z": "banana"}, "a": 1})
        assert dataframe.process(row).value == expected.value

    def test_column_subset(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[["x", "y"]]
        row = row_factory({"x": 1, "y": 2, "z": 3})
        expected = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == expected.value

    def test_column_subset_with_funcs(
        self, dataframe_factory, row_factory, row_plus_n_func
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe[["x", "y"]].apply(row_plus_n_func(n=5))
        row = row_factory({"x": 1, "y": 2, "z": 3})
        expected = row_factory({"x": 6, "y": 7})
        assert dataframe.process(row).value == expected.value

    def test_inequality_filter(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[dataframe["x"] > 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row.value

    def test_inequality_filter_is_filtered(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[dataframe["x"] >= 1000]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row) is None

    def test_inequality_filter_with_operation(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[(dataframe["x"] - 0 + dataframe["y"]) > 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row.value

    def test_inequality_filter_with_operation_is_filtered(
        self, dataframe_factory, row_factory
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe[(dataframe["x"] - dataframe["y"]) > 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row) is None

    def test_inequality_filtering_with_apply(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[dataframe["x"].apply(lambda v, ctx: v - 1) >= 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row.value

    def test_inequality_filtering_with_apply_is_filtered(
        self, dataframe_factory, row_factory
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe[dataframe["x"].apply(lambda v, ctx: v - 10) >= 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row) is None

    def test_compound_inequality_filter(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[(dataframe["x"] >= 0) & (dataframe["y"] < 10)]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row.value

    def test_compound_inequality_filter_is_filtered(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[(dataframe["x"] >= 0) & (dataframe["y"] < 0)]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row) is None

    def test_contains_on_existing_column(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe['has_column'] = dataframe.contains('x')
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "has_column": True}).value

    def test_contains_on_missing_column(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe['has_column'] = dataframe.contains('wrong_column')
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "has_column": False}).value

    def test_contains_as_filter(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[dataframe.contains('x')]

        valid_row = row_factory({"x": 1, "y": 2})
        valid_result = dataframe.process(valid_row)
        assert valid_result is not None and valid_result.value == valid_row.value

        invalid_row = row_factory({"y": 2})
        assert dataframe.process(invalid_row) is None


class TestDataframeKafka:
    def test_to_topic(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        row_factory,
        topic_json_serdes_factory,
    ):
        topic = topic_json_serdes_factory()
        producer = row_producer_factory()

        dataframe = dataframe_factory()
        dataframe.producer = producer
        dataframe.to_topic(topic)

        assert dataframe.topics_out[topic.name] == topic

        row_to_produce = row_factory(
            topic="ignore_me",
            key=b"test_key",
            value={"x": "1", "y": "2"},
        )

        with producer:
            dataframe.process(row_to_produce)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == topic.name
        assert row_to_produce.key == consumed_row.key
        assert row_to_produce.value == consumed_row.value

    def test_to_topic_custom_key(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        row_factory,
        topic_json_serdes_factory,
    ):
        topic = topic_json_serdes_factory()
        producer = row_producer_factory()

        dataframe = dataframe_factory()
        dataframe.producer = producer
        # Using value of "x" column as a new key
        dataframe.to_topic(topic, key=lambda value, ctx: value["x"])

        row_to_produce = row_factory(
            topic=topic.name,
            key=b"test_key",
            value={"x": "1", "y": "2"},
        )

        with producer:
            dataframe.process(row_to_produce)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            consumed_row = consumer.poll_row(timeout=5.0)

        assert consumed_row
        assert consumed_row.topic == topic.name
        assert consumed_row.value == row_to_produce.value
        assert consumed_row.key == row_to_produce.value["x"].encode()

    def test_to_topic_multiple_topics_out(
        self,
        dataframe_factory,
        row_consumer_factory,
        row_producer_factory,
        row_factory,
        topic_json_serdes_factory,
    ):
        topic_0 = topic_json_serdes_factory()
        topic_1 = topic_json_serdes_factory()
        producer = row_producer_factory()

        dataframe = dataframe_factory()
        dataframe.producer = producer

        dataframe.to_topic(topic_0)
        dataframe.to_topic(topic_1)

        row_to_produce = row_factory(
            key=b"test_key",
            value={"x": "1", "y": "2"},
        )

        with producer:
            dataframe.process(row_to_produce)

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
            assert row_to_produce.key == consumed_row.key
            assert row_to_produce.value == consumed_row.value

    def test_to_topic_no_producer_assigned(self, dataframe_factory, row_factory):
        topic = Topic("whatever")

        dataframe = dataframe_factory()
        dataframe.to_topic(topic)

        with pytest.raises(RuntimeError):
            dataframe.process(
                row_factory(
                    topic=topic.name, key=b"test_key", value={"x": "1", "y": "2"}
                )
            )


class TestDataframeStateful:
    def test_apply_stateful(self, dataframe_factory, state_manager, row_factory):
        topic = Topic("test")

        def stateful_func(value, ctx, state: State):
            current_max = state.get("max")
            if current_max is None:
                current_max = value["number"]
            else:
                current_max = max(current_max, value["number"])
            state.set("max", current_max)
            value["max"] = current_max

        sdf = dataframe_factory(topic, state_manager=state_manager)
        sdf.apply(stateful_func, stateful=True)

        state_manager.on_partition_assign(
            tp=TopicPartitionStub(topic=topic.name, partition=0)
        )
        rows = [
            row_factory(topic=topic.name, value={"number": 1}),
            row_factory(topic=topic.name, value={"number": 10}),
            row_factory(topic=topic.name, value={"number": 3}),
        ]
        result = None
        for row in rows:
            with state_manager.start_store_transaction(
                topic=row.topic, partition=row.partition, offset=row.offset
            ):
                result = sdf.process(row)

        assert result
        assert result.value["max"] == 10
