import pytest

from streamingdataframes.dataframe.pipeline import Pipeline
from streamingdataframes.models.topics import Topic


class TestDataframe:
    def test_dataframe(self, dataframe_factory):
        dataframe = dataframe_factory()
        assert isinstance(dataframe._pipeline, Pipeline)
        assert dataframe._pipeline.id == dataframe.id


class TestDataframeProcess:
    def test_apply(self, dataframe_factory, row_factory, row_plus_n_func):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(row_plus_n_func(1))
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row_factory({"x": 2, "y": 3}).value

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
        dataframe["new"] = dataframe["x"].apply(lambda v: v + 5)
        row = row_factory({"x": 1})
        assert dataframe.process(row).value == row_factory({"x": 1, "new": 6}).value

    def test_setitem_column_with_operations(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe["new"] = dataframe["x"] + dataframe["y"].apply(lambda v: v + 5) + 1
        row = row_factory({"x": 1, "y": 2})
        expected = row_factory({"x": 1, "y": 2, "new": 9})
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
        dataframe = dataframe[dataframe["x"].apply(lambda v: v - 1) >= 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row.value

    def test_inequality_filtering_with_apply_is_filtered(
        self, dataframe_factory, row_factory
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe[dataframe["x"].apply(lambda v: v - 10) >= 0]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row) is None

    def test_compound_inequality_filter(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe[(dataframe["x"] >= 0) & (dataframe["y"] < 10)]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row).value == row.value

    def test_compound_inequality_filter_is_filtered(
        self, dataframe_factory, row_factory
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe[(dataframe["x"] >= 0) & (dataframe["y"] < 0)]
        row = row_factory({"x": 1, "y": 2})
        assert dataframe.process(row) is None

    @pytest.mark.skip("This should fail based on our outline but currently does not")
    # TODO: make this fail correctly
    def test_non_row_apply_breaks_things(self, dataframe_factory, row_factory):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(lambda row: False)
        dataframe = dataframe.apply(lambda row: row)
        row = row_factory({"x": 1, "y": 2})
        dataframe.process(row)

    def test_multiple_row_generation(
        self, dataframe_factory, more_rows_func, row_factory
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(more_rows_func)
        expected = [row_factory({"x": 1, "x_list": i}) for i in range(3)]
        actual = dataframe.process(row_factory({"x": 1, "x_list": [0, 1, 2]}))
        assert len(actual) == len(expected)
        for idx in range(len(actual)):
            assert actual[idx].value == expected[idx].value

    def test_multiple_row_generation_with_additional_apply(
        self, dataframe_factory, more_rows_func, row_factory, row_plus_n_func
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(row_plus_n_func(n=1))
        expected = [row_factory({"x": 2, "x_list": i + 1}) for i in range(3)]
        actual = dataframe.process(row_factory({"x": 1, "x_list": [0, 1, 2]}))
        assert len(actual) == len(expected)
        for idx in range(len(actual)):
            assert actual[idx].value == expected[idx].value

    def test_multiple_row_generation_with_additional_filtering(
        self, dataframe_factory, more_rows_func, row_factory
    ):
        dataframe = dataframe_factory()
        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(lambda row: row if row["x_list"] > 0 else None)
        expected = [row_factory({"x": 1, "x_list": i}) for i in range(1, 3)]
        actual = dataframe.process(row_factory({"x": 1, "x_list": [0, 1, 2]}))
        assert len(actual) == len(expected)
        for idx in range(len(actual)):
            assert actual[idx].value == expected[idx].value


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
