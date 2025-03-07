import time
import uuid
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition

from quixstreams.runtracker import RunTracker
from quixstreams.sinks.core.list import ListSink
from quixstreams.state.recovery import RecoveryManager


class TestRunTracker:
    @pytest.mark.parametrize(
        "conditions, should_have_stopper",
        [
            ({}, False),
            ({"timeout": -1.0}, False),
            ({"timeout": 0.0}, False),
            ({"timeout": 1.0}, True),
            ({"count": -1}, False),
            ({"count": 0}, False),
            ({"count": 1}, True),
            ({"timeout": 0.0, "count": 0}, False),
            ({"timeout": -1.0, "count": -1}, False),
            ({"timeout": -1.0, "count": 1}, True),
            ({"timeout": 1.0, "count": -1}, True),
            ({"timeout": 1.0, "count": 1}, True),
        ],
    )
    def test_stop_condition_detected(self, caplog, conditions, should_have_stopper):
        run_tracker = RunTracker(MagicMock())
        with caplog.at_level("INFO"):
            run_tracker.set_stop_condition(**conditions)
        condition_detected = "APP STOP CONDITIONS SET" in caplog.text
        assert should_have_stopper is condition_detected

    def test_count(
        self,
        app_factory,
        row_consumer_factory,
    ):
        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(lambda x: time.sleep(0.01)).to_topic(
            output_topic
        ).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        app.run(count=1)
        assert list_sink == values_in[:1]
        app.run(count=2)
        assert list_sink == values_in[:3]

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)

    def test_timeout(
        self,
        app_factory,
        row_consumer_factory,
    ):
        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        # timeout large enough for at least one message to be consumed
        timeout = 0.1

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(lambda x: time.sleep(timeout)).to_topic(
            output_topic
        ).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        app.run(timeout=timeout)
        assert list_sink == values_in[:1]
        app.run(timeout=timeout * 2)
        assert list_sink == values_in[:3]

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)

    def test_count_repartition_logic(
        self,
        app_factory,
        row_consumer_factory,
        caplog,
    ):
        """
        Count-based stopping does not track/count repartition (groupby) messages
        It does however flush all downstream repartitions when it's the stopping trigger
        """
        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        timeout = 0.1

        list_sink_pre_gb = ListSink()
        list_sink_post_gb = ListSink()
        sdf = app.dataframe(topic=input_topic).update(lambda x: time.sleep(timeout))
        sdf.sink(list_sink_pre_gb)
        sdf.group_by(key=lambda v: str(v["x"]), name="gb").to_topic(output_topic).sink(
            list_sink_post_gb
        )

        # produce 1 message to later show count does not include groupby messages
        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        # force a timeout after first message (don't want to consume the groupby)
        app.run(timeout=timeout)
        assert list_sink_pre_gb == values_in[:1]
        assert list_sink_post_gb == []

        # count does not include groupby messages for triggering stop
        # remember that timeout check occurs before count check
        with caplog.at_level("INFO"):
            # timeout should trigger since count only tracks non-repartition messages
            app.run(count=1, timeout=timeout * 10)
            assert f"Timeout of {timeout}s reached!" in caplog.text
            assert list_sink_pre_gb == values_in[:1]
            assert list_sink_post_gb == values_in[:1]

        with app.get_producer() as producer:
            for value in values_in[1:]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        # count enforces ALL downstream groupby's to be fully flushed
        # when it's the stop trigger (unlike timeout)
        app.run(count=2)
        assert list_sink_pre_gb == values_in[:3]
        assert list_sink_post_gb == values_in[:3]

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink_post_gb)

    def test_multiple_mixed_runs(
        self,
        app_factory,
        row_consumer_factory,
    ):
        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        timeout = 0.1

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(lambda x: time.sleep(timeout)).to_topic(
            output_topic
        ).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        app.run(timeout=timeout)
        app.run(count=2)

        assert list_sink == values_in[:3]

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)

    def test_timeout_after_recovery(
        self,
        app_factory,
        row_consumer_factory,
    ):
        """
        Timeout is set only after recovery is complete
        """

        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        # timeout large enough for at least one message to be consumed
        timeout = 0.1

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(
            lambda v, state: state.set("blah", 1), stateful=True
        ).to_topic(output_topic).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)
        app.run(count=1)

        # force a recovery
        app.clear_state()

        original_do_recovery = RecoveryManager.do_recovery

        def sleep_recovery(self):
            original_do_recovery(self)
            # force a sleep to ensure that if timeout was for some reason not set
            # after recovery, this test would fail by timing out too soon (would only
            # handle 1 message rather than the rest)
            time.sleep(timeout * 2)

        with patch(
            "quixstreams.state.recovery.RecoveryManager.do_recovery", new=sleep_recovery
        ):
            app.run(timeout=timeout)

        assert list_sink == values_in

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)

    def test_timeout_or_count(
        self,
        app_factory,
        row_consumer_factory,
        caplog,
    ):
        """
        Timeout is set only after recovery is complete
        """

        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        # timeout large enough for at least one message to be consumed
        timeout = 0.1

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(lambda v: time.sleep(timeout)).to_topic(
            output_topic
        ).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        with caplog.at_level("INFO"):
            # gets 1 message (hits timeout)
            app.run(count=3, timeout=timeout)
            assert f"Timeout of {timeout}s reached!" in caplog.text
            assert list_sink == values_in[:1]
            caplog.clear()

        with caplog.at_level("INFO"):
            # gets 1 message (hits timeout and count, but timeout takes priority)
            app.run(count=1, timeout=timeout)
            assert f"Timeout of {timeout}s reached!" in caplog.text
            assert list_sink == values_in[:2]
            caplog.clear()

        with caplog.at_level("INFO"):
            # gets 1 message (hits count)
            app.run(count=1, timeout=5)
            assert f"Count of {1} records reached!" in caplog.text
            assert list_sink == values_in[:3]

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)

    def test_timeout_buffer_waits_for_first_msg(
        self,
        app_factory,
        row_consumer_factory,
        executor,
    ):
        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        timeout = 0.001
        buffer = 3.0

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(lambda x: time.sleep(timeout)).to_topic(
            output_topic
        ).sink(list_sink)

        def produce_on_delay(app_producer):
            time.sleep(buffer)
            with app_producer as producer:
                for value in values_in:
                    msg = input_topic.serialize(key="some_key", value=value)
                    producer.produce(input_topic.name, key=msg.key, value=msg.value)

        producer = app.get_producer()
        executor.submit(produce_on_delay, producer)
        time_start = time.monotonic()
        app.run(timeout=timeout, timeout_wait_buffer=buffer * 2)
        time_end = time.monotonic()
        assert list_sink == values_in[:1]
        # confirm the buffer ended from the message arriving rather than timing out
        assert (time_end - time_start) < buffer * 2

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)

    def test_timeout_buffer_exceeded(
        self,
        app_factory,
        row_consumer_factory,
        executor,
    ):
        app = app_factory(
            auto_offset_reset="earliest",
        )
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        timeout = 1.0

        list_sink = ListSink()
        app.dataframe(topic=input_topic).update(lambda x: time.sleep(timeout)).to_topic(
            output_topic
        ).sink(list_sink)

        def produce_on_delay(app_producer):
            time.sleep(timeout * 1.5)
            with app_producer as producer:
                for value in values_in:
                    msg = input_topic.serialize(key="some_key", value=value)
                    producer.produce(input_topic.name, key=msg.key, value=msg.value)

        producer = app.get_producer()
        executor.submit(produce_on_delay, producer)
        time_start = time.monotonic()
        app.run(timeout=timeout, timeout_wait_buffer=timeout)
        time_end = time.monotonic()
        assert list_sink == values_in[:1]
        # confirm the buffer reached the end
        # (reminder: the sdf.update sleeps for "timeout" time)
        assert (time_end - time_start) > timeout * 2

        with app.get_consumer() as consumer:
            assert consumer.get_watermark_offsets(TopicPartition(output_topic.name, 0))[
                1
            ] == len(list_sink)
