import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

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
        app.dataframe(topic=input_topic).to_topic(output_topic).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        app.run(count=1)
        assert list_sink == values_in[:1]
        app.run(count=2)
        assert list_sink == values_in[:3]

    def test_timeout(
        self,
        app_factory,
        row_consumer_factory,
        caplog,
    ):
        """
        Timeout resets if a message was processed, else stops app.
        """
        timeout = 1.0
        app = app_factory(
            auto_offset_reset="earliest",
            # force tighter polling for faster and more accurate timeout assessments
            consumer_poll_timeout=timeout * 0.3,
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
        # extended process times (time.sleep) don't affect the timeout
        app.dataframe(topic=input_topic).update(
            lambda _: time.sleep(timeout * 1.1)
        ).to_topic(output_topic).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in[:2]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        app.run(timeout=timeout)
        assert list_sink == values_in[:2]

    def test_timeout_or_count(
        self,
        app_factory,
        row_consumer_factory,
        caplog,
    ):
        """
        Passing timeout or count together each trigger as expected.
        """
        timeout = 1.0

        app = app_factory(
            auto_offset_reset="earliest",
            # force tighter polling for faster and more accurate timeout assessments
            consumer_poll_timeout=timeout * 0.3,
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
        app.dataframe(topic=input_topic).to_topic(output_topic).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        with caplog.at_level("INFO"):
            # gets 1 message (hits timeout)
            app.run(count=2, timeout=timeout)
            assert f"Timeout of {timeout}s reached" in caplog.text
            assert list_sink == values_in[:1]
            caplog.clear()

        with app.get_producer() as producer:
            for value in values_in[1:]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        with caplog.at_level("INFO"):
            # gets 1 message (hits timeout and count, but timeout takes priority)
            app.run(count=2, timeout=timeout)
            assert "Count of 2 records reached" in caplog.text
            assert list_sink == values_in[:3]

    def test_count_excludes_repartition(
        self,
        app_factory,
        row_consumer_factory,
        executor,
        caplog,
    ):
        """
        Count-based stopping does not track/count repartition (groupby) messages.
        """
        timeout = 1.0
        app = app_factory(
            auto_offset_reset="earliest",
            # force tighter polling for faster and more accurate timeout assessments
            consumer_poll_timeout=timeout * 0.3,
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
        list_sink_pre_gb = ListSink()
        list_sink_post_gb = ListSink()
        sdf = app.dataframe(topic=input_topic)
        sdf.sink(list_sink_pre_gb)
        sdf.group_by(key=lambda v: str(v["x"]), name="gb").to_topic(output_topic).sink(
            list_sink_post_gb
        )

        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        # count does not include groupby messages for triggering stop
        with caplog.at_level("INFO"):
            app.run(count=2, timeout=timeout)
            assert f"Timeout of {timeout}s reached" in caplog.text
            assert list_sink_pre_gb == values_in[:1]
            assert list_sink_post_gb == values_in[:1]

    def test_count_flushes_repartition(
        self,
        app_factory,
        row_consumer_factory,
        executor,
        caplog,
    ):
        """
        Count-based stopping flushes any underlying repartition/groupby
        messages before shutting down.
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
        list_sink_pre_gb = ListSink()
        list_sink_post_gb = ListSink()
        sdf = app.dataframe(topic=input_topic)
        sdf.sink(list_sink_pre_gb)
        sdf.group_by(key=lambda v: str(v["x"]), name="gb").to_topic(output_topic).sink(
            list_sink_post_gb
        )

        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        # count condition immediately triggers on first message, but downstream
        # groupby should still get processed.
        with caplog.at_level("INFO"):
            app.run(count=1)
            assert "Count of 1 records reached" in caplog.text
            assert list_sink_pre_gb == values_in[:1]
            assert list_sink_post_gb == values_in[:1]

    def test_timeout_resets_after_recovery(
        self,
        app_factory,
        row_consumer_factory,
        executor,
    ):
        """
        Timeout is set only after recovery is complete
        """
        timeout = 1.0

        app = app_factory(
            auto_offset_reset="earliest",
            # force tighter polling for faster and more accurate timeout assessments
            consumer_poll_timeout=timeout * 0.3,
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
        app.dataframe(topic=input_topic).update(
            lambda v, state: state.set("blah", 1), stateful=True
        ).to_topic(output_topic).sink(list_sink)

        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)
        app.run(count=1)

        # force a recovery
        app.clear_state()

        # set up a delayed recovery and produce
        original_do_recovery = RecoveryManager.do_recovery
        producer = app.get_producer()

        def produce_on_delay(app_producer):
            # produce just on the cusp of the expected timeout
            time.sleep(timeout * 0.8)
            with app_producer as producer:
                for value in values_in[1:]:
                    msg = input_topic.serialize(key="some_key", value=value)
                    producer.produce(input_topic.name, key=msg.key, value=msg.value)

        def sleep_recovery(self):
            original_do_recovery(self)
            # sleep to ensure that if timeout was for some reason not set
            # after recovery, this test would fail by not consuming the messages
            # produced after the delay.
            time.sleep(timeout)
            executor.submit(produce_on_delay, producer)

        with patch(
            "quixstreams.state.recovery.RecoveryManager.do_recovery", new=sleep_recovery
        ):
            app.run(timeout=timeout)
        assert list_sink == values_in
