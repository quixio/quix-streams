import time
from concurrent.futures import Future
from json import loads, dumps
from unittest.mock import patch

import pytest
from confluent_kafka import KafkaException, TopicPartition

from streamingdataframes import StreamingDataFrame, Topic
from streamingdataframes.models import (
    DoubleDeserializer,
    DoubleSerializer,
)
from streamingdataframes.models import JSONDeserializer
from streamingdataframes.models import SerializationError
from streamingdataframes.rowconsumer import (
    KafkaMessageError,
    RowConsumer,
)
from streamingdataframes.runner import RunnerNotStarted, Runner


def _stop_runner_on_future(runner: Runner, future: Future, timeout: float):
    """
    Call "Runner.stop" after the future is resolved to stop the poll loop
    """
    try:
        future.result(timeout)
    finally:
        runner.stop()


def _stop_runner_on_timeout(runner: Runner, timeout: float):
    """
    Call "Runner.stop" after the timeout expires to stop the poll loop
    """
    time.sleep(timeout)
    runner.stop()


class TestRunner:
    def test_run_consume_and_produce(
        self,
        runner_factory,
        producer,
        topic_factory,
        topic_json_serdes_factory,
        row_consumer_factory,
        executor,
        row_factory,
    ):
        """
        Test that StreamingDataFrame processes 3 messages from Kafka by having the
        runner produce the consumed messages verbatim to a new topic, and of course
        committing the respective offsets after handling each message.
        """
        column_name = "root"
        topic_name, _ = topic_factory()
        topic_in = Topic(
            topic_name, value_deserializer=JSONDeserializer(column_name=column_name)
        )
        topic_out = topic_json_serdes_factory()

        df = StreamingDataFrame(topics=[topic_in])
        df.to_topic(topic_out)

        processed_count = 0
        total_messages = 3
        # Produce messages to the topic and flush
        data = {"key": b"key", "value": b'"value"'}
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_name, **data)

        done = Future()

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        with runner_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        ) as runner:
            # Stop runner when the future is resolved
            executor.submit(_stop_runner_on_future, runner, done, 10.0)
            runner.run(df)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure that the right offset is committed
        with row_consumer_factory(auto_offset_reset="latest") as row_consumer:
            committed, *_ = row_consumer.committed([TopicPartition(topic_in.name, 0)])
            assert committed.offset == total_messages

        # confirm messages actually ended up being produced by the runner
        rows_out = []
        with row_consumer_factory(auto_offset_reset="earliest") as row_consumer:
            row_consumer.subscribe([topic_out])
            while len(rows_out) < total_messages:
                rows_out.append(row_consumer.poll_row(timeout=5))

        assert len(rows_out) == total_messages
        for row in rows_out:
            assert row.topic == topic_out.name
            assert row.key == data["key"]
            assert row.value == {column_name: loads(data["value"].decode())}

    def test_run_consumer_error_raised(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(
            topic_name, value_deserializer=JSONDeserializer(column_name="root")
        )
        df = StreamingDataFrame(topics=[topic])

        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        with runner_factory(auto_offset_reset="error") as runner:
            # Stop runner after 10s if nothing failed
            executor.submit(_stop_runner_on_timeout, runner, 10.0)
            with pytest.raises(KafkaMessageError):
                runner.run(df)

    def test_run_deserialization_error_raised(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(topic_name, value_deserializer=DoubleDeserializer())

        # Produce a string while double is expected
        with producer:
            producer.produce(topic=topic_name, value=b"abc")

        df = StreamingDataFrame(topics=[topic])

        with runner_factory(auto_offset_reset="earliest") as runner:
            with pytest.raises(SerializationError):
                # Stop runner after 10s if nothing failed
                executor.submit(_stop_runner_on_timeout, runner, 10.0)
                runner.run(df)

    def test_run_consumer_error_suppressed(
        self, runner_factory, producer, topic_json_serdes_factory, consumer, executor
    ):
        topic = topic_json_serdes_factory()
        df = StreamingDataFrame(topics=[topic])

        done = Future()
        polled = 0

        def on_error(exc, *args):
            nonlocal polled
            assert isinstance(exc, ValueError)
            polled += 1
            if polled > 1 and not done.done():
                done.set_result(True)
            return True

        with runner_factory(on_consumer_error=on_error) as runner, patch.object(
            RowConsumer, "poll"
        ) as mocked:
            # Patch RowConsumer.poll to simulate failures
            mocked.side_effect = ValueError("test")
            # Stop runner when the future is resolved
            executor.submit(_stop_runner_on_future, runner, done, 10.0)
            runner.run(df)
        assert polled > 1

    def test_run_processing_error_raised(
        self, topic_json_serdes_factory, producer, runner_factory, executor
    ):
        topic = topic_json_serdes_factory()
        df = StreamingDataFrame(topics=[topic])

        def fail(*args):
            raise ValueError("test")

        df = df.apply(fail)

        # Produce a string while double is expected
        with producer:
            producer.produce(topic=topic.name, value=b'{"field":"value"}')

        with runner_factory(auto_offset_reset="earliest") as runner:
            with pytest.raises(ValueError):
                executor.submit(_stop_runner_on_timeout, runner, 10.0)
                runner.run(df)

    def test_run_processing_error_suppressed(
        self, topic_json_serdes_factory, producer, runner_factory, executor
    ):
        topic = topic_json_serdes_factory()
        df = StreamingDataFrame(topics=[topic])

        def fail(*args):
            raise ValueError("test")

        df = df.apply(fail)

        produced = 2
        consumed = 0
        done = Future()

        with producer:
            for i in range(produced):
                producer.produce(topic=topic.name, value=b'{"field":"value"}')

        def on_error(exc, *args):
            nonlocal consumed
            assert isinstance(exc, ValueError)
            consumed += 1
            if consumed == produced:
                done.set_result(True)
            return True

        with runner_factory(
            auto_offset_reset="earliest", on_processing_error=on_error
        ) as runner:
            # Stop runner from the background thread when the future is resolved
            executor.submit(_stop_runner_on_future, runner, done, 10.0)
            runner.run(df)
        assert produced == consumed

    def test_run_runner_isnot_started(self, runner_factory):
        topic = Topic("abc", value_deserializer=JSONDeserializer())
        df = StreamingDataFrame(topics=[topic])
        runner = runner_factory()
        with pytest.raises(RunnerNotStarted):
            runner.run(df)

    def test_run_producer_error_raised(
        self, runner_factory, producer, topic_json_serdes_factory, executor
    ):
        topic_in = topic_json_serdes_factory()
        topic_out = topic_json_serdes_factory()

        df = StreamingDataFrame(topics=[topic_in])
        df.to_topic(topic_out)

        with producer:
            producer.produce(topic_in.name, dumps({"field": 1001 * "a"}))

        with runner_factory(
            auto_offset_reset="earliest",
            producer_extra_config={"message.max.bytes": 1000},
        ) as runner:
            with pytest.raises(KafkaException):
                executor.submit(_stop_runner_on_timeout, runner, 10.0)
                runner.run(df)

    def test_run_serialization_error_raised(
        self, runner_factory, producer, topic_factory, executor
    ):
        topic_in_name, _ = topic_factory()
        topic_in = Topic(topic_in_name, value_deserializer=JSONDeserializer())
        topic_out_name, _ = topic_factory()
        topic_out = Topic(topic_out_name, value_serializer=DoubleSerializer())

        df = StreamingDataFrame(topics=[topic_in])
        df.to_topic(topic_out)

        with producer:
            producer.produce(topic_in.name, b'{"field":"value"}')

        with runner_factory(auto_offset_reset="earliest") as runner:
            with pytest.raises(SerializationError):
                executor.submit(_stop_runner_on_timeout, runner, 10.0)
                runner.run(df)

    def test_run_producer_error_suppressed(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_in_name, _ = topic_factory()
        topic_in = Topic(topic_in_name, value_deserializer=JSONDeserializer())
        topic_out_name, _ = topic_factory()
        topic_out = Topic(topic_out_name, value_serializer=DoubleSerializer())

        df = StreamingDataFrame(topics=[topic_in])
        df.to_topic(topic_out)

        produce_input = 2
        produce_output_attempts = 0
        done = Future()

        with producer:
            for _ in range(produce_input):
                producer.produce(topic_in.name, b'{"field":"value"}')

        def on_error(exc, *args):
            nonlocal produce_output_attempts
            assert isinstance(exc, SerializationError)
            produce_output_attempts += 1
            if produce_output_attempts == produce_input:
                done.set_result(True)
            return True

        with runner_factory(
            auto_offset_reset="earliest", on_producer_error=on_error
        ) as runner:
            executor.submit(_stop_runner_on_future, runner, done, 10.0)
            runner.run(df)
        assert produce_output_attempts == produce_input
