from concurrent.futures import Future
from unittest.mock import patch

import pytest

from src.quixstreams.dataframes import StreamingDataFrame, Topic
from src.quixstreams.dataframes.models import DoubleDeserializer
from src.quixstreams.dataframes.models import JSONDeserializer
from src.quixstreams.dataframes.models import SerializationError
from src.quixstreams.dataframes.rowconsumer import KafkaMessageError, RowConsumer
from src.quixstreams.dataframes.runner import RunnerNotStarted


class TestRunner:
    def test_run_success(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        """
        Test that StreamingDataFrame processes 3 messages from Kafka and commits
        the offsets
        """
        topic_name, _ = topic_factory()
        topic = Topic(
            topic_name, value_deserializer=JSONDeserializer(column_name="root")
        )
        df = StreamingDataFrame(topics=[topic])
        processed_count = 0
        total_messages = 3
        # Produce messages to the topic and flush
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_name, key=b"key", value=b'"value"')

        done = Future()

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggerred if processing fails
            nonlocal processed_count, runner

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        # Setup a runner and run it in background thread
        with runner_factory(
            auto_offset_reset="earliest", on_message_processed=on_message_processed
        ) as runner:
            executor.submit(runner.run, df)

            done.result(timeout=10.0)
            # Check that all messages have been processed
            assert processed_count == total_messages

    def test_run_consumer_error_raised(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(
            topic_name, value_deserializer=JSONDeserializer(column_name="root")
        )
        df = StreamingDataFrame(topics=[topic])

        # Launch a Runner in a background thread
        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        with runner_factory(auto_offset_reset="error") as runner:
            fut: Future = executor.submit(runner.run, df)
            # Wait for exception to fail
            exc = fut.exception(timeout=10.0)
            # Ensure the type of the exception
            assert isinstance(exc, KafkaMessageError)

    def test_run_deserialization_error_raised(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(topic_name, value_deserializer=DoubleDeserializer())

        # Produce a string while double is expected
        with producer:
            producer.produce(topic=topic_name, value=b"abc")

        df = StreamingDataFrame(topics=[topic])
        # Launch a Runner in a background thread
        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        with runner_factory(auto_offset_reset="earliest") as runner:
            fut: Future = executor.submit(runner.run, df)
            # Wait for exception to fail
            exc = fut.exception(timeout=10.0)
            # Ensure the type of the exception
            assert isinstance(exc, SerializationError)

    def test_run_consumer_error_suppressed(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(topic_name, value_deserializer=JSONDeserializer())
        df = StreamingDataFrame(topics=[topic])

        done = Future()
        consumed = 0

        def on_error(exc, *args):
            nonlocal consumed
            assert isinstance(exc, ValueError)
            consumed += 1
            if consumed > 1:
                done.set_result(True)
            return True

        # Launch a Runner in a background thread
        # Patch RowConsumer.poll to simulate failures
        with runner_factory(on_consumer_error=on_error) as runner, patch.object(
            RowConsumer, "poll"
        ) as mocked:
            mocked.side_effect = ValueError("test")
            executor.submit(runner.run, df)

            assert done.result(10.0)
            assert consumed > 1

    def test_run_processing_error_raised(
        self, topic_factory, producer, runner_factory, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(topic_name, value_deserializer=JSONDeserializer())
        df = StreamingDataFrame(topics=[topic])

        def fail(*args):
            raise ValueError("test")

        df = df.apply(fail)

        # Produce a string while double is expected
        with producer:
            producer.produce(topic=topic_name, value=b'{"field":"value"}')

        with runner_factory(auto_offset_reset="earliest") as runner:
            # Launch a Runner in a background thread
            fut: Future = executor.submit(runner.run, df)
            # Wait for exception to fail
            exc = fut.exception(timeout=10.0)
            # Ensure the type of the exception
            assert isinstance(exc, ValueError)

    def test_run_processing_error_suppressed(
        self, topic_factory, producer, runner_factory, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(topic_name, value_deserializer=JSONDeserializer())
        df = StreamingDataFrame(topics=[topic])

        def fail(*args):
            raise ValueError("test")

        df = df.apply(fail)

        produced = 2
        consumed = 0
        done = Future()

        with producer:
            for i in range(produced):
                producer.produce(topic=topic_name, value=b'{"field":"value"}')

        def on_error(exc, *args):
            nonlocal consumed
            assert isinstance(exc, ValueError)
            consumed += 1
            if consumed == produced:
                done.set_result(True)
            return True

        # Launch a Runner in a background thread
        with runner_factory(
            auto_offset_reset="earliest", on_processing_error=on_error
        ) as runner:
            executor.submit(runner.run, df)
            assert done.result(10.0)
            assert produced == consumed

    def test_run_runner_isnot_started(self, runner_factory):
        topic = Topic("abc", value_deserializer=JSONDeserializer())
        df = StreamingDataFrame(topics=[topic])
        runner = runner_factory()
        with pytest.raises(RunnerNotStarted):
            runner.run(df)

    def test_run_producer_error_raised(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        # TODO: Waiting for .sink() to be implemented
        ...

    def test_run_serialization_error_raised(self):
        # TODO: Waiting for .sink() to be implemented
        ...

    def test_run_producer_error_suppressed(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        # TODO: Waiting for .sink() to be implemented
        ...
