from concurrent.futures import Future
from json import loads, dumps
from unittest.mock import patch

import pytest
from confluent_kafka import KafkaException

from src.quixstreams.dataframes import StreamingDataFrame, Topic
from src.quixstreams.dataframes.models import DoubleDeserializer, DoubleSerializer
from src.quixstreams.dataframes.models import JSONDeserializer
from src.quixstreams.dataframes.models import SerializationError
from src.quixstreams.dataframes.rowconsumer import KafkaMessageError, RowConsumer
from src.quixstreams.dataframes.runner import RunnerNotStarted


class TestRunner:
    def test_run_consume_and_produce(
        self, runner_factory, producer, topic_factory, topic_json_serdes_factory,
            row_consumer_factory, executor, producable_row_factory
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
        data = {'key': b"key", 'value': b'"value"'}
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_name, **data)

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

        # confirm messages actually ended up being produced by the runner
        rows_out = []
        with row_consumer_factory(auto_offset_reset='earliest') as row_consumer:
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
        self, runner_factory, producer, topic_json_serdes_factory, executor
    ):

        topic_in = topic_json_serdes_factory()
        topic_out = topic_json_serdes_factory()

        df = StreamingDataFrame(topics=[topic_in])
        df.to_topic(topic_out)

        with producer:
            producer.produce(topic_in.name, dumps({"field": 1001 * "a"}))

        with runner_factory(
            auto_offset_reset='earliest',
            producer_extra_config={"message.max.bytes": 1000}
        ) as runner:
            fut: Future = executor.submit(runner.run, df)
            # Wait for exception to fail
            exc = fut.exception(timeout=10.0)
            # Ensure the type of the exception
            assert isinstance(exc, KafkaException)

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

        with runner_factory(auto_offset_reset='earliest') as runner:
            fut: Future = executor.submit(runner.run, df)
            # Wait for exception to fail
            exc = fut.exception(timeout=10.0)
            # Ensure the type of the exception
            assert isinstance(exc, SerializationError)

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
            auto_offset_reset='earliest', on_producer_error=on_error
        ) as runner:
            executor.submit(runner.run, df)
            done.result(timeout=10.0)
        assert produce_output_attempts == produce_input