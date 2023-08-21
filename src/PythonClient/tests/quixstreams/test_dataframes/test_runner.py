from concurrent.futures import Future

from src.quixstreams.dataframes import StreamingDataFrame, Topic
from src.quixstreams.dataframes.models import DoubleDeserializer
from src.quixstreams.dataframes.models import JSONDeserializer
from src.quixstreams.dataframes.models import SerializationError
from src.quixstreams.dataframes.rowconsumer import KafkaMessageError


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

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggerred if processing fails
            nonlocal processed_count, runner

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                runner.stop()

        # Setup a runner and run it in background thread
        runner = runner_factory(
            auto_offset_reset="earliest", on_message_processed=on_message_processed
        )
        fut = executor.submit(runner.run, df)

        fut.result(timeout=10.0)
        # Check that all messages have been processed
        assert processed_count == total_messages

        # TODO: Check the watermarks

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
        runner = runner_factory(auto_offset_reset="error")
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
        runner = runner_factory(auto_offset_reset="earliest")
        fut: Future = executor.submit(runner.run, df)
        # Wait for exception to fail
        exc = fut.exception(timeout=10.0)
        # Ensure the type of the exception
        assert isinstance(exc, SerializationError)

    def test_run_consumer_error_suppressed(
        self, runner_factory, producer, topic_factory, consumer, executor
    ):
        topic_name, _ = topic_factory()
        topic = Topic(
            topic_name, value_deserializer=JSONDeserializer(column_name="root")
        )
        df = StreamingDataFrame(topics=[topic])

        failed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, KafkaMessageError)
            failed.set_result(True)
            runner.stop()
            return True

        # Launch a Runner in a background thread
        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        runner = runner_factory(
            auto_offset_reset="error",
            on_consumer_error=on_error,
        )
        completed = executor.submit(runner.run, df)

        assert failed.result(10.0)
        assert completed.exception(10.0) is None

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

        # Launch a Runner in a background thread
        runner = runner_factory(auto_offset_reset="earliest")
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

        # Produce a string while double is expected
        with producer:
            producer.produce(topic=topic_name, value=b'{"field":"value"}')

        failed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, ValueError)
            failed.set_result(True)
            runner.stop()
            return True

        # Launch a Runner in a background thread
        runner = runner_factory(
            auto_offset_reset="earliest", on_processing_error=on_error
        )
        completed = executor.submit(runner.run, df)

        assert failed.result(10.0)
        assert completed.exception(10.0) is None

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
