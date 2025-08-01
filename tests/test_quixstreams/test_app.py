import contextlib
import os
import time
import uuid
from concurrent.futures import Future
from json import dumps, loads
from pathlib import Path
from unittest.mock import create_autospec, patch

import pytest
from confluent_kafka import KafkaException, TopicPartition

from quixstreams.app import Application
from quixstreams.dataframe import StreamingDataFrame
from quixstreams.dataframe.windows.base import get_window_ranges
from quixstreams.exceptions import PartitionAssignmentError
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.kafka.exceptions import KafkaConsumerException
from quixstreams.models import (
    DoubleDeserializer,
    DoubleSerializer,
    JSONDeserializer,
    JSONSerializer,
    SerializationError,
    TopicConfig,
)
from quixstreams.models.topics.exceptions import TopicNotFoundError
from quixstreams.platforms.quix import QuixApplicationConfig, QuixKafkaConfigsBuilder
from quixstreams.platforms.quix.env import QuixEnvironment
from quixstreams.sinks import SinkBackpressureError, SinkBatch
from quixstreams.sources import SourceException, multiprocessing
from quixstreams.state import RecoveryManager, State
from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.rocksdb import RocksDBStore
from tests.utils import DummySink, DummySource, DummyStatefulSource


def _stop_app_on_future(app: Application, future: Future, timeout: float):
    """
    Call "Application.stop" after the future is resolved to stop the poll loop
    """
    try:
        future.result(timeout)
    finally:
        app.stop()


def _stop_app_on_timeout(app: Application, timeout: float):
    """
    Call "Application.stop" after the timeout expires to stop the poll loop
    """
    time.sleep(timeout)
    app.stop()


class TestApplication:
    def test_produce_and_consume(self, app_factory, topic_factory):
        """
        Test that the basic producer can produce messages to a Kafka topic and the consumer
        can consume them.
        """
        total_messages = 3
        consumer_timeout_seconds = 10
        messages_to_produce = [
            {"key": f"key-{i}", "value": f"value-{i}"} for i in range(total_messages)
        ]

        app = app_factory(auto_offset_reset="earliest")
        topic_name, _ = topic_factory()

        # Produce messages
        with app.get_producer() as producer:
            for msg in messages_to_produce:
                producer.produce(
                    topic_name,
                    key=msg["key"].encode(),
                    value=msg["value"].encode(),
                )
            producer.flush()

        # Consume messages
        consumed_messages = []
        start_time = time.time()
        with app.get_consumer() as consumer:
            consumer.subscribe([topic_name])
            while (
                len(consumed_messages) < total_messages
                and time.time() - start_time < consumer_timeout_seconds
            ):
                msg = consumer.poll(timeout=5.0)
                if msg is not None and not msg.error():
                    consumed_messages.append(
                        {"key": msg.key().decode(), "value": msg.value().decode()}
                    )

        # Check that all messages have been produced and consumed correctly
        assert len(consumed_messages) == total_messages
        for msg in consumed_messages:
            assert msg in messages_to_produce

    def test_run_success(
        self,
        app_factory,
        internal_consumer_factory,
        executor,
    ):
        """
        Test that StreamingDataFrame processes 3 messages from Kafka by having the
        app produce the consumed messages verbatim to a new topic, and of course
        committing the respective offsets after handling each message.
        """

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        )

        partition_num = 0
        topic_in = app.topic(
            str(uuid.uuid4()),
            value_deserializer=JSONDeserializer(),
        )
        topic_out = app.topic(
            str(uuid.uuid4()),
            value_serializer=JSONSerializer(),
            value_deserializer=JSONDeserializer(),
        )
        sdf = app.dataframe(topic_in)
        sdf = sdf.to_topic(topic_out)

        processed_count = 0
        total_messages = 3
        # Produce messages to the topic and flush
        timestamp_ms = int(time.time() / 1000)
        headers = [("header", b"value")]
        data = {
            "key": b"key",
            "value": b'"value"',
            "partition": partition_num,
            "timestamp": timestamp_ms,
            "headers": headers,
        }
        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(topic_in.name, **data)

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 15.0)
        app.run(sdf)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure that the right offset is committed
        with internal_consumer_factory(auto_offset_reset="latest") as internal_consumer:
            committed, *_ = internal_consumer.committed(
                [TopicPartition(topic_in.name, partition_num)]
            )
            assert committed.offset == total_messages

        # confirm messages actually ended up being produced by the app
        rows_out = []
        with internal_consumer_factory(
            auto_offset_reset="earliest"
        ) as internal_consumer:
            internal_consumer.subscribe([topic_out])
            while len(rows_out) < total_messages:
                rows_out.append(internal_consumer.poll_row(timeout=5))

        assert len(rows_out) == total_messages
        for row in rows_out:
            assert row.topic == topic_out.name
            assert row.key == data["key"]
            assert row.value == loads(data["value"].decode())
            assert row.timestamp == timestamp_ms
            assert row.headers == headers

    def test_run_fails_no_commit(
        self,
        app_factory,
        internal_consumer_factory,
        executor,
    ):
        """
        Test that Application doesn't commit the checkpoint in case of failure
        """

        app = app_factory(
            auto_offset_reset="earliest",
            commit_interval=9999,  # Set a high commit interval to ensure no autocommit
        )

        partition_num = 0
        topic_in = app.topic(str(uuid.uuid4()))

        def count_and_fail(_):
            # Count the incoming messages and fail on processing the last one
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                raise ValueError("test")

        sdf = app.dataframe(topic_in).apply(count_and_fail)

        processed_count = 0
        total_messages = 3
        # Produce messages to the topic and flush
        data = {"key": b"key", "value": b'"value"', "partition": partition_num}
        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(topic_in.name, **data)

        failed = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, failed, 10.0)
        with pytest.raises(ValueError):
            app.run(sdf)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure the offset is not committed to Kafka
        with internal_consumer_factory() as internal_consumer:
            committed, *_ = internal_consumer.committed(
                [TopicPartition(topic_in.name, partition_num)]
            )
        assert committed.offset == -1001

    def test_run_consumer_error_raised(self, app_factory, executor):
        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        app = app_factory(auto_offset_reset="error")
        topic = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())
        sdf = app.dataframe(topic)

        # Stop app after 10s if nothing failed
        executor.submit(_stop_app_on_timeout, app, 10.0)
        with pytest.raises(KafkaConsumerException):
            app.run(sdf)

    def test_run_deserialization_error_raised(self, app_factory, executor):
        app = app_factory(auto_offset_reset="earliest")
        topic = app.topic(str(uuid.uuid4()), value_deserializer=DoubleDeserializer())

        # Produce a string while double is expected
        with app.get_producer() as producer:
            producer.produce(topic=topic.name, value=b"abc")

        sdf = app.dataframe(topic)

        with pytest.raises(SerializationError):
            # Stop app after 10s if nothing failed
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(sdf)

    def test_run_consumer_error_suppressed(self, app_factory, executor):
        done = Future()
        polled = 0

        def on_consumer_error(exc, *args):
            nonlocal polled
            assert isinstance(exc, ValueError)
            polled += 1
            if polled > 1 and not done.done():
                done.set_result(True)
            return True

        app = app_factory(on_consumer_error=on_consumer_error)
        topic = app.topic(str(uuid.uuid4()))
        sdf = app.dataframe(topic)

        with patch.object(InternalConsumer, "poll") as mocked:
            # Patch InternalConsumer.poll to simulate failures
            mocked.side_effect = ValueError("test")
            # Stop app when the future is resolved
            executor.submit(_stop_app_on_future, app, done, 10.0)
            app.run(sdf)
        assert polled > 1

    def test_run_processing_error_raised(self, app_factory, executor):
        app = app_factory(auto_offset_reset="earliest")

        topic = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())
        sdf = app.dataframe(topic)

        def fail(*args):
            raise ValueError("test")

        sdf = sdf.apply(fail)

        with app.get_producer() as producer:
            producer.produce(topic=topic.name, value=b'{"field":"value"}')

        with pytest.raises(ValueError):
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(sdf)

    def test_run_processing_error_suppressed(self, app_factory, executor):
        produced = 2
        consumed = 0
        done = Future()

        def on_processing_error(exc, *args):
            nonlocal consumed
            assert isinstance(exc, ValueError)
            consumed += 1
            if consumed == produced:
                done.set_result(True)
            return True

        app = app_factory(
            auto_offset_reset="earliest", on_processing_error=on_processing_error
        )
        topic = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())
        sdf = app.dataframe(topic)

        def fail(*args):
            raise ValueError("test")

        sdf = sdf.apply(fail)

        with app.get_producer() as producer:
            for i in range(produced):
                producer.produce(topic=topic.name, value=b'{"field":"value"}')

        # Stop app from the background thread when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)
        assert produced == consumed

    def test_run_producer_error_raised(self, app_factory, producer, executor):
        app = app_factory(
            auto_offset_reset="earliest",
            producer_extra_config={"message.max.bytes": 1000},
        )

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())
        topic_out = app.topic(str(uuid.uuid4()), value_serializer=JSONSerializer())

        sdf = app.dataframe(topic_in)
        sdf = sdf.to_topic(topic_out)

        # use separate producer instance which won't share extra_config
        with producer:
            producer.produce(topic_in.name, dumps({"field": 1001 * "a"}))

        with pytest.raises(KafkaException):
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(sdf)

    def test_run_serialization_error_raised(self, app_factory, executor):
        app = app_factory(auto_offset_reset="earliest")

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())
        topic_out = app.topic(str(uuid.uuid4()), value_serializer=DoubleSerializer())

        sdf = app.dataframe(topic_in)
        sdf = sdf.to_topic(topic_out)

        with app.get_producer() as producer:
            producer.produce(topic_in.name, b'{"field":"value"}')

        with pytest.raises(SerializationError):
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(sdf)

    def test_run_producer_error_suppressed(self, app_factory, executor):
        produce_input = 2
        produce_output_attempts = 0
        done = Future()

        def on_producer_error(exc, *args):
            nonlocal produce_output_attempts
            assert isinstance(exc, SerializationError)
            produce_output_attempts += 1
            if produce_output_attempts == produce_input:
                done.set_result(True)
            return True

        app = app_factory(
            auto_offset_reset="earliest", on_producer_error=on_producer_error
        )
        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())
        topic_out = app.topic(str(uuid.uuid4()), value_serializer=DoubleSerializer())

        sdf = app.dataframe(topic_in)
        sdf = sdf.to_topic(topic_out)

        with app.get_producer() as producer:
            for _ in range(produce_input):
                producer.produce(topic_in.name, b'{"field":"value"}')

        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)

        assert produce_output_attempts == produce_input

    def test_topic_init(self, app_factory):
        app = app_factory()
        topic = app.topic(name=str(uuid.uuid4()))
        assert topic

    def test_streamingdataframe_init(self, app_factory):
        app = app_factory()
        topic = app.topic(name=str(uuid.uuid4()))
        sdf = app.dataframe(topic)
        assert isinstance(sdf, StreamingDataFrame)

    def test_topic_auto_create_true(self, app_factory, kafka_admin_client):
        """
        Topics are auto-created when auto_create_topics=True
        """
        app = app_factory(auto_create_topics=True)

        topic = app.topic(name=str(uuid.uuid4()))
        topics = kafka_admin_client.list_topics().topics
        assert topic.name in topics

    def test_topic_auto_create_false(self, app_factory):
        """
        Topics are not auto-created when auto_create_topics=False
        """
        app = app_factory(auto_create_topics=False)

        # Topic is not created by validated
        with pytest.raises(TopicNotFoundError):
            app.topic(name=str(uuid.uuid4()))

    def test_consumer_extra_config(self, app_factory):
        """
        Some configs like `enable.auto.offset.store` are overridable and others are not
        """
        app = app_factory(
            auto_offset_reset="latest",
            consumer_extra_config={
                "auto.offset.reset": "earliest",
                "enable.auto.offset.store": True,
            },
        )

        with app.get_consumer() as x:
            assert x._consumer_config["enable.auto.offset.store"] is True
            assert x._consumer_config["auto.offset.reset"] == "latest"

    def test_producer_extra_config(self, app_factory):
        """
        Test that producer receives the Application extra configs
        """
        app = app_factory(
            producer_extra_config={"linger.ms": 10},
        )

        with app.get_producer() as x:
            assert x._producer_config["linger.ms"] == 10

    @pytest.mark.parametrize(
        ["processing_guarantee", "transactional"],
        [
            ("exactly-once", True),
            ("exactly-once", False),
            ("at-least-once", False),
            ("at-least-once", True),
        ],
    )
    def test_get_producer_transactional(
        self, app_factory, processing_guarantee, transactional
    ):
        app = app_factory(processing_guarantee=processing_guarantee)
        with app.get_producer(transactional=transactional) as producer:
            transactional_id = producer._producer_config.get("transactional.id")
            assert bool(transactional_id) == transactional

    def test_missing_broker_id_raise(self):
        # confirm environment is empty
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as e_info:
                Application()
            error_str = 'Either "broker_address" or "quix_sdk_token" must be provided'
            assert error_str in e_info.value.args

    def test_consumer_group_env(self):
        """
        Sanity check consumer_group gets set from the environment via getenv.
        """
        with patch.dict(os.environ, {"Quix__Consumer_Group": "environ_group"}):
            app = Application(broker_address="my_address")
        assert app.config.consumer_group == "environ_group"

        with patch.dict(os.environ, {"Quix__Consumer_Group": "environ_group"}):
            app = Application(broker_address="my_address", consumer_group="app_group")
        assert app.config.consumer_group == "app_group"

    def test_consumer_group_default(self):
        """
        Sanity check behavior around getenv defaults
        """
        with patch.dict(os.environ, {}, clear=True):
            app = Application(broker_address="my_address")
        assert app.config.consumer_group == "quixstreams-default"

    def test_state_dir_env(self):
        """
        Sanity check state_dir gets set from the environment via getenv.
        """
        app = Application(broker_address="my_address")
        assert app.config.state_dir == Path("state")

        with patch.dict(os.environ, {"Quix__State__Dir": "/path/to/state"}):
            app = Application(broker_address="my_address")
        assert app.config.state_dir == Path("/path/to/state")

        with patch.dict(os.environ, {"Quix__State__Dir": "/path/to/state"}):
            app = Application(broker_address="my_address", state_dir="/path/to/other")
        assert app.config.state_dir == Path("/path/to/other")


@pytest.mark.parametrize("number_of_partitions", [1, 2])
class TestAppGroupBy:
    def test_group_by(
        self,
        app_factory,
        internal_consumer_factory,
        executor,
        number_of_partitions,
    ):
        """
        Test that StreamingDataFrame processes 6 messages from Kafka and groups them
        by each record's specified column value.
        """

        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        processed_count = 0

        timestamp_ms = int(time.time() * 1000)
        user_id = "abc123"
        value_in = {"user": user_id}

        if number_of_partitions == 1:
            total_messages = 1  # groupby optimisation for 1 partition
        else:
            total_messages = 2  # groupby reproduces each message

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        )

        app_topic_in = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
            config=TopicConfig(
                num_partitions=number_of_partitions, replication_factor=1
            ),
        )
        app_topic_out = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        sdf = app.dataframe(topic=app_topic_in)
        sdf = sdf.group_by("user")
        # Capture original message timestamp to ensure it's forwarded
        # to the repartition topic
        sdf["groupby_timestamp"] = sdf.apply(
            lambda value, key, timestamp_, headers: timestamp_, metadata=True
        )
        sdf = sdf.to_topic(app_topic_out)

        with app.get_producer() as producer:
            msg = app_topic_in.serialize(
                key="some_key", value=value_in, timestamp_ms=timestamp_ms
            )
            producer.produce(
                app_topic_in.name, key=msg.key, value=msg.value, timestamp=msg.timestamp
            )

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Consume the message from the output topic
        with internal_consumer_factory(
            auto_offset_reset="earliest"
        ) as internal_consumer:
            internal_consumer.subscribe([app_topic_out])
            row = internal_consumer.poll_row(timeout=5)

        # Check that "user_id" is now used as a message key
        assert row.key.decode() == user_id
        # Check that message timestamp of the repartitioned message is the same
        # as original one
        assert row.value == {
            "user": user_id,
            "groupby_timestamp": timestamp_ms,
        }

    @pytest.mark.parametrize("processing_guarantee", ["exactly-once", "at-least-once"])
    def test_group_by_with_window(
        self,
        app_factory,
        internal_consumer_factory,
        executor,
        processing_guarantee,
        number_of_partitions,
    ):
        """
        Test that StreamingDataFrame processes 6 messages from Kafka and groups them
        by each record's specified column value.
        """

        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        processed_count = 0

        window_duration_ms = 1000
        timestamp_ms = int(time.time() * 1000)
        # use a "window-friendly" timestamp for easier testing
        timestamp_ms = timestamp_ms - (timestamp_ms % window_duration_ms)
        user_id = "abc123"
        value_in = {"user": user_id}

        if number_of_partitions == 1:
            total_messages = 1  # groupby optimisation for 1 partition
        else:
            total_messages = 2  # groupby reproduces each message

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
            processing_guarantee=processing_guarantee,
        )

        app_topic_in = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
            config=TopicConfig(
                num_partitions=number_of_partitions, replication_factor=1
            ),
        )
        app_topic_out = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        sdf = app.dataframe(topic=app_topic_in)
        sdf = sdf.group_by("user")
        # Capture original message timestamp to ensure it's forwarded
        # to the repartition topic
        sdf["groupby_timestamp"] = sdf.apply(
            lambda value, key, timestamp_, headers: timestamp_, metadata=True
        )
        sdf = sdf.tumbling_window(duration_ms=window_duration_ms).count().current()
        sdf = sdf.to_topic(app_topic_out)

        with app.get_producer() as producer:
            msg = app_topic_in.serialize(
                key="some_key", value=value_in, timestamp_ms=timestamp_ms
            )
            producer.produce(
                app_topic_in.name, key=msg.key, value=msg.value, timestamp=msg.timestamp
            )

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Consume the message from the output topic
        with internal_consumer_factory(
            auto_offset_reset="earliest"
        ) as internal_consumer:
            internal_consumer.subscribe([app_topic_out])
            row = internal_consumer.poll_row(timeout=5)

        # Check that "user_id" is now used as a message key
        assert row.key.decode() == user_id
        # Check that window is calculated based on the original timestamp
        assert row.value == {
            "start": timestamp_ms,
            "end": timestamp_ms + window_duration_ms,
            "value": 1,
        }


class TestAppExactlyOnce:
    def test_exactly_once(
        self,
        app_factory,
        topic_manager_factory,
        internal_consumer_factory,
        executor,
    ):
        """
        An Application that forwards messages to a new topic crashes after producing 2
        messages, then restarts (will reprocess all 3 messages again).

        The second run succeeds in processing all 3 messages and commits transaction.

        The 2 non-committed produces should be ignored by a downstream consumer.
        """
        processed_count = 0
        total_messages = 3
        fail_idx = 1
        done = Future()

        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count
            processed_count += 1
            # Stop processing after consuming all the messages
            # if (processed_count % total_messages) == 0:
            if processed_count == total_messages:
                done.set_result(True)

        class ForceFail(Exception): ...

        def fail_once(value):
            if processed_count == fail_idx:
                # sleep here to ensure produced messages actually make it to topic
                time.sleep(2)
                raise ForceFail
            return value

        consumer_group = str(uuid.uuid4())
        topic_in_name = str(uuid.uuid4())
        topic_out_name = str(uuid.uuid4())

        def get_app(fail: bool):
            app = app_factory(
                commit_interval=30,
                auto_offset_reset="earliest",
                on_message_processed=on_message_processed,
                consumer_group=consumer_group,
                processing_guarantee="exactly-once",
            )
            topic_in = app.topic(topic_in_name, value_deserializer="json")
            topic_out = app.topic(topic_out_name, value_serializer="json")
            sdf = app.dataframe(topic_in)
            sdf = sdf.to_topic(topic_out)
            if fail:
                sdf = sdf.apply(fail_once)
            return app, sdf, topic_in, topic_out

        # first run of app that encounters an error during processing
        app, sdf, topic_in, topic_out = get_app(fail=True)

        # produce initial messages to consume
        with app.get_producer() as producer:
            for i in range(total_messages):
                msg = topic_in.serialize(key=str(i), value={"my_val": str(i)})
                producer.produce(topic=topic_in.name, key=msg.key, value=msg.value)

        with pytest.raises(ForceFail):
            app.run(sdf)
        assert processed_count == fail_idx

        # re-init the app, only this time it won't fail
        processed_count = 0
        app, sdf, topic_in, topic_out = get_app(fail=False)
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)

        # only committed messages are read by a downstream consumer
        with internal_consumer_factory(
            auto_offset_reset="earliest"
        ) as internal_consumer:
            internal_consumer.subscribe([topic_out])
            rows = []
            while (row := internal_consumer.poll_row(timeout=5)) is not None:
                rows.append(row)
            lowwater, highwater = internal_consumer.get_watermark_offsets(
                TopicPartition(topic_out.name, 0), 3
            )
        assert len(rows) == total_messages

        # Sanity check that non-committed messages actually made it to topic
        assert lowwater == 0
        # The first message being at offset 3 affirms the first transaction
        # was successfully aborted, as expected.
        assert rows[0].offset == fail_idx + 2 == 3
        # The last message being at offset 7 affirms the second transaction
        # was successfully committed, as expected.
        assert highwater == rows[-1].offset + 2 == 7


class TestQuixApplication:
    @pytest.mark.parametrize(
        "quix_portal_api_arg, quix_portal_api_expected",
        [
            (None, "https://portal-api.platform.quix.io/"),
            ("http://example.com", "http://example.com"),
        ],
    )
    def test_init_with_quix_sdk_token_arg(
        self, quix_portal_api_arg, quix_portal_api_expected
    ):
        consumer_group = "c_group"
        expected_workspace_cgroup = f"my_ws-{consumer_group}"
        quix_sdk_token = "my_sdk_token"
        quix_extras = {"quix": "extras"}
        extra_config = {"extra": "config"}
        connection_config = ConnectionConfig.from_librdkafka_dict(
            {
                "bootstrap.servers": "address1,address2",
                "sasl.mechanisms": "SCRAM-SHA-256",
                "security.protocol": "SASL_SSL",
                "sasl.username": "my-username",
                "sasl.password": "my-password",
                "ssl.ca.location": "/mock/dir/ca.cert",
            }
        )
        expected_producer_extra_config = {
            "enable.idempotence": True,
            **extra_config,
            **quix_extras,
        }
        expected_consumer_extra_config = {
            **extra_config,
            **quix_extras,
            "fetch.queue.backoff.ms": 100,
            "partition.assignment.strategy": "range",
        }

        # Mock QuixKafkaConfigsBuilder
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg_builder.workspace_id = "abcd"
        cfg_builder.librdkafka_connection_config = connection_config
        cfg_builder.prepend_workspace_id.return_value = expected_workspace_cgroup
        cfg_builder.quix_sdk_token = quix_sdk_token
        cfg_builder.get_application_config.side_effect = lambda cg: (
            QuixApplicationConfig(
                connection_config, quix_extras, cfg_builder.prepend_workspace_id(cg)
            )
        )
        cfg_builder.from_credentials.return_value = cfg_builder

        # Mock consumer and producer to check the init args
        with (
            patch("quixstreams.app.QuixKafkaConfigsBuilder", cfg_builder),
            patch("quixstreams.app.InternalConsumer") as consumer_init_mock,
            patch("quixstreams.app.InternalProducer") as producer_init_mock,
        ):
            app = Application(
                consumer_group=consumer_group,
                quix_sdk_token=quix_sdk_token,
                quix_portal_api=quix_portal_api_arg,
                consumer_extra_config=extra_config,
                producer_extra_config=extra_config,
            )
            assert app.is_quix_app

        # Check that quix_portal_api is passed correctly
        cfg_builder.from_credentials.assert_called_with(
            quix_sdk_token=quix_sdk_token, quix_portal_api=quix_portal_api_expected
        )

        # Check if items from the Quix config have been passed
        # to the low-level configs of producer and consumer
        producer_call_kwargs = producer_init_mock.call_args.kwargs
        assert producer_call_kwargs["broker_address"] == connection_config
        assert producer_call_kwargs["extra_config"] == expected_producer_extra_config

        consumer_call_kwargs = consumer_init_mock.call_args.kwargs
        assert consumer_call_kwargs["broker_address"] == connection_config
        assert consumer_call_kwargs["consumer_group"] == expected_workspace_cgroup
        assert consumer_call_kwargs["extra_config"] == expected_consumer_extra_config

    @pytest.mark.parametrize(
        "quix_portal_api_arg, quix_portal_api_expected",
        [
            ("", "https://portal-api.platform.quix.io/"),
            ("http://example.com", "http://example.com"),
        ],
    )
    def test_init_with_quix_sdk_token_env(
        self, monkeypatch, quix_portal_api_arg, quix_portal_api_expected
    ):
        consumer_group = "c_group"
        expected_workspace_cgroup = f"my_ws-{consumer_group}"
        quix_sdk_token = "my_sdk_token"
        extra_config = {"extra": "config"}
        quix_extras = {"quix": "extras"}
        connection_config = ConnectionConfig.from_librdkafka_dict(
            {
                "bootstrap.servers": "address1,address2",
                "sasl.mechanisms": "SCRAM-SHA-256",
                "security.protocol": "SASL_SSL",
                "sasl.username": "my-username",
                "sasl.password": "my-password",
                "ssl.ca.location": "/mock/dir/ca.cert",
            }
        )
        expected_producer_extra_config = {
            "enable.idempotence": True,
            **extra_config,
            **quix_extras,
        }
        expected_consumer_extra_config = {
            **extra_config,
            **quix_extras,
            "fetch.queue.backoff.ms": 100,
            "partition.assignment.strategy": "range",
        }

        # Mock QuixKafkaConfigsBuilder
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg_builder.workspace_id = "abcd"
        cfg_builder.librdkafka_connection_config = connection_config
        cfg_builder.prepend_workspace_id.return_value = expected_workspace_cgroup
        cfg_builder.quix_sdk_token = quix_sdk_token
        cfg_builder.get_application_config.side_effect = lambda cg: (
            QuixApplicationConfig(
                connection_config, quix_extras, cfg_builder.prepend_workspace_id(cg)
            )
        )
        cfg_builder.from_credentials.return_value = cfg_builder

        monkeypatch.setenv("Quix__Sdk__Token", quix_sdk_token)
        monkeypatch.setenv("Quix__Portal__Api", quix_portal_api_arg)
        with (
            patch("quixstreams.app.QuixKafkaConfigsBuilder", cfg_builder),
            patch("quixstreams.app.InternalConsumer") as consumer_init_mock,
            patch("quixstreams.app.InternalProducer") as producer_init_mock,
        ):
            Application(
                consumer_group=consumer_group,
                consumer_extra_config=extra_config,
                producer_extra_config=extra_config,
            )

        # Check that quix_portal_api is passed correctly
        cfg_builder.from_credentials.assert_called_with(
            quix_sdk_token=quix_sdk_token, quix_portal_api=quix_portal_api_expected
        )

        # Check if items from the Quix config have been passed
        # to the low-level configs of producer and consumer
        producer_call_kwargs = producer_init_mock.call_args.kwargs
        assert producer_call_kwargs["broker_address"] == connection_config
        assert producer_call_kwargs["extra_config"] == expected_producer_extra_config

        consumer_call_kwargs = consumer_init_mock.call_args.kwargs
        assert consumer_call_kwargs["broker_address"] == connection_config
        assert consumer_call_kwargs["consumer_group"] == expected_workspace_cgroup
        assert consumer_call_kwargs["extra_config"] == expected_consumer_extra_config

    def test_init_with_quix_config_builder(self):
        consumer_group = "c_group"
        expected_workspace_cgroup = f"my_ws-{consumer_group}"
        quix_sdk_token = "my_sdk_token"
        extra_config = {"extra": "config"}
        quix_extras = {"quix": "extras"}
        connection_config = ConnectionConfig.from_librdkafka_dict(
            {
                "bootstrap.servers": "address1,address2",
                "sasl.mechanisms": "SCRAM-SHA-256",
                "security.protocol": "SASL_SSL",
                "sasl.username": "my-username",
                "sasl.password": "my-password",
                "ssl.ca.location": "/mock/dir/ca.cert",
            }
        )
        expected_producer_extra_config = {
            "enable.idempotence": True,
            **extra_config,
            **quix_extras,
        }
        expected_consumer_extra_config = {
            **extra_config,
            **quix_extras,
            "fetch.queue.backoff.ms": 100,
            "partition.assignment.strategy": "range",
        }

        def get_cfg_builder(quix_sdk_token):
            cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
            cfg_builder.workspace_id = "abcd"
            cfg_builder.librdkafka_connection_config = connection_config
            cfg_builder.prepend_workspace_id.return_value = expected_workspace_cgroup
            cfg_builder.quix_sdk_token = quix_sdk_token
            cfg_builder.get_application_config.side_effect = lambda cg: (
                QuixApplicationConfig(
                    connection_config, quix_extras, cfg_builder.prepend_workspace_id(cg)
                )
            )
            return cfg_builder

        with (
            patch("quixstreams.app.InternalConsumer") as consumer_init_mock,
            patch("quixstreams.app.InternalProducer") as producer_init_mock,
        ):
            Application(
                consumer_group=consumer_group,
                quix_config_builder=get_cfg_builder(quix_sdk_token),
                consumer_extra_config=extra_config,
                producer_extra_config=extra_config,
            )

        # Check if items from the Quix config have been passed
        # to the low-level configs of producer and consumer
        producer_call_kwargs = producer_init_mock.call_args.kwargs
        assert producer_call_kwargs["broker_address"] == connection_config
        assert producer_call_kwargs["extra_config"] == expected_producer_extra_config

        consumer_call_kwargs = consumer_init_mock.call_args.kwargs
        assert consumer_call_kwargs["broker_address"] == connection_config
        assert consumer_call_kwargs["consumer_group"] == expected_workspace_cgroup
        assert consumer_call_kwargs["extra_config"] == expected_consumer_extra_config

    def test_init_with_broker_id_dont_raises(self):
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg_builder.workspace_id = "abcd"

        app = Application(
            broker_address="address",
            quix_config_builder=cfg_builder,
        )

        assert not app.is_quix_app

    def test_topic_name_and_config(
        self,
        quix_app_factory,
        quix_topic_manager_factory,
        quix_mock_config_builder_factory,
    ):
        """
        Topic names created with Quix API have workspace id prefixed
        Topic config has provided values else defaults
        """
        workspace_id = "my-workspace"
        cfg_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = quix_topic_manager_factory(
            workspace_id=workspace_id, quix_config_builder=cfg_builder
        )
        app = quix_app_factory(
            workspace_id=workspace_id,
            topic_manager=topic_manager,
            quix_config_builder=cfg_builder,
        )
        initial_topic_name = "input_topic"
        topic_partitions = 5
        topic = app.topic(
            initial_topic_name,
            config=topic_manager.topic_config(num_partitions=topic_partitions),
        )
        topic_id = f"{workspace_id}-{initial_topic_name}"
        expected_topic = topic_manager.topics[topic_id]
        assert topic.name == topic_id
        assert (
            expected_topic.broker_config.replication_factor
            == topic_manager.default_replication_factor
        )
        assert expected_topic.broker_config.num_partitions == topic_partitions

    def test_transactional_id_prefixed_with_workspace_id(
        self,
        quix_app_factory,
        quix_topic_manager_factory,
        quix_mock_config_builder_factory,
    ):
        workspace_id = "my-workspace"
        cfg_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = quix_topic_manager_factory(
            workspace_id=workspace_id, quix_config_builder=cfg_builder
        )
        app = quix_app_factory(
            workspace_id=workspace_id,
            topic_manager=topic_manager,
            quix_config_builder=cfg_builder,
            processing_guarantee="exactly-once",
        )
        assert app._config.producer_extra_config["transactional.id"].startswith(
            workspace_id
        )


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestQuixApplicationWithState:
    def test_quix_app_no_state_management_warning(
        self, quix_app_factory, monkeypatch, executor
    ):
        """
        Ensure that Application.run() prints a warning if the app is stateful,
        runs on Quix (the "Quix__Deployment__Id" env var is set),
        but the "State Management" flag is disabled for the deployment.
        """
        app = quix_app_factory()
        topic = app.topic(str(uuid.uuid4()))
        sdf = app.dataframe(topic)
        sdf = sdf.apply(lambda x, state: x, stateful=True)

        monkeypatch.setenv(
            QuixEnvironment.DEPLOYMENT_ID,
            "123",
        )
        monkeypatch.setenv(
            QuixEnvironment.STATE_MANAGEMENT_ENABLED,
            "",
        )

        with pytest.warns(RuntimeWarning) as warned:
            executor.submit(_stop_app_on_timeout, app, 5.0)
            app.run()

        warnings = [w for w in warned.list if w.category is RuntimeWarning]
        warning = str(warnings[0].message)
        assert "State Management feature is disabled" in warning

    def test_quix_app_state_dir_mismatch_warning(
        self, quix_app_factory, monkeypatch, caplog
    ):
        """
        Ensure that Application.Quix() logs a warning
        if the app runs on Quix (the "Quix__Deployment__Id" env var is set),
        but the "state_dir" path doesn't match the one on Quix.
        """
        monkeypatch.setenv(
            QuixEnvironment.DEPLOYMENT_ID,
            "123",
        )
        with pytest.warns(RuntimeWarning) as warned:
            quix_app_factory()
        warnings = [w for w in warned.list if w.category is RuntimeWarning]
        warning = str(warnings[0].message)
        assert "does not match the state directory" in warning


@pytest.mark.parametrize(
    ["deployment_id", "expected_state_dir"],
    [
        ("123", "/app/state"),
        ("", "state"),
    ],
)
def test_quix_app_state_dir_default(
    monkeypatch, quix_mock_config_builder_factory, deployment_id, expected_state_dir
):
    monkeypatch.setenv(QuixEnvironment.DEPLOYMENT_ID, deployment_id)
    app = Application(quix_config_builder=quix_mock_config_builder_factory())
    assert app._config.state_dir == Path(expected_state_dir)


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestApplicationWithState:
    def _validate_state(
        self,
        stores,
        stream_id,
        partition_index,
        state_manager_factory,
        consumer_group,
        state_dir,
        validator,
    ):
        store = stores[stream_id]
        partition = store.partitions[partition_index]
        with partition.begin() as tx:
            validator(tx)

        store.revoke_partition(partition_index)

        if isinstance(store, RocksDBStore):
            # Check that the values are actually in the DB
            state_manager = state_manager_factory(
                group_id=consumer_group, state_dir=state_dir
            )
            state_manager.register_store(stream_id, "default")
            state_manager.on_partition_assign(
                stream_id=stream_id,
                partition=partition_index,
                committed_offsets={stream_id: -1001},
            )
            store = state_manager.get_store(stream_id=stream_id, store_name="default")
            with store.start_partition_transaction(partition=partition_index) as tx:
                validator(tx)

    def test_run_stateful_success(
        self,
        app_factory,
        executor,
        state_manager_factory,
        tmp_path,
    ):
        """
        Test that StreamingDataFrame processes 3 messages from Kafka and updates
        the counter in the state store
        """

        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        partition_num = 0
        app = app_factory(
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
        )

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())

        # Define a function that counts incoming Rows using state
        def count(_, state: State):
            total = state.get("total", 0)
            total += 1
            state.set("total", total)
            if total == total_messages:
                total_consumed.set_result(total)

        sdf = app.dataframe(topic_in)
        sdf = sdf.update(count, stateful=True)

        total_messages = 3
        # Produce messages to the topic and flush
        message_key = b"key"
        data = {
            "key": message_key,
            "value": dumps({"key": "value"}),
            "partition": partition_num,
        }
        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(topic_in.name, **data)

        total_consumed = Future()

        stores = {}

        def revoke_partition(store, partition):
            stores[store.stream_id] = store

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, total_consumed, 15.0)
        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run(sdf)

        def validate_state(tx):
            assert tx.get("total", prefix=message_key) == total_consumed.result(
                timeout=5
            )

        self._validate_state(
            stores,
            sdf.stream_id,
            partition_num,
            state_manager_factory,
            consumer_group,
            state_dir,
            validate_state,
        )

    def test_run_stateful_fails_no_commit(
        self,
        app_factory,
        executor,
        state_manager_factory,
        tmp_path,
    ):
        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        app = app_factory(
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
            commit_interval=9999,  # Set a high commit interval to ensure no autocommit
        )

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())

        # Define a function that counts incoming Rows using state
        def count_and_fail(_, state: State):
            total = state.get("total", 0)
            total += 1
            state.set("total", total)
            # Fail after processing all messages
            if total == total_messages:
                raise ValueError("test")

        failed = Future()

        sdf = app.dataframe(topic_in).update(count_and_fail, stateful=True)

        total_messages = 3
        # Produce messages to the topic and flush
        key = b"key"
        value = dumps({"key": "value"})

        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(topic_in.name, key=key, value=value)

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, failed, 10.0)
        with pytest.raises(ValueError):
            app.run(sdf)

        # Ensure that nothing was committed to the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(sdf.stream_id, "default")
        state_manager.on_partition_assign(
            stream_id=sdf.stream_id,
            partition=0,
            committed_offsets={},
        )
        store = state_manager.get_store(stream_id=sdf.stream_id, store_name="default")
        with store.start_partition_transaction(partition=0) as tx:
            assert tx.get("total", prefix=key) is None

    def test_run_stateful_suppress_processing_errors(
        self,
        app_factory,
        executor,
        state_manager_factory,
        tmp_path,
    ):
        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        partition_num = 0
        app = app_factory(
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
            # Suppress errors during message processing
            on_processing_error=lambda exc, *args: True
            if isinstance(exc, ValueError)
            else False,
        )

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())

        # Define a function that counts incoming Rows using state
        def count(_, state: State):
            total = state.get("total", 0)
            total += 1
            state.set("total", total)
            if total == total_messages:
                total_consumed.set_result(total)

        def fail(_):
            raise ValueError("test")

        sdf = app.dataframe(topic_in)
        sdf.update(count, stateful=True).apply(fail)

        total_messages = 3
        message_key = b"key"
        # Produce messages to the topic and flush
        data = {
            "key": message_key,
            "value": dumps({"key": "value"}),
            "partition": partition_num,
        }

        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(topic_in.name, **data)

        total_consumed = Future()

        stores = {}

        def revoke_partition(store, partition):
            stores[store.stream_id] = store

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, total_consumed, 10.0)
        # Run the application
        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run()

        def validate_state(tx):
            assert tx.get("total", prefix=message_key) == total_consumed.result(
                timeout=5
            )

        self._validate_state(
            stores,
            sdf.stream_id,
            partition_num,
            state_manager_factory,
            consumer_group,
            state_dir,
            validate_state,
        )

    def test_clear_state(
        self,
        app_factory,
        producer,
        topic_factory,
        executor,
        state_manager_factory,
        tmp_path,
    ):
        """
        Test that clear_state() removes all data from the state stores
        """

        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        app = app_factory(
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
        )

        topic_in_name, _ = topic_factory()
        prefix = b"key"

        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )

        # Add data to the state store
        with state_manager:
            state_manager.register_store(topic_in_name, "default")
            state_manager.on_partition_assign(
                stream_id=topic_in_name,
                partition=0,
                committed_offsets={topic_in_name: -1001},
            )
            store = state_manager.get_store(
                stream_id=topic_in_name, store_name="default"
            )
            with store.start_partition_transaction(partition=0) as tx:
                # All keys in state must be prefixed with the message key
                tx.set(key="my_state", value=True, prefix=prefix)

        # Clear the state
        app.clear_state()

        # Check that the date is cleared from the state store
        with state_manager:
            state_manager.register_store(topic_in_name, "default")
            state_manager.on_partition_assign(
                stream_id=topic_in_name,
                partition=0,
                committed_offsets={topic_in_name: -1001},
            )
            store = state_manager.get_store(
                stream_id=topic_in_name, store_name="default"
            )
            with store.start_partition_transaction(partition=0) as tx:
                assert tx.get("my_state", prefix=prefix) is None

    def test_app_use_changelog_false(self, app_factory):
        """
        `Application`s StateStoreManager should not have a TopicManager if
        use_changelog_topics is set to `False`.
        """
        app = app_factory(use_changelog_topics=False)
        assert not app._state_manager.using_changelogs


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestApplicationRecovery:
    def test_changelog_recovery_default_store(
        self,
        app_factory,
        executor,
        tmp_path,
        state_manager_factory,
        store_type,
    ):
        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        topic_name = str(uuid.uuid4())
        sum_key = "my_sum"
        store_name = "default"
        msg_int_value = 10
        processed_count = {0: 0, 1: 0}
        partition_msg_count = {0: 4, 1: 2}

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            processed_count[partition] += 1
            if processed_count == partition_msg_count:
                done.set_result(True)

        def sum_value(value: dict, state: State):
            new_value = state.get(sum_key, 0) + value["my_value"]
            state.set(sum_key, new_value)
            return new_value

        def get_app():
            app = app_factory(
                commit_interval=0,  # Commit every processed message
                auto_offset_reset="earliest",
                use_changelog_topics=True,
                on_message_processed=on_message_processed,
                consumer_group=consumer_group,
                state_dir=state_dir,
            )
            topic = app.topic(
                topic_name,
                config=TopicConfig(
                    num_partitions=len(partition_msg_count), replication_factor=1
                ),
            )
            sdf = app.dataframe(topic)
            sdf = sdf.apply(sum_value, stateful=True)
            return app, sdf, topic

        def _validate_transaction_state(tx, partition, count):
            # All keys in state must be pre,fixed with the message key
            prefix = f"key{partition}".encode()
            assert tx.get(sum_key, prefix=prefix) == count * msg_int_value

        def validate_state(stores):
            for p_num, count in partition_msg_count.items():
                store = stores[sdf.stream_id]
                partition = store.partitions[p_num]
                assert partition.get_changelog_offset() == count - 1
                with partition.begin() as tx:
                    _validate_transaction_state(tx, p_num, count)
                store.revoke_partition(p_num)

            if store_type == RocksDBStore:
                with state_manager_factory(
                    group_id=consumer_group,
                    state_dir=state_dir,
                ) as state_manager:
                    state_manager.register_store(sdf.stream_id, store_name)
                    for p_num, count in partition_msg_count.items():
                        state_manager.on_partition_assign(
                            stream_id=sdf.stream_id,
                            partition=p_num,
                            committed_offsets={topic.name: -1001},
                        )
                        store = state_manager.get_store(
                            stream_id=sdf.stream_id, store_name=store_name
                        )
                        partition = store.partitions[p_num]
                        assert partition.get_changelog_offset() == count - 1
                        with partition.begin() as tx:
                            _validate_transaction_state(tx, p_num, count)

        # Produce messages to the topic and flush
        app, sdf, topic = get_app()
        with app.get_producer() as producer:
            for p_num, count in partition_msg_count.items():
                serialized = topic.serialize(
                    key=f"key{p_num}".encode(), value={"my_value": msg_int_value}
                )
                for _ in range(count):
                    producer.produce(
                        topic.name,
                        key=serialized.key,
                        value=serialized.value,
                        partition=p_num,
                    )

        stores = {}

        def revoke_partition(store, partition):
            stores[store.stream_id] = store

        # run app to populate state with data
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run(sdf)

        # validate and then delete the state
        assert processed_count == partition_msg_count
        validate_state(stores)
        app.clear_state()

        # run the app again and validate the recovered state
        processed_count = {0: 0, 1: 0}
        app, sdf, topic = get_app()
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)

        stores = {}
        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run(sdf)

        # no messages should have been processed outside of recovery loop
        assert processed_count == {0: 0, 1: 0}
        # State should be the same as before deletion
        validate_state(stores)

    @pytest.mark.parametrize("processing_guarantee", ["at-least-once", "exactly-once"])
    def test_changelog_recovery_window_store(
        self,
        app_factory,
        executor,
        tmp_path,
        state_manager_factory,
        processing_guarantee,
    ):
        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        topic_name = str(uuid.uuid4())
        store_name = "window"
        window_duration_ms = 5000
        window_step_ms = 2000

        msg_tick_ms = 1000
        msg_int_value = 10

        partition_timestamps = {
            0: list(range(10000, 14000, msg_tick_ms)),
            1: list(range(10000, 12000, msg_tick_ms)),
        }
        partition_windows = {
            p: [
                w
                for ts in ts_list
                for w in get_window_ranges(ts, window_duration_ms, window_step_ms)
            ]
            for p, ts_list in partition_timestamps.items()
        }

        # how many times window updates should occur (1:1 with changelog updates)
        expected_window_updates = {0: {}, 1: {}}
        # expired windows should have no values (changelog updates per tx == num_exp_windows + 1)
        expected_expired_windows = {0: set(), 1: set()}

        for p, windows in partition_windows.items():
            latest_timestamp = partition_timestamps[p][-1]
            for w in windows:
                if latest_timestamp >= w[1]:
                    expected_expired_windows[p].add(w)
                expected_window_updates[p][w] = (
                    expected_window_updates[p].setdefault(w, 0) + 1
                )

        processed_count = {0: 0, 1: 0}
        partition_msg_count = {
            p: len(partition_timestamps[p]) for p in partition_timestamps
        }

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            processed_count[partition] += 1
            if processed_count == partition_msg_count:
                done.set_result(True)

        def get_app():
            app = app_factory(
                commit_interval=0,  # Commit every processed message
                auto_offset_reset="earliest",
                use_changelog_topics=True,
                consumer_group=consumer_group,
                on_message_processed=on_message_processed,
                state_dir=state_dir,
                processing_guarantee=processing_guarantee,
            )
            topic = app.topic(
                topic_name,
                config=TopicConfig(
                    num_partitions=len(partition_msg_count), replication_factor=1
                ),
            )
            # Create a streaming dataframe with a hopping window
            sdf = (
                app.dataframe(topic)
                .apply(lambda row: row["my_value"])
                .hopping_window(
                    duration_ms=window_duration_ms,
                    step_ms=window_step_ms,
                    name=store_name,
                )
                .sum()
                .final()
            )
            return app, sdf, topic

        def validate_state():
            actual_store_name = (
                f"{store_name}_hopping_window_{window_duration_ms}_{window_step_ms}_sum"
            )
            with state_manager_factory(
                group_id=consumer_group, state_dir=state_dir
            ) as state_manager:
                state_manager.register_windowed_store(sdf.stream_id, actual_store_name)
                for p_num, windows in expected_window_updates.items():
                    state_manager.on_partition_assign(
                        stream_id=sdf.stream_id,
                        partition=p_num,
                        committed_offsets={topic.name: -1001},
                    )
                    store = state_manager.get_store(
                        stream_id=sdf.stream_id,
                        store_name=actual_store_name,
                    )

                    # Calculate how many messages should be send to the changelog topic
                    expected_offset = (
                        # A number of total window updates
                        sum(expected_window_updates[p_num].values())
                        # A number of expired windows
                        + 2 * len(expected_expired_windows[p_num])
                        # A number of total timestamps
                        # (each timestamp updates the <LATEST_TIMESTAMPS_CF_NAME>)
                        + len(partition_timestamps[p_num])
                        # Correction for zero-based index
                        - 1
                    )
                    if processing_guarantee == "exactly-once":
                        # In this test, we commit after each message is processed, so
                        # must add PMC-1 to our offset calculation since each kafka
                        # to account for transaction commit markers (except last one)
                        expected_offset += partition_msg_count[p_num] - 1
                    assert (
                        expected_offset
                        == store.partitions[p_num].get_changelog_offset()
                    )

                    partition = store.partitions[p_num]

                    with partition.begin() as tx:
                        prefix = b"key"
                        for window, count in windows.items():
                            expected = count
                            if window in expected_expired_windows[p_num]:
                                expected = None
                            else:
                                # each message value was 10
                                expected *= msg_int_value
                            assert tx.get_window(*window, prefix=prefix) == expected

        app, sdf, topic = get_app()
        # Produce messages to the topic and flush
        with app.get_producer() as producer:
            for p_num, timestamps in partition_timestamps.items():
                serialized = topic.serialize(
                    key=b"key", value={"my_value": msg_int_value}
                )
                for ts in timestamps:
                    producer.produce(
                        topic=topic.name,
                        key=serialized.key,
                        value=serialized.value,
                        partition=p_num,
                        timestamp=ts,
                    )

        # run app to populate state
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run()
        # validate and then delete the state
        assert processed_count == partition_msg_count
        validate_state()

        # run the app again and validate the recovered state
        processed_count = {0: 0, 1: 0}
        app, sdf, topic = get_app()
        app.clear_state()
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run()
        # no messages should have been processed outside of recovery loop
        assert processed_count == {0: 0, 1: 0}
        # State should be the same as before deletion
        validate_state()

    @pytest.mark.parametrize("processing_guarantee", ["at-least-once", "exactly-once"])
    def test_changelog_recovery_consistent_after_failed_commit(
        self,
        store_type,
        app_factory,
        executor,
        tmp_path,
        state_manager_factory,
        internal_consumer_factory,
        processing_guarantee,
    ):
        """
        Scenario: application processes messages and successfully produces changelog
        messages but fails to commit the topic offsets.

        We expect that the app will be recovered to a consistent state and changes
        for the yet uncommitted messages will not be applied.
        """
        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        topic_name = str(uuid.uuid4())
        store_name = "default"

        if processing_guarantee == "exactly-once":
            commit_patch = patch.object(
                InternalProducer, "commit_transaction", side_effect=ValueError("Fail")
            )
        else:
            commit_patch = patch.object(
                InternalConsumer, "commit", side_effect=ValueError("Fail")
            )

        # Messages to be processed successfully
        succeeded_messages = [
            ("key1", "1"),
            ("key2", "2"),
            ("key3", "3"),
        ]
        # Messages to fail
        failed_messages = [
            ("key1", "4"),
            ("key2", "5"),
            ("key3", "6"),
        ]
        # Ensure the same number of messages in both sets to simplify testing
        assert len(failed_messages) == len(succeeded_messages)
        total_count = len(succeeded_messages)
        processed_count = 0

        def on_message_processed(topic_, partition, offset):
            nonlocal processed_count
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            processed_count += 1
            if processed_count == total_count:
                done.set_result(True)

        def get_app():
            app = app_factory(
                commit_interval=999,  # Simulate a very long commit interval
                auto_offset_reset="earliest",
                use_changelog_topics=True,
                on_message_processed=on_message_processed,
                consumer_group=consumer_group,
                state_dir=state_dir,
                processing_guarantee=processing_guarantee,
            )
            topic = app.topic(topic_name)
            sdf = app.dataframe(topic)
            sdf = sdf.update(
                lambda value, state: state.set("latest", value["number"]), stateful=True
            )
            return app, sdf, topic

        def _validate_transaction_state(tx):
            for key, value in succeeded_messages:
                state = tx.as_state(prefix=key.encode())
                assert state.get("latest") == value

        def validate_state(stores):
            store = stores[sdf.stream_id]
            partition = store.partitions[0]
            with partition.begin() as tx:
                _validate_transaction_state(tx)

            store.revoke_partition(0)

            if store_type == RocksDBStore:
                with (
                    state_manager_factory(
                        group_id=consumer_group,
                        state_dir=state_dir,
                    ) as state_manager,
                    internal_consumer_factory(
                        consumer_group=consumer_group
                    ) as consumer,
                ):
                    committed_offset = consumer.committed(
                        [TopicPartition(topic=topic_name, partition=0)]
                    )[0].offset
                    state_manager.register_store(sdf.stream_id, store_name)
                    partition = state_manager.on_partition_assign(
                        stream_id=sdf.stream_id,
                        partition=0,
                        committed_offsets={topic_name: committed_offset},
                    )["default"]
                    with partition.begin() as tx:
                        _validate_transaction_state(tx)

        # Produce messages from the "succeeded" set
        app, sdf, topic = get_app()
        with app.get_producer() as producer:
            for key, value in succeeded_messages:
                serialized = topic.serialize(key=key.encode(), value={"number": value})
                producer.produce(topic.name, key=serialized.key, value=serialized.value)

        stores = {}

        def revoke_partition(store, partition):
            stores[store.stream_id] = store

        # Run the application to apply changes to state
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)

        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run(sdf)

        assert processed_count == total_count
        # Validate the state
        validate_state(stores)

        # Init application again
        processed_count = 0
        app, sdf, topic = get_app()

        # Produce messages from the "failed" set
        with app.get_producer() as producer:
            for key, value in failed_messages:
                serialized = topic.serialize(key=key.encode(), value={"number": value})
                producer.produce(topic.name, key=serialized.key, value=serialized.value)

        # Run the app second time and fail the consumer commit
        with commit_patch:
            done = Future()
            executor.submit(_stop_app_on_future, app, done, 10.0)
            stores = {}
            with patch(
                "quixstreams.state.base.Store.revoke_partition", revoke_partition
            ):
                with contextlib.suppress(PartitionAssignmentError):
                    with pytest.raises(ValueError):
                        app.run(sdf)

        # state should remain the same
        validate_state(stores)

        # Run the app again to recover the state
        app, sdf, topic = get_app()
        # Clear the state to recover from scratch
        app.clear_state()

        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        # Run app for the third time and fail on commit to prevent state changes
        with commit_patch:
            stores = {}
            with patch(
                "quixstreams.state.base.Store.revoke_partition", revoke_partition
            ):
                with contextlib.suppress(PartitionAssignmentError):
                    with pytest.raises(ValueError):
                        app.run(sdf)

        # state should remain the same
        validate_state(stores)


class TestApplicationSink:
    def test_run_with_sink_success(
        self,
        app_factory,
        executor,
    ):
        processed_count = 0
        total_messages = 3

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        )
        sink = DummySink()

        topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="str",
            config=TopicConfig(num_partitions=3, replication_factor=1),
        )
        sdf = app.dataframe(topic)
        sdf.sink(sink)

        key, value, timestamp_ms = b"key", "value", 1000
        headers = [("key", b"value")]

        # Produce messages to different topic partitions and flush
        with app.get_producer() as producer:
            for i in range(total_messages):
                producer.produce(
                    topic=topic.name,
                    partition=i,
                    key=key,
                    value=value,
                    timestamp=timestamp_ms,
                    headers=headers,
                )

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 15.0)
        app.run(sdf)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure all messages were flushed to the sink
        assert len(sink.results) == 3
        for item in sink.results:
            assert item.key == key
            assert item.value == value
            assert item.timestamp == timestamp_ms
            assert item.headers == headers

        # Ensure that the offsets are committed
        with app.get_consumer() as consumer:
            committed0, committed1, committed2 = consumer.committed(
                [
                    TopicPartition(topic=topic.name, partition=0),
                    TopicPartition(topic=topic.name, partition=1),
                    TopicPartition(topic=topic.name, partition=2),
                ]
            )
        assert committed0.offset == 1
        assert committed1.offset == 1
        assert committed2.offset == 1

    def test_run_with_sink_backpressure(
        self,
        app_factory,
        executor,
    ):
        """
        Test that backpressure is handled correctly by the app
        """

        total_messages = 10
        topic_name = str(uuid.uuid4())

        class _BackpressureSink(DummySink):
            _backpressured = False

            def write(self, batch: SinkBatch):
                # Backpressure sink once here to ensure the offset rewind works
                if not self._backpressured:
                    self._backpressured = True
                    raise SinkBackpressureError(retry_after=1)
                return super().write(batch=batch)

        app = app_factory(
            auto_offset_reset="earliest",
            commit_interval=1.0,  # Commit every second
        )
        sink = _BackpressureSink()

        topic = app.topic(
            topic_name,
            value_deserializer="str",
        )
        sdf = app.dataframe(topic)
        sdf.sink(sink)

        key, value, timestamp_ms = b"key", "value", 1000
        headers = [("key", b"value")]

        # Produce messages to different topic partitions and flush
        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(
                    topic=topic.name,
                    key=key,
                    value=value,
                    timestamp=timestamp_ms,
                    headers=headers,
                )

        executor.submit(_stop_app_on_timeout, app, 15.0)
        app.run()

        # Ensure all messages were flushed to the sink
        assert len(sink.results) == total_messages
        for item in sink.results:
            assert item.key == key
            assert item.value == value
            assert item.timestamp == timestamp_ms
            assert item.headers == headers

        # Ensure that the offsets are committed
        with app.get_consumer() as consumer:
            committed, *_ = consumer.committed(
                [TopicPartition(topic=topic.name, partition=0)]
            )
        assert committed.offset == total_messages

    def test_run_with_sink_branches_success(
        self,
        app_factory,
        executor,
    ):
        processed_count = 0
        total_messages = 3

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        )
        sink = DummySink()

        topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="str",
            config=TopicConfig(num_partitions=3, replication_factor=1),
        )
        sdf = app.dataframe(topic)
        sdf = sdf.apply(lambda x: x + "_branch")
        sdf.apply(lambda x: x + "0").sink(sink)
        sdf.apply(lambda x: x + "1").sink(sink)
        sdf = sdf.apply(lambda x: x + "2")
        sdf.sink(sink)

        key, value, timestamp_ms = b"key", "value", 1000
        headers = [("key", b"value")]

        # Produce messages to different topic partitions and flush
        with app.get_producer() as producer:
            for i in range(total_messages):
                producer.produce(
                    topic=topic.name,
                    partition=i,
                    key=key,
                    value=value,
                    timestamp=timestamp_ms,
                    headers=headers,
                )

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 15.0)
        app.run(sdf)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure all messages were flushed to the sink
        assert len(sink.results) == 9
        for i in range(3):
            assert (
                len([r for r in sink.results if f"_branch{i}" in r.value])
                == total_messages
            )
        for item in sink.results:
            assert item.key == key
            assert value in item.value
            assert item.timestamp == timestamp_ms
            assert item.headers == headers

        # Ensure that the offsets are committed
        with app.get_consumer() as consumer:
            committed0, committed1, committed2 = consumer.committed(
                [
                    TopicPartition(topic=topic.name, partition=0),
                    TopicPartition(topic=topic.name, partition=1),
                    TopicPartition(topic=topic.name, partition=2),
                ]
            )
        assert committed0.offset == 1
        assert committed1.offset == 1
        assert committed2.offset == 1


class TestApplicationSource:
    MESSAGES_COUNT = 3

    def wait_finished(self, app, event, timeout=15.0):
        try:
            event.wait(timeout)
        finally:
            app.stop()

    def test_run_with_source_success(
        self, app_factory, executor, topic_manager_factory
    ):
        done = Future()
        processed_count = 0

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == self.MESSAGES_COUNT:
                done.set_result(True)

        topic_manager = topic_manager_factory()
        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
            topic_manager=topic_manager,
        )
        source = DummySource(values=range(self.MESSAGES_COUNT))
        sdf = app.dataframe(source=source)

        default_topic_name = f"source__{source.default_topic().name}"
        assert default_topic_name in topic_manager.topics

        executor.submit(_stop_app_on_future, app, done, 10.0)

        values = []
        sdf = sdf.apply(lambda value: values.append(value))
        app.run(sdf)

        assert values == [0, 1, 2]

    def test_run_source_only(self, app_factory, executor):
        done = multiprocessing.Event()

        topic_name = str(uuid.uuid4())
        app = app_factory(
            auto_offset_reset="earliest",
        )

        source = DummySource(values=range(self.MESSAGES_COUNT), finished=done)
        app.add_source(source, topic=app.topic(topic_name))

        executor.submit(self.wait_finished, app, done, 15.0)

        app.run()

        results = []
        with app.get_consumer() as consumer:
            consumer.subscribe(topics=[topic_name])

            for _ in range(self.MESSAGES_COUNT):
                msg = consumer.poll()
                results.append(msg.value())

        assert results == [b"0", b"1", b"2"]

    @pytest.mark.parametrize(
        "raise_is,exitcode", [("run", 0), ("cleanup", 0), ("stop", -9)]
    )
    @pytest.mark.parametrize("pickleable", [True, False])
    def test_source_with_error(
        self, app_factory, executor, raise_is, exitcode, pickleable
    ):
        done = multiprocessing.Event()

        app = app_factory(
            auto_offset_reset="earliest",
        )

        source = DummySource(
            values=range(self.MESSAGES_COUNT),
            finished=done,
            error_in=raise_is,
            pickeable_error=pickleable,
        )
        sdf = app.dataframe(source=source)

        executor.submit(self.wait_finished, app, done, 15.0)

        # The app stops on source error
        try:
            with pytest.raises(SourceException) as exc:
                app.run(sdf)
        finally:
            # shutdown the thread waiting for exit
            done.set()

        assert exc.value.exitcode == exitcode
        assert exc.value.__cause__
        if pickleable:
            assert isinstance(exc.value.__cause__, ValueError)
        else:
            assert isinstance(exc.value.__cause__, RuntimeError)
        assert str(exc.value.__cause__) == f"test {raise_is} error"

    def test_stateful_source(self, app_factory, executor):
        def _run_app(source, done):
            app = app_factory(
                auto_offset_reset="earliest",
            )

            sdf = app.dataframe(source=source)
            executor.submit(self.wait_finished, app, done, 15.0)

            # The app stops on source error
            try:
                app.run(sdf)
            finally:
                # shutdown the thread waiting for exit
                done.set()

        source_name = str(uuid.uuid4())
        finished = multiprocessing.Event()
        source = DummyStatefulSource(
            name=source_name,
            values=range(self.MESSAGES_COUNT),
            finished=finished,
            state_key="test",
        )
        _run_app(source, finished)

        finished = multiprocessing.Event()
        source = DummyStatefulSource(
            name=source_name,
            values=range(self.MESSAGES_COUNT),
            finished=finished,
            state_key="test",
            assert_state_value=self.MESSAGES_COUNT - 1,
        )
        _run_app(source, finished)


class TestApplicationMultipleSdf:
    def test_multiple_sdfs(
        self,
        app_factory,
        internal_consumer_factory,
        executor,
    ):
        """
        Test that StreamingDataFrame processes 3 messages from Kafka by having the
        app produce the consumed messages verbatim to a new topic, and of course
        committing the respective offsets after handling each message.
        """

        def on_message_processed(topic_, partition, offset):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        )

        partition_num = 0
        topic_a = app.topic(
            str(uuid.uuid4()),
            value_deserializer=JSONDeserializer(),
        )
        topic_b = app.topic(
            str(uuid.uuid4()),
            value_deserializer=JSONDeserializer(),
        )
        input_topics = [topic_a, topic_b]
        topic_out = app.topic(
            str(uuid.uuid4()),
            value_serializer=JSONSerializer(),
            value_deserializer=JSONDeserializer(),
        )
        sdf_a = app.dataframe(topic_a)
        sdf_a.to_topic(topic_out)

        sdf_b = app.dataframe(topic_b)
        sdf_b.to_topic(topic_out)

        processed_count = 0
        messages_per_topic = 3
        total_messages = messages_per_topic * len(input_topics)
        # Produce messages to the topic and flush
        timestamp_ms = int(time.time() / 1000)
        headers = [("header", b"value")]
        data = {
            "key": b"key",
            "value": b'"value"',
            "partition": partition_num,
            "timestamp": timestamp_ms,
            "headers": headers,
        }
        with app.get_producer() as producer:
            for topic in input_topics:
                for _ in range(messages_per_topic):
                    producer.produce(topic.name, **data)

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 15.0)
        app.run()

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure that the right offset is committed
        with internal_consumer_factory(auto_offset_reset="latest") as internal_consumer:
            committed = internal_consumer.committed(
                [TopicPartition(topic.name, partition_num) for topic in input_topics]
            )
            for topic in committed:
                assert topic.offset == messages_per_topic

        # confirm messages actually ended up being produced by the app
        rows_out = []
        with internal_consumer_factory(
            auto_offset_reset="earliest"
        ) as internal_consumer:
            internal_consumer.subscribe([topic_out])
            while len(rows_out) < total_messages:
                rows_out.append(internal_consumer.poll_row(timeout=5))

        assert len(rows_out) == total_messages
        for row in rows_out:
            assert row.topic == topic_out.name
            assert row.key == data["key"]
            assert row.value == loads(data["value"].decode())
            assert row.timestamp == timestamp_ms
            assert row.headers == headers

    @pytest.mark.parametrize("number_of_partitions", [1, 2])
    def test_group_by(
        self, app_factory, internal_consumer_factory, executor, number_of_partitions
    ):
        """
        Test that StreamingDataFrame processes 6 messages from Kafka and groups them
        by each record's specified column value.
        """

        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == expected_processed:
                done.set_result(True)

        processed_count = 0

        app = app_factory(
            auto_offset_reset="earliest",
            on_message_processed=on_message_processed,
        )
        input_topic_a = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
            config=TopicConfig(
                num_partitions=number_of_partitions, replication_factor=1
            ),
        )
        input_topic_b = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
            config=TopicConfig(
                num_partitions=number_of_partitions, replication_factor=1
            ),
        )
        input_topics = [input_topic_a, input_topic_b]
        output_topic_user = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        output_topic_account = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        timestamp = 1000
        user_id = "abc123"
        account_id = "def456"
        value_in = {"user": user_id, "account": account_id}

        if number_of_partitions == 1:
            # expected_processed = 1 (input msg per SDF) * 1 (2 optimized groupbys that don't reprocesses input) * 2 SDFs
            expected_processed = 2
        else:
            # expected_processed = 1 (input msg per SDF) * 3 (2 groupbys, each reprocesses input) * 2 SDFs
            expected_processed = 6

        expected_output_topic_count = 2

        sdf_a = app.dataframe(topic=input_topic_a)
        sdf_a_user = sdf_a.group_by("user")
        sdf_a_user["groupby_timestamp"] = sdf_a_user.apply(
            lambda value, key, timestamp_, headers: timestamp_, metadata=True
        )
        sdf_a_user.to_topic(output_topic_user)

        sdf_a_account = sdf_a.group_by("account")
        sdf_a_account["groupby_timestamp"] = sdf_a_account.apply(
            lambda value, key, timestamp_, headers: timestamp_, metadata=True
        )
        sdf_a_account.to_topic(output_topic_account)

        sdf_b = app.dataframe(topic=input_topic_b)
        sdf_b_user = sdf_b.group_by("user")
        sdf_b_user["groupby_timestamp"] = sdf_b_user.apply(
            lambda value, key, timestamp_, headers: timestamp_, metadata=True
        )
        sdf_b_user.to_topic(output_topic_user)

        sdf_b_account = sdf_b.group_by("account")
        sdf_b_account["groupby_timestamp"] = sdf_b_account.apply(
            lambda value, key, timestamp_, headers: timestamp_, metadata=True
        )
        sdf_b_account.to_topic(output_topic_account)

        with app.get_producer() as producer:
            for topic in input_topics:
                msg = topic.serialize(
                    key="some_key", value=value_in, timestamp_ms=timestamp
                )
                producer.produce(
                    topic.name, key=msg.key, value=msg.value, timestamp=msg.timestamp
                )

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run()

        # Check that all messages have been processed
        assert processed_count == expected_processed

        # Consume the message from the output topic
        for key, output_topic in [
            (user_id, output_topic_user),
            (account_id, output_topic_account),
        ]:
            rows = []
            with internal_consumer_factory(
                auto_offset_reset="earliest"
            ) as internal_consumer:
                internal_consumer.subscribe([output_topic])
                while row := internal_consumer.poll_row(timeout=5):
                    rows.append(row)

            assert len(rows) == expected_output_topic_count
            for row in rows:
                # Check that "user_id" is now used as a message key
                assert row.key.decode() == key
                # Check that message timestamp of the repartitioned message is the same
                # as original one
                assert row.value == {
                    **value_in,
                    "groupby_timestamp": timestamp,
                }

    def _validate_state(
        self,
        stream_ids,
        stores,
        partition_num,
        message_key,
        messages_per_topic,
        state_manager_factory,
        consumer_group,
        state_dir,
    ):
        for stream_id in stream_ids:
            store = stores[stream_id]
            partition = store.partitions[partition_num]
            with partition.begin() as tx:
                assert tx.get("total", prefix=message_key) == messages_per_topic
            store.revoke_partition(partition_num)

            # on disk state only exist for RocksDBStore
            if isinstance(store, RocksDBStore):
                # Check that the values are actually in the DB
                state_manager = state_manager_factory(
                    group_id=consumer_group, state_dir=state_dir
                )
                state_manager.register_store(stream_id, "default")
                state_manager.on_partition_assign(
                    stream_id=stream_id,
                    partition=partition_num,
                    committed_offsets={},
                )
                store = state_manager.get_store(
                    stream_id=stream_id, store_name="default"
                )
                with store.start_partition_transaction(partition=partition_num) as tx:
                    # All keys in state must be prefixed with the message key
                    assert tx.get("total", prefix=message_key) == messages_per_topic

    @pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
    def test_stateful(
        self,
        app_factory,
        executor,
        state_manager_factory,
        tmp_path,
    ):
        """
        Test that StreamingDataFrame processes 3 messages from Kafka and updates
        the counter in the state store
        """

        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        processed_count = 0

        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        partition_num = 0
        app = app_factory(
            commit_interval=0,
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
            on_message_processed=on_message_processed,
            use_changelog_topics=True,
        )
        input_topic_a = app.topic(
            str(uuid.uuid4()), value_deserializer=JSONDeserializer()
        )
        input_topic_b = app.topic(
            str(uuid.uuid4()), value_deserializer=JSONDeserializer()
        )
        input_topics = [input_topic_a, input_topic_b]
        messages_per_topic = 3
        total_messages = messages_per_topic * len(input_topics)

        # Define a function that counts incoming Rows using state
        def count(_, state: State):
            total = state.get("total", 0)
            total += 1
            state.set("total", total)

        sdf_a = app.dataframe(input_topic_a)
        sdf_a.update(count, stateful=True)

        sdf_b = app.dataframe(input_topic_b)
        sdf_b.update(count, stateful=True)

        # Produce messages to the topic and flush
        message_key = b"key"
        data = {
            "key": message_key,
            "value": dumps({"key": "value"}),
            "partition": partition_num,
        }
        with app.get_producer() as producer:
            for topic in input_topics:
                for _ in range(messages_per_topic):
                    producer.produce(topic.name, **data)

        stores = {}

        def revoke_partition(store, partition):
            stores[store.stream_id] = store

        done = Future()
        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)

        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run()

        assert processed_count == total_messages
        self._validate_state(
            [sdf_a.stream_id, sdf_b.stream_id],
            stores,
            partition_num,
            message_key,
            messages_per_topic,
            state_manager_factory,
            consumer_group,
            state_dir,
        )

    @pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
    def test_changelog_recovery(
        self,
        app_factory,
        executor,
        tmp_path,
        state_manager_factory,
    ):
        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == total_messages:
                done.set_result(True)

        processed_count = 0
        input_topic_a_name = str(uuid.uuid4())
        input_topic_b_name = str(uuid.uuid4())
        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        partition_num = 0
        messages_per_topic = 3
        total_messages = 2 * messages_per_topic  # 2 topics
        message_key = b"key"
        data = {
            "key": message_key,
            "value": dumps({"key": "value"}),
            "partition": partition_num,
        }

        def count(_, state: State):
            total = state.get("total", 0)
            total += 1
            state.set("total", total)

        def produce_messages(app, topics):
            with app.get_producer() as producer:
                for topic in topics:
                    for _ in range(messages_per_topic):
                        producer.produce(topic.name, **data)

        def get_app():
            app = app_factory(
                commit_interval=0,  # Commit every processed message
                use_changelog_topics=True,
                consumer_group=consumer_group,
                auto_offset_reset="earliest",
                state_dir=state_dir,
                on_message_processed=on_message_processed,
            )

            input_topic_a = app.topic(
                input_topic_a_name, value_deserializer=JSONDeserializer()
            )
            input_topic_b = app.topic(
                input_topic_b_name, value_deserializer=JSONDeserializer()
            )
            input_topics = [input_topic_a, input_topic_b]

            sdf_a = app.dataframe(input_topic_a)
            sdf_a.update(count, stateful=True)

            sdf_b = app.dataframe(input_topic_b)
            sdf_b.update(count, stateful=True)

            return app, [sdf_a, sdf_b], input_topics

        # produce messages, then run app until all are processed else timeout
        app, sdf, input_topics = get_app()
        produce_messages(app, input_topics)

        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run()
        assert processed_count == total_messages

        # Clear state, repeat the same produce/run process
        # Should result in 2x the original expected count
        processed_count = 0
        app, sdfs, input_topics = get_app()
        app.clear_state()
        produce_messages(app, input_topics)

        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)

        stores = {}

        def revoke_partition(store, partition):
            stores[store.stream_id] = store

        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run()

        assert processed_count == total_messages

        self._validate_state(
            [sdf.stream_id for sdf in sdfs],
            stores,
            partition_num,
            message_key,
            messages_per_topic * 2,
            state_manager_factory,
            consumer_group,
            state_dir,
        )

    @pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
    def test_concatenated_sdfs_stateful(
        self,
        app_factory,
        executor,
        state_manager_factory,
        tmp_path,
    ):
        def on_message_processed(*_):
            # Set the callback to track total messages processed
            # The callback is not triggered if processing fails
            nonlocal processed_count

            processed_count += 1
            # Stop processing after consuming all the messages
            if processed_count == len(messages):
                done.set_result(True)

        processed_count = 0

        consumer_group = str(uuid.uuid4())
        state_dir = (tmp_path / "state").absolute()
        partition_num = 0
        app = app_factory(
            commit_interval=0,
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
            on_message_processed=on_message_processed,
            use_changelog_topics=True,
        )
        input_topic_a = app.topic(
            str(uuid.uuid4()), value_deserializer=JSONDeserializer()
        )
        input_topic_b = app.topic(
            str(uuid.uuid4()), value_deserializer=JSONDeserializer()
        )

        # Produce messages to the topic and flush
        message_key = b"key"
        messages = [
            (input_topic_a, 10),
            (input_topic_a, 12),
            (input_topic_a, 14),
            (input_topic_b, 11),
            (input_topic_b, 13),
            (input_topic_b, 15),
        ]

        with app.get_producer() as producer:
            for topic, timestamp in messages:
                producer.produce(topic.name, timestamp=timestamp, key=message_key)

        # Define a function that counts incoming Rows using state
        def count(value, key, timestamp, headers, state: State):
            timestamps = state.get("timestamps", [])
            timestamps.append(timestamp)
            state.set("timestamps", timestamps)

        sdf_a = app.dataframe(input_topic_a)
        sdf_b = app.dataframe(input_topic_b)

        sdf_concat = sdf_a.concat(sdf_b)
        sdf_concat.update(count, stateful=True, metadata=True)

        done = Future()
        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)

        stores = {}

        def revoke_partition(store_, partition):
            stores[store_.stream_id] = store_

        with patch("quixstreams.state.base.Store.revoke_partition", revoke_partition):
            app.run()

        assert processed_count == len(messages)

        store = stores[sdf_concat.stream_id]
        partition = store.partitions[partition_num]

        # Ensure that messages are processed in timestamp order
        # from concatenated partitions
        timestamps_sorted = sorted(m[1] for m in messages)
        with partition.begin() as tx:
            assert tx.get("timestamps", prefix=message_key) == timestamps_sorted
        store.revoke_partition(partition_num)


class TestApplicationRun:
    def test_run_with_count(
        self,
        app_factory,
        internal_consumer_factory,
    ):
        app = app_factory(auto_offset_reset="earliest")
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]

        app.dataframe(topic=input_topic).group_by("x")

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        assert app.run(count=1) == values_in[:1]
        assert app.run(count=2) == values_in[1:3]

    def test_run_with_count_multiple_branches(
        self,
        app_factory,
        internal_consumer_factory,
    ):
        app = app_factory(auto_offset_reset="earliest")
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]

        sdf = app.dataframe(topic=input_topic).group_by("x")
        # Add two branches
        sdf.apply(lambda v: v)
        sdf.apply(lambda v: v)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        # Branching can multiply the outputs.
        # Make sure that every output is counted
        expected = []
        for v in values_in:
            expected.append(v)
            expected.append(v)

        assert app.run(count=len(expected)) == expected

    def test_run_with_count_concat(
        self,
        app_factory,
        internal_consumer_factory,
    ):
        app = app_factory(auto_offset_reset="earliest")
        input_topic1 = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )
        input_topic2 = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]

        sdf1 = app.dataframe(topic=input_topic1)
        sdf2 = app.dataframe(topic=input_topic2)
        sdf1.concat(sdf2)

        with app.get_producer() as producer:
            ts = int(time.time() * 1000)
            for topic in (input_topic1, input_topic2):
                for value in values_in:
                    msg = topic.serialize(key="some_key", value=value)
                    producer.produce(
                        topic.name, key=msg.key, value=msg.value, timestamp=ts
                    )
                    ts += 1

        expected = values_in + values_in
        assert app.run(count=len(expected)) == expected

    def test_run_with_count_collect_True_and_metadata_True(
        self,
        app_factory,
        internal_consumer_factory,
    ):
        app = app_factory(auto_offset_reset="earliest")
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}]
        timestamp = 100
        key = b"some_key"

        app.dataframe(topic=input_topic)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key=key, value=value)
                producer.produce(
                    input_topic.name, key=msg.key, value=msg.value, timestamp=timestamp
                )

        expected = [
            {
                "_key": key,
                "_timestamp": timestamp,
                "_topic": input_topic.name,
                "_partition": 0,
                "_offset": 0,
                "_headers": None,
                "x": 1,
            },
            {
                "_key": key,
                "_timestamp": timestamp,
                "_topic": input_topic.name,
                "_partition": 0,
                "_offset": 1,
                "_headers": None,
                "x": 2,
            },
        ]
        assert app.run(count=len(values_in), collect=True, metadata=True) == expected

    def test_run_with_count_collect_False(
        self,
        app_factory,
        internal_consumer_factory,
    ):
        app = app_factory(auto_offset_reset="earliest")
        input_topic = app.topic(
            str(uuid.uuid4()),
            value_deserializer="json",
            value_serializer="json",
        )

        values_in = [{"x": 1}, {"x": 2}]
        timestamp = 100
        key = b"some_key"

        app.dataframe(topic=input_topic)

        with app.get_producer() as producer:
            for value in values_in:
                msg = input_topic.serialize(key=key, value=value)
                producer.produce(
                    input_topic.name, key=msg.key, value=msg.value, timestamp=timestamp
                )

        assert app.run(count=len(values_in), collect=False) == []

    def test_run_with_timeout(
        self,
        app_factory,
        internal_consumer_factory,
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
        # extended process times (time.sleep) don't affect the timeout
        app.dataframe(topic=input_topic).update(
            lambda _: time.sleep(timeout * 1.1)
        ).to_topic(output_topic)

        with app.get_producer() as producer:
            for value in values_in[:2]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        assert app.run(timeout=timeout) == values_in[:2]

    def test_run_with_timeout_or_count(
        self,
        app_factory,
        internal_consumer_factory,
        caplog,
    ):
        """
        Passing timeout or count together each trigger as expected.
        """
        app = app_factory(
            auto_offset_reset="earliest",
            # force tighter polling for faster and more accurate timeout assessments
            consumer_poll_timeout=0.1,
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
        app.dataframe(topic=input_topic).to_topic(output_topic)

        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        with caplog.at_level("INFO"):
            # gets 1 message (hits timeout)
            timeout = 1.0
            assert app.run(count=2, timeout=timeout) == values_in[:1]
            assert f"Timeout of {timeout}s reached" in caplog.text
            caplog.clear()

        with app.get_producer() as producer:
            for value in values_in[1:]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)

        with caplog.at_level("INFO"):
            assert app.run(count=2, timeout=1.0) == values_in[1:3]
            assert "Count of 2 records reached" in caplog.text

    def test_run_with_timeout_resets_after_recovery(
        self,
        app_factory,
        internal_consumer_factory,
        executor,
    ):
        """
        Timeout is set only after recovery is complete
        """
        timeout = 3.0

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
        app.dataframe(topic=input_topic).update(
            lambda v, state: state.set("blah", 1), stateful=True
        ).to_topic(output_topic)

        with app.get_producer() as producer:
            for value in values_in[:1]:
                msg = input_topic.serialize(key="some_key", value=value)
                producer.produce(input_topic.name, key=msg.key, value=msg.value)
        assert app.run(count=1) == values_in[:1]

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
            assert app.run(timeout=timeout) == values_in[1:]
