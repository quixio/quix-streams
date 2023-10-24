import time
import uuid
from concurrent.futures import Future
from json import loads, dumps
from unittest.mock import patch, create_autospec

import pytest
from confluent_kafka import KafkaException, TopicPartition
from tests.utils import TopicPartitionStub

from streamingdataframes.app import Application
from streamingdataframes.models import (
    DoubleDeserializer,
    DoubleSerializer,
    JSONDeserializer,
    SerializationError,
    JSONSerializer,
)
from streamingdataframes.platforms.quix import (
    QuixKafkaConfigsBuilder,
    TopicCreationConfigs,
)
from streamingdataframes.rowconsumer import (
    KafkaMessageError,
    RowConsumer,
)


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
    def test_run_consume_and_produce(
        self,
        app_factory,
        producer,
        topic_factory,
        row_consumer_factory,
        executor,
        row_factory,
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

        column_name = "root"
        topic_in_name, _ = topic_factory()
        topic_out_name, _ = topic_factory()

        topic_in = app.topic(
            topic_in_name, value_deserializer=JSONDeserializer(column_name=column_name)
        )
        topic_out = app.topic(
            topic_out_name,
            value_serializer=JSONSerializer(),
            value_deserializer=JSONDeserializer(),
        )

        df = app.dataframe(topics_in=[topic_in])
        df.to_topic(topic_out)

        processed_count = 0
        total_messages = 3
        # Produce messages to the topic and flush
        data = {"key": b"key", "value": b'"value"'}
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_in_name, **data)

        done = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(df)

        # Check that all messages have been processed
        assert processed_count == total_messages

        # Ensure that the right offset is committed
        with row_consumer_factory(auto_offset_reset="latest") as row_consumer:
            committed, *_ = row_consumer.committed([TopicPartition(topic_in.name, 0)])
            assert committed.offset == total_messages

        # confirm messages actually ended up being produced by the app
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
        self, app_factory, producer, topic_factory, consumer, executor
    ):
        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        app = app_factory(auto_offset_reset="error")
        topic_name, _ = topic_factory()
        topic = app.topic(
            topic_name, value_deserializer=JSONDeserializer(column_name="root")
        )
        df = app.dataframe(topics_in=[topic])

        # Stop app after 10s if nothing failed
        executor.submit(_stop_app_on_timeout, app, 10.0)
        with pytest.raises(KafkaMessageError):
            app.run(df)

    def test_run_deserialization_error_raised(
        self, app_factory, producer, topic_factory, consumer, executor
    ):
        app = app_factory(auto_offset_reset="earliest")
        topic_name, _ = topic_factory()
        topic = app.topic(topic_name, value_deserializer=DoubleDeserializer())

        # Produce a string while double is expected
        with producer:
            producer.produce(topic=topic_name, value=b"abc")

        df = app.dataframe(topics_in=[topic])

        with pytest.raises(SerializationError):
            # Stop app after 10s if nothing failed
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(df)

    def test_run_consumer_error_suppressed(
        self, app_factory, producer, topic_factory, consumer, executor
    ):
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
        topic_name, _ = topic_factory()
        topic = app.topic(topic_name)
        df = app.dataframe(topics_in=[topic])

        with patch.object(RowConsumer, "poll") as mocked:
            # Patch RowConsumer.poll to simulate failures
            mocked.side_effect = ValueError("test")
            # Stop app when the future is resolved
            executor.submit(_stop_app_on_future, app, done, 10.0)
            app.run(df)
        assert polled > 1

    def test_run_processing_error_raised(
        self, app_factory, topic_factory, producer, executor
    ):
        app = app_factory(auto_offset_reset="earliest")

        topic_name, _ = topic_factory()
        topic = app.topic(topic_name, value_deserializer=JSONDeserializer())
        df = app.dataframe(topics_in=[topic])

        def fail(*args):
            raise ValueError("test")

        df = df.apply(fail)

        with producer:
            producer.produce(topic=topic.name, value=b'{"field":"value"}')

        with pytest.raises(ValueError):
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(df)

    def test_run_processing_error_suppressed(
        self, app_factory, topic_factory, producer, executor
    ):
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
        topic_name, _ = topic_factory()
        topic = app.topic(topic_name, value_deserializer=JSONDeserializer())
        df = app.dataframe(topics_in=[topic])

        def fail(*args):
            raise ValueError("test")

        df = df.apply(fail)

        with producer:
            for i in range(produced):
                producer.produce(topic=topic.name, value=b'{"field":"value"}')

        # Stop app from the background thread when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(df)
        assert produced == consumed

    def test_run_producer_error_raised(
        self, app_factory, topic_factory, producer, topic_json_serdes_factory, executor
    ):
        app = app_factory(
            auto_offset_reset="earliest",
            producer_extra_config={"message.max.bytes": 1000},
        )

        topic_in_name, _ = topic_factory()
        topic_out_name, _ = topic_factory()
        topic_in = app.topic(topic_in_name, value_deserializer=JSONDeserializer())
        topic_out = app.topic(topic_out_name, value_serializer=JSONSerializer())

        df = app.dataframe(topics_in=[topic_in])
        df.to_topic(topic_out)

        with producer:
            producer.produce(topic_in.name, dumps({"field": 1001 * "a"}))

        with pytest.raises(KafkaException):
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(df)

    def test_run_serialization_error_raised(
        self, app_factory, producer, topic_factory, executor
    ):
        app = app_factory(auto_offset_reset="earliest")

        topic_in_name, _ = topic_factory()
        topic_in = app.topic(topic_in_name, value_deserializer=JSONDeserializer())
        topic_out_name, _ = topic_factory()
        topic_out = app.topic(topic_out_name, value_serializer=DoubleSerializer())

        df = app.dataframe(topics_in=[topic_in])
        df.to_topic(topic_out)

        with producer:
            producer.produce(topic_in.name, b'{"field":"value"}')

        with pytest.raises(SerializationError):
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(df)

    def test_run_producer_error_suppressed(
        self, app_factory, producer, topic_factory, consumer, executor
    ):
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
        topic_in_name, _ = topic_factory()
        topic_in = app.topic(topic_in_name, value_deserializer=JSONDeserializer())
        topic_out_name, _ = topic_factory()
        topic_out = app.topic(topic_out_name, value_serializer=DoubleSerializer())

        df = app.dataframe(topics_in=[topic_in])
        df.to_topic(topic_out)

        with producer:
            for _ in range(produce_input):
                producer.produce(topic_in.name, b'{"field":"value"}')

        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(df)

        assert produce_output_attempts == produce_input

    def test_topic_init(self):
        app = Application(broker_address="localhost", consumer_group="test")
        topic = app.topic(name="test-topic")

        assert topic

    def test_streamingdataframe_init(self):
        app = Application(broker_address="localhost", consumer_group="test")
        topic = app.topic(name="test-topic")
        sdf = app.dataframe(topics_in=[topic])

        assert sdf


class TestQuixApplication:
    def test_init(self):
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg = {
            "sasl.mechanisms": "SCRAM-SHA-256",
            "security.protocol": "SASL_SSL",
            "bootstrap.servers": "address1,address2",
            "sasl.username": "my-username",
            "sasl.password": "my-password",
            "ssl.ca.location": "/mock/dir/ca.cert",
            "ssl.endpoint.identification.algorithm": "none",
        }
        cfg_builder.get_confluent_broker_config.return_value = cfg
        cfg_builder.append_workspace_id.return_value = "my_ws-c_group"
        app = Application.Quix(
            quix_config_builder=cfg_builder,
            consumer_group="c_group",
            consumer_extra_config={"extra": "config"},
            producer_extra_config={"extra": "config"},
        )

        # Check if items from the Quix config have been passed
        # to the low-level configs of producer and consumer
        assert cfg.items() <= app._producer._producer_config.items()
        assert cfg.items() <= app._consumer._consumer_config.items()

        assert app._producer._producer_config["extra"] == "config"
        assert app._consumer._consumer_config["extra"] == "config"
        assert app._consumer._consumer_config["group.id"] == "my_ws-c_group"
        cfg_builder.append_workspace_id.assert_called_with("c_group")

    def test_topic_default(self, quix_app_factory):
        """
        Topic names created from Quix apps are prefixed by the workspace id
        """
        app = quix_app_factory()
        builder = app._quix_config_builder

        initial_topic_name = "input_topic"
        topic = app.topic(initial_topic_name)
        expected_name = f"{builder.workspace_id}-{initial_topic_name}"
        assert topic.name == expected_name
        assert builder.create_topic_configs[expected_name].name == expected_name

    def test_topic_config(self, quix_app_factory):
        """
        Topic names created from Quix apps are prefixed by the workspace id
        """
        app = quix_app_factory()
        builder = app._quix_config_builder

        initial_topic_name = "input_topic"
        topic = app.topic(
            initial_topic_name,
            creation_configs=TopicCreationConfigs(name="billy bob", num_partitions=5),
        )
        expected_name = f"{builder.workspace_id}-{initial_topic_name}"
        assert topic.name == expected_name
        assert builder.create_topic_configs[expected_name].name == expected_name
        assert builder.create_topic_configs[expected_name].num_partitions == 5

    def test_topic_auto_create_false_topic_confirmation(
        self, dataframe_factory, quix_app_factory
    ):
        """
        Topics are confirmed when auto_create_topics=False
        """
        app = quix_app_factory(auto_create_topics=False)
        builder = app._quix_config_builder
        topics = [app.topic("topic_in"), app.topic("topic_out")]

        app._quix_runtime_init()

        actual_call_arg = [_ for _ in builder.confirm_topics_exist.call_args[0][0]]
        assert actual_call_arg == list(builder.create_topic_configs.values())
        assert {c.name for c in actual_call_arg} == {t.name for t in topics}

    def test_topic_auto_create_true(self, dataframe_factory, quix_app_factory):
        """
        Topics are created when auto_create_topics=True
        """
        app = quix_app_factory(auto_create_topics=True)
        builder = app._quix_config_builder
        topics = [app.topic("topic_in"), app.topic("topic_out")]

        app._quix_runtime_init()

        actual_call_arg = [_ for _ in builder.create_topics.call_args[0][0]]
        assert actual_call_arg == list(builder.create_topic_configs.values())
        assert {c.name for c in actual_call_arg} == {t.name for t in topics}


class TestApplicationWithState:
    def test_run_stateful_success(
        self,
        app_factory,
        producer,
        topic_factory,
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
        app = app_factory(
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            state_dir=state_dir,
        )

        topic_in_name, _ = topic_factory()

        topic_in = app.topic(topic_in_name, value_deserializer=JSONDeserializer())
        state_manager = app._state_manager
        state_manager.register_store(topic_in.name, "default")

        # TODO: Use stateful functions after they're implemented
        # Define a function that counts incoming Rows using state
        def count(_):
            state = state_manager.get_store_transaction("default")
            total = state.get("total", 0)
            total += 1
            state.set("total", total)
            if total == total_messages:
                total_consumed.set_result(total)

        df = app.dataframe(topics_in=[topic_in])
        df.apply(count)

        total_messages = 3
        # Produce messages to the topic and flush
        data = {"key": b"key", "value": dumps({"key": "value"})}
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_in_name, **data)

        total_consumed = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, total_consumed, 10.0)
        app.run(df)

        # Check that the values are actually in the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(topic_in.name, "default")
        state_manager.on_partition_assign(
            TopicPartitionStub(topic=topic_in.name, partition=0)
        )
        store = state_manager.get_store(topic=topic_in.name, store_name="default")
        with store.start_partition_transaction(partition=0) as tx:
            assert tx.get("total") == total_consumed.result()

    def test_run_stateful_processing_fails(
        self,
        app_factory,
        producer,
        topic_factory,
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
        )

        topic_in_name, _ = topic_factory()

        topic_in = app.topic(topic_in_name, value_deserializer=JSONDeserializer())
        state_manager = app._state_manager
        state_manager.register_store(topic_in.name, "default")

        # TODO: Use stateful functions after they're implemented
        # Define a function that counts incoming Rows using state
        def count(_):
            state = state_manager.get_store_transaction("default")
            total = state.get("total", 0)
            total += 1
            state.set("total", total)

        failed = Future()

        def fail(_):
            failed.set_result(True)
            raise ValueError("test")

        df = app.dataframe(topics_in=[topic_in])
        df.apply(count)
        df.apply(fail)

        total_messages = 3
        # Produce messages to the topic and flush
        data = {"key": b"key", "value": dumps({"key": "value"})}
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_in_name, **data)

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, failed, 10.0)
        with pytest.raises(ValueError):
            app.run(df)

        # Ensure that nothing was committed to the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(topic_in.name, "default")
        state_manager.on_partition_assign(
            TopicPartitionStub(topic=topic_in.name, partition=0)
        )
        store = state_manager.get_store(topic=topic_in.name, store_name="default")
        with store.start_partition_transaction(partition=0) as tx:
            assert tx.get("total") is None

    def test_run_stateful_suppress_processing_errors(
        self,
        app_factory,
        producer,
        topic_factory,
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
            # Suppress errors during message processing
            on_processing_error=lambda *args: True,
        )

        topic_in_name, _ = topic_factory()

        topic_in = app.topic(topic_in_name, value_deserializer=JSONDeserializer())
        state_manager = app._state_manager
        state_manager.register_store(topic_in.name, "default")

        # TODO: Use stateful functions after they're implemented
        # Define a function that counts incoming Rows using state
        def count(_):
            state = state_manager.get_store_transaction("default")
            total = state.get("total", 0)
            total += 1
            state.set("total", total)
            if total == total_messages:
                total_consumed.set_result(total)

        def fail(_):
            raise ValueError("test")

        df = app.dataframe(topics_in=[topic_in])
        df.apply(count)
        df.apply(fail)

        total_messages = 3
        # Produce messages to the topic and flush
        data = {"key": b"key", "value": dumps({"key": "value"})}
        with producer:
            for _ in range(total_messages):
                producer.produce(topic_in_name, **data)

        total_consumed = Future()

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, total_consumed, 10.0)
        # Run the application
        app.run(df)

        # Ensure that data is committed to the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(topic_in.name, "default")
        state_manager.on_partition_assign(
            TopicPartitionStub(topic=topic_in.name, partition=0)
        )
        store = state_manager.get_store(topic=topic_in.name, store_name="default")
        with store.start_partition_transaction(partition=0) as tx:
            assert tx.get("total") == total_consumed.result()
