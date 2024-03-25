import logging
import time
import uuid
from concurrent.futures import Future
from json import loads, dumps
from unittest.mock import patch, create_autospec

import pytest
from confluent_kafka import KafkaException, TopicPartition

from quixstreams.app import Application
from quixstreams.dataframe import StreamingDataFrame
from quixstreams.dataframe.windows.base import get_window_ranges
from quixstreams.models import (
    DoubleDeserializer,
    DoubleSerializer,
    JSONDeserializer,
    SerializationError,
    JSONSerializer,
)
from quixstreams.platforms.quix import (
    QuixKafkaConfigsBuilder,
)
from quixstreams.platforms.quix.env import QuixEnvironment
from quixstreams.rowconsumer import (
    KafkaMessageError,
    RowConsumer,
)
from quixstreams.state import State
from tests.utils import TopicPartitionStub


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

    def test_run_consume_and_produce(
        self,
        app_factory,
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
        partition_num = 0
        topic_in = app.topic(
            str(uuid.uuid4()),
            value_deserializer=JSONDeserializer(column_name=column_name),
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
        data = {"key": b"key", "value": b'"value"', "partition": partition_num}
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
        with row_consumer_factory(auto_offset_reset="latest") as row_consumer:
            committed, *_ = row_consumer.committed(
                [TopicPartition(topic_in.name, partition_num)]
            )
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

    def test_run_consumer_error_raised(self, app_factory, executor):
        # Set "auto_offset_reset" to "error" to simulate errors in Consumer
        app = app_factory(auto_offset_reset="error")
        topic = app.topic(
            str(uuid.uuid4()), value_deserializer=JSONDeserializer(column_name="root")
        )
        sdf = app.dataframe(topic)

        # Stop app after 10s if nothing failed
        executor.submit(_stop_app_on_timeout, app, 10.0)
        with pytest.raises(KafkaMessageError):
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

        with patch.object(RowConsumer, "poll") as mocked:
            # Patch RowConsumer.poll to simulate failures
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
        app._topic_manager.create_all_topics()

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

    def test_topic_init(self):
        app = Application(broker_address="localhost", consumer_group="test")
        topic = app.topic(name="test-topic")

        assert topic

    def test_streamingdataframe_init(self):
        app = Application(broker_address="localhost", consumer_group="test")
        topic = app.topic(name="test-topic")
        sdf = app.dataframe(topic)
        assert isinstance(sdf, StreamingDataFrame)

    def test_topic_auto_create_true(self, app_factory):
        """
        Topics are auto-created when auto_create_topics=True
        """
        app = app_factory(auto_create_topics=True)
        topic_manager = app._topic_manager
        _ = [app.topic("topic_in"), app.topic("topic_out")]

        with patch.object(topic_manager, "create_all_topics") as create:
            with patch.object(topic_manager, "validate_all_topics"):
                app._setup_topics()

        create.assert_called()

    def test_topic_auto_create_false(self, app_factory):
        """
        Topics are not auto-created when auto_create_topics=False
        """
        app = app_factory(auto_create_topics=False)
        topic_manager = app._topic_manager
        _ = [app.topic("topic_in"), app.topic("topic_out")]

        with patch.object(topic_manager, "create_all_topics") as create:
            with patch.object(topic_manager, "validate_all_topics"):
                app._setup_topics()

        create.assert_not_called()

    def test_topic_validation(self, app_factory):
        """
        Topics are validated
        """
        app = app_factory()
        topic_manager = app._topic_manager

        with patch.object(topic_manager, "validate_all_topics") as validate:
            with patch.object(topic_manager, "create_all_topics"):
                app._setup_topics()

        validate.assert_called()

    def test_topic_setup_on_get_producer(self, app_factory):
        """
        Topics are set up according to app settings when get_producer is called
        """
        app = app_factory()
        with patch.object(app, "_setup_topics") as setup_topics:
            with app.get_producer():
                ...
        setup_topics.assert_called()

    def test_topic_setup_on_get_consumer(self, app_factory):
        """
        Topics are set up according to app settings when get_consumer is called
        """
        app = app_factory()
        with patch.object(app, "_setup_topics") as setup_topics:
            with app.get_consumer():
                ...
        setup_topics.assert_called()


class TestQuixApplication:
    def test_init(self):
        def cfg():
            return {
                "sasl.mechanisms": "SCRAM-SHA-256",
                "security.protocol": "SASL_SSL",
                "bootstrap.servers": "address1,address2",
                "sasl.username": "my-username",
                "sasl.password": "my-password",
                "ssl.ca.location": "/mock/dir/ca.cert",
            }

        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg_builder.get_confluent_broker_config.side_effect = cfg
        cfg_builder.prepend_workspace_id.return_value = "my_ws-c_group"
        cfg_builder.strip_workspace_id_prefix.return_value = "c_group"
        app = Application.Quix(
            quix_config_builder=cfg_builder,
            consumer_group="c_group",
            consumer_extra_config={"extra": "config"},
            producer_extra_config={"extra": "config"},
        )

        # Check if items from the Quix config have been passed
        # to the low-level configs of producer and consumer
        assert cfg().items() <= app._producer._producer_config.items()
        assert cfg().items() <= app._consumer._consumer_config.items()

        assert app._producer._producer_config["extra"] == "config"
        assert app._consumer._consumer_config["extra"] == "config"
        assert app._consumer._consumer_config["group.id"] == "my_ws-c_group"
        cfg_builder.prepend_workspace_id.assert_called_with("c_group")

    def test_topic_name_and_config(self, quix_app_factory):
        """
        Topic names created from Quix apps are prefixed by the workspace id
        Topic config has provided values else defaults
        """
        workspace_id = "my-workspace"
        app = quix_app_factory(workspace_id=workspace_id)
        topic_manager = app._topic_manager

        initial_topic_name = "input_topic"
        topic_partitions = 5
        topic = app.topic(
            initial_topic_name,
            config=topic_manager.topic_config(num_partitions=topic_partitions),
        )
        expected_name = f"{workspace_id}-{initial_topic_name}"
        expected_topic = topic_manager.topics[expected_name]
        assert topic.name == expected_name
        assert expected_name in topic_manager.topics
        assert (
            expected_topic.config.replication_factor == topic_manager._topic_replication
        )
        assert expected_topic.config.num_partitions == topic_partitions

    def test_consumer_extra_config(self, app_factory):
        """
        Test that some configs like `enable.auto.offset.store` are overridable and others are not
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
            assert x._consumer_config["auto.offset.reset"] is "latest"

    def test_producer_extra_config(self, app_factory):
        """
        Test that setting same configs through different ways works in the expected way
        """
        app = app_factory(
            auto_offset_reset="latest",
            producer_extra_config={"auto.offset.reset": "earliest"},
        )

        with app.get_consumer() as x:
            assert x._consumer_config["auto.offset.reset"] is "latest"

    def test_quix_app_stateful_quix_deployment_no_state_management_warning(
        self, quix_app_factory, monkeypatch, topic_factory, executor
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
            executor.submit(_stop_app_on_timeout, app, 10.0)
            app.run(sdf)

        warnings = [w for w in warned.list if w.category is RuntimeWarning]
        warning = str(warnings[0].message)
        assert "State Management feature is disabled" in warning

    def test_quix_app_stateful_quix_deployment_state_dir_mismatch_warning(
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


class TestApplicationWithState:
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

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, total_consumed, 10.0)
        app.run(sdf)

        # Check that the values are actually in the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(topic_in.name, "default")
        state_manager.on_partition_assign(
            TopicPartitionStub(topic=topic_in.name, partition=partition_num)
        )
        store = state_manager.get_store(topic=topic_in.name, store_name="default")
        with store.start_partition_transaction(partition=partition_num) as tx:
            # All keys in state must be prefixed with the message key
            with tx.with_prefix(message_key):
                assert tx.get("total") == total_consumed.result()

    def test_run_stateful_processing_fails(
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
        )

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())

        # Define a function that counts incoming Rows using state
        def count(_, state: State):
            total = state.get("total", 0)
            total += 1
            state.set("total", total)

        failed = Future()

        def fail(*_):
            failed.set_result(True)
            raise ValueError("test")

        sdf = app.dataframe(topic_in).update(count, stateful=True).update(fail)

        total_messages = 3
        # Produce messages to the topic and flush
        data = {
            "key": b"key",
            "value": dumps({"key": "value"}),
            "partition": partition_num,
        }
        with app.get_producer() as producer:
            for _ in range(total_messages):
                producer.produce(topic_in.name, **data)

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, failed, 10.0)
        with pytest.raises(ValueError):
            app.run(sdf)

        # Ensure that nothing was committed to the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(topic_in.name, "default")
        state_manager.on_partition_assign(
            TopicPartitionStub(topic=topic_in.name, partition=partition_num)
        )
        store = state_manager.get_store(topic=topic_in.name, store_name="default")
        with store.start_partition_transaction(partition=partition_num) as tx:
            assert tx.get("total") is None

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
            on_processing_error=lambda *args: True,
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

        sdf = app.dataframe(topic_in).update(count, stateful=True).apply(fail)

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

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, total_consumed, 10.0)
        # Run the application
        app.run(sdf)

        # Ensure that data is committed to the DB
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        state_manager.register_store(topic_in.name, "default")
        state_manager.on_partition_assign(
            TopicPartitionStub(topic=topic_in.name, partition=partition_num)
        )
        store = state_manager.get_store(topic=topic_in.name, store_name="default")
        with store.start_partition_transaction(partition=partition_num) as tx:
            with tx.with_prefix(message_key):
                assert tx.get("total") == total_consumed.result()

    def test_on_assign_topic_offset_behind_warning(
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
        )

        topic_in = app.topic(str(uuid.uuid4()), value_deserializer=JSONDeserializer())

        # Set the store partition offset to 9999
        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )
        with state_manager:
            state_manager.register_store(topic_in.name, "default")
            state_partitions = state_manager.on_partition_assign(
                TopicPartitionStub(topic=topic_in.name, partition=partition_num)
            )
            with state_manager.start_store_transaction(
                topic=topic_in.name, partition=partition_num, offset=9999
            ):
                tx = state_manager.get_store_transaction()
                tx.set("key", "value")
            assert state_partitions[partition_num].get_processed_offset() == 9999

        # Define some stateful function so the App assigns store partitions
        done = Future()

        sdf = app.dataframe(topic_in).update(
            lambda *_: done.set_result(True), stateful=True
        )

        # Produce a message to the topic and flush
        data = {
            "key": b"key",
            "value": dumps({"key": "value"}),
            "partition": partition_num,
        }
        with app.get_producer() as producer:
            producer.produce(topic_in.name, **data)

        # Stop app when the future is resolved
        executor.submit(_stop_app_on_future, app, done, 10.0)
        # Run the application
        with patch.object(logging.getLoggerClass(), "warning") as mock:
            app.run(sdf)

        assert mock.called
        assert "is behind the stored offset" in mock.call_args[0][0]

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
        tx_prefix = b"key"

        state_manager = state_manager_factory(
            group_id=consumer_group, state_dir=state_dir
        )

        # Add data to the state store
        with state_manager:
            state_manager.register_store(topic_in_name, "default")
            state_manager.on_partition_assign(
                TopicPartitionStub(topic=topic_in_name, partition=0)
            )
            store = state_manager.get_store(topic=topic_in_name, store_name="default")
            with store.start_partition_transaction(partition=0) as tx:
                # All keys in state must be prefixed with the message key
                with tx.with_prefix(tx_prefix):
                    tx.set("my_state", True)

        # Clear the state
        app.clear_state()

        # Check that the date is cleared from the state store
        with state_manager:
            state_manager.register_store(topic_in_name, "default")
            state_manager.on_partition_assign(
                TopicPartitionStub(topic=topic_in_name, partition=0)
            )
            store = state_manager.get_store(topic=topic_in_name, store_name="default")
            with store.start_partition_transaction(partition=0) as tx:
                # All keys in state must be prefixed with the message key
                with tx.with_prefix(tx_prefix):
                    assert tx.get("my_state") is None

    def test_app_use_changelog_false(self, app_factory):
        """
        `Application`s StateStoreManager should not have a TopicManager if
        use_changelog_topics is set to `False`.
        """
        app = app_factory(use_changelog_topics=False)
        assert not app._state_manager.using_changelogs


class TestAppRecovery:
    def test_changelog_recovery_default_store(
        self,
        app_factory,
        executor,
        tmp_path,
        state_manager_factory,
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
                auto_offset_reset="earliest",
                use_changelog_topics="True",
                on_message_processed=on_message_processed,
                consumer_group=consumer_group,
                state_dir=state_dir,
            )
            topic = app.topic(
                topic_name, config=app._topic_manager.topic_config(num_partitions=2)
            )
            sdf = app.dataframe(topic)
            sdf = sdf.apply(sum_value, stateful=True)
            return app, sdf, topic

        def validate_state():
            with state_manager_factory(
                group_id=consumer_group,
                state_dir=state_dir,
            ) as state_manager:
                state_manager.register_store(topic.name, store_name)
                for p_num in partition_msg_count:
                    state_manager.on_partition_assign(
                        TopicPartitionStub(topic=topic.name, partition=p_num)
                    )
                store = state_manager.get_store(topic=topic.name, store_name=store_name)
                for p_num, count in partition_msg_count.items():
                    assert store._partitions[p_num].get_changelog_offset() == count
                    with store.start_partition_transaction(partition=p_num) as tx:
                        # All keys in state must be prefixed with the message key
                        with tx.with_prefix(f"key{p_num}".encode()):
                            assert tx.get(sum_key) == count * msg_int_value

                for p_num in partition_msg_count:
                    state_manager.on_partition_revoke(
                        TopicPartitionStub(topic=topic.name, partition=p_num)
                    )
                state_manager.clear_stores()

        # Produce messages to the topic and flush
        app, sdf, topic = get_app()
        with app.get_producer() as producer:
            for p_num, count in partition_msg_count.items():
                serialized = topic.serialize(
                    key=f"key{p_num}".encode(), value={"my_value": msg_int_value}
                )
                data = {
                    "key": serialized.key,
                    "value": serialized.value,
                    "partition": p_num,
                }
                for _ in range(count):
                    producer.produce(topic.name, **data)

        # run app to populate state
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)
        # validate and then delete the state
        assert processed_count == partition_msg_count
        processed_count = {0: 0, 1: 0}
        validate_state()

        # run the app again and validate the recovered state
        app, sdf, topic = get_app()
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)
        # no messages should have been processed outside of recovery loop
        assert processed_count == {0: 0, 1: 0}
        # State should be the same as before deletion
        validate_state()

    def test_changelog_recovery_window_store(
        self, app_factory, executor, tmp_path, state_manager_factory
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
            0: list(range(1707260000000, 1707260004000, msg_tick_ms)),
            1: list(range(1707260000000, 1707260002000, msg_tick_ms)),
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
                auto_offset_reset="earliest",
                use_changelog_topics="True",
                consumer_group=consumer_group,
                on_message_processed=on_message_processed,
                state_dir=state_dir,
            )
            topic = app.topic(
                topic_name, config=app._topic_manager.topic_config(num_partitions=2)
            )
            sdf = app.dataframe(topic)
            sdf = sdf.apply(lambda row: row["my_value"])
            sdf = (
                sdf.hopping_window(
                    duration_ms=window_duration_ms,
                    step_ms=window_step_ms,
                    name=store_name,
                )
                .sum()
                .final()
            )
            sdf = sdf.apply(
                lambda value: {
                    "sum": value["value"],
                    "window": (value["start"], value["end"]),
                }
            )
            return app, sdf, topic

        def validate_state():
            with state_manager_factory(
                group_id=consumer_group, state_dir=state_dir
            ) as state_manager:
                state_manager.register_windowed_store(topic.name, store_name)
                for p_num in partition_timestamps:
                    state_manager.on_partition_assign(
                        TopicPartitionStub(topic=topic.name, partition=p_num)
                    )
                store = state_manager.get_store(topic=topic.name, store_name=store_name)
                for p_num, windows in expected_window_updates.items():
                    # in this test, each expiration check only deletes one window,
                    # simplifying the offset counting.
                    expected_offset = sum(
                        expected_window_updates[p_num].values()
                    ) + 2 * len(expected_expired_windows[p_num])
                    assert (
                        expected_offset
                        == store._partitions[p_num].get_changelog_offset()
                    )

                    with store.start_partition_transaction(partition=p_num) as tx:
                        with tx.with_prefix(f"key{p_num}".encode()):
                            for window, count in windows.items():
                                expected = count
                                if window in expected_expired_windows[p_num]:
                                    expected = None
                                else:
                                    # each message value was 10
                                    expected *= msg_int_value
                                assert tx.get_window(*window) == expected

                for p_num in partition_timestamps:
                    state_manager.on_partition_revoke(
                        TopicPartitionStub(topic=topic.name, partition=p_num)
                    )
                state_manager.clear_stores()

        app, sdf, topic = get_app()
        # Produce messages to the topic and flush
        with app.get_producer() as producer:
            for p_num, timestamps in partition_timestamps.items():
                serialized = topic.serialize(
                    key=f"key{p_num}".encode(), value={"my_value": msg_int_value}
                )
                data = {
                    "key": serialized.key,
                    "value": serialized.value,
                    "partition": p_num,
                }
                for ts in timestamps:
                    data["timestamp"] = ts
                    producer.produce(topic.name, **data)

        # run app to populate state
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)
        # validate and then delete the state
        assert processed_count == partition_msg_count
        processed_count = {0: 0, 1: 0}
        validate_state()

        # run the app again and validate the recovered state
        app, sdf, topic = get_app()
        done = Future()
        executor.submit(_stop_app_on_future, app, done, 10.0)
        app.run(sdf)
        # no messages should have been processed outside of recovery loop
        assert processed_count == {0: 0, 1: 0}
        # State should be the same as before deletion
        validate_state()
