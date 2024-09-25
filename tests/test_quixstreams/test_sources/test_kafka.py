import time
import uuid
import pytest
import logging

from confluent_kafka import TopicPartition
from unittest.mock import patch

from quixstreams.platforms.quix.config import ConnectionConfig
from quixstreams.kafka.consumer import Consumer
from quixstreams.kafka.producer import Producer
from quixstreams.models.topics import TopicAdmin, Topic, TopicConfig
from quixstreams.sources import (
    KafkaReplicatorSource,
    SourceException,
    QuixEnvironmentSource,
)

logger = logging.getLogger("quixstreams")


class Base:
    NUMBER_OF_MESSAGES = 10

    @pytest.fixture(scope="class")
    def external_kafka_container(self, kafka_container_factory):
        yield from kafka_container_factory()

    @pytest.fixture(autouse=True)
    def external_kafka(self, external_kafka_container):
        self._external_broker_address = external_kafka_container.broker_address

    @pytest.fixture()
    def app(self, app_factory):
        return app_factory(auto_offset_reset="earliest", request_timeout=1)


class TestKafkaReplicatorSource(Base):
    def create_source_topic(self, num_partitions=1):
        source_topic = Topic(
            str(uuid.uuid4()),
            config=TopicConfig(num_partitions=num_partitions, replication_factor=1),
            key_serializer="bytes",
            value_serializer="json",
        )

        source_admin = TopicAdmin(broker_address=self._external_broker_address)
        source_admin.create_topics(topics=[source_topic])
        return source_topic

    def source(self, config, topic, broker_address=None):
        if broker_address is None:
            broker_address = self._external_broker_address

        return KafkaReplicatorSource(
            "test source",
            app_config=config,
            topic=topic.name,
            broker_address=broker_address,
            auto_offset_reset="earliest",
        )

    def test_missing_source_topic(self, app):
        source_topic = Topic(
            str(uuid.uuid4()),
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        destination_topic = app.topic(str(uuid.uuid4()))

        source = self.source(app.config, source_topic)
        app.add_source(source, destination_topic)

        with pytest.raises(SourceException) as exc:
            app._run()

        assert isinstance(exc.value.__cause__, ValueError)
        assert str(exc.value.__cause__) == f"Source topic {source_topic.name} not found"

    def test_missing_destination_topic(self, app):
        destination_topic = Topic(
            str(uuid.uuid4()),
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        source_topic = self.create_source_topic(num_partitions=4)

        source = self.source(app.config, source_topic)
        app.add_source(source, destination_topic)

        with pytest.raises(SourceException) as exc:
            app._run()

        assert isinstance(exc.value.__cause__, ValueError)
        assert (
            str(exc.value.__cause__)
            == f"Destination topic {destination_topic.name} not found"
        )

    def test_more_source_partitions(self, app):
        destination_topic = app.topic(str(uuid.uuid4()))
        source_topic = self.create_source_topic(num_partitions=4)

        source = self.source(app.config, source_topic)
        app.add_source(source, destination_topic)

        with pytest.raises(SourceException) as exc:
            app._run()

        assert isinstance(exc.value.__cause__, ValueError)
        assert (
            str(exc.value.__cause__)
            == "Source topic has more partitions than destination topic"
        )

    def test_invalid_destination_broker(self, app):
        destination_topic = app.topic(str(uuid.uuid4()))
        source_topic = self.create_source_topic()

        source = self.source(app.config, source_topic, broker_address="127.0.0.1:aaa")
        app.add_source(source, destination_topic)

        with pytest.raises(SourceException) as exc:
            app._run()

        assert isinstance(exc.value.__cause__, RuntimeError)
        assert (
            str(exc.value.__cause__)
            == """KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata: Local: Broker transport failure"}"""
        )

    def test_invalid_source_broker(self, app):
        destination_topic = app.topic(str(uuid.uuid4()))
        source_topic = self.create_source_topic()

        source = self.source(
            app.config.copy(
                broker_address=ConnectionConfig(bootstrap_servers="127.0.0.1:aaa")
            ),
            source_topic,
        )
        app.add_source(source, destination_topic)

        with pytest.raises(SourceException) as exc:
            app._run()

        assert isinstance(exc.value.__cause__, RuntimeError)
        assert (
            str(exc.value.__cause__)
            == """KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata: Local: Broker transport failure"}"""
        )

    def test_default_topic_config(self, app):
        source_topic = self.create_source_topic(num_partitions=7)

        source = self.source(app.config, source_topic)
        destination_topic = app.add_source(source)

        source_topic_config = (
            TopicAdmin(broker_address=self._external_broker_address)
            .inspect_topics(topic_names=[source_topic.name])
            .get(source_topic.name)
        )

        assert destination_topic.config == source_topic_config
        assert destination_topic.config.num_partitions == 7

    def test_kafka_source(self, app, executor):
        destination_topic = app.topic(str(uuid.uuid4()))
        source_topic = self.create_source_topic()

        source = self.source(app.config, source_topic)
        app.add_source(source, destination_topic)

        # Produce data to the external kafka
        data = list(range(self.NUMBER_OF_MESSAGES))
        with Producer(broker_address=self._external_broker_address) as producer:
            for i in data:
                msg = source_topic.serialize("test", i)
                producer.produce(
                    topic=source_topic.name,
                    key=msg.key,
                    value=msg.value,
                )

        # Consume result data from main kafka
        results = []

        def _consume(config):
            try:
                with Consumer(
                    broker_address=config.broker_address,
                    consumer_group=config.consumer_group,
                    auto_offset_reset="earliest",
                    auto_commit_enable=False,
                    extra_config=config.consumer_extra_config,
                ) as consumer:
                    consumer.subscribe([destination_topic.name])
                    while True:
                        msg = consumer.poll(timeout=10)
                        if msg is None:
                            app.stop()
                            return

                        data = destination_topic.deserialize(msg)
                        results.append(data.value)

                        if data.value == self.NUMBER_OF_MESSAGES - 1:
                            app.stop()
                            return
            except BaseException:
                logger.exception("error")
                raise

        app.setup_topics()
        executor.submit(_consume, app.config)
        app._run()

        assert results == data

    def test_start_at_offset(self, app, executor):
        destination_topic = app.topic(str(uuid.uuid4()))
        source_topic = self.create_source_topic()

        source = self.source(app.config, source_topic)
        app.add_source(source, destination_topic)

        # Produce data to the external kafka
        data = list(range(self.NUMBER_OF_MESSAGES))
        with Producer(broker_address=self._external_broker_address) as producer:
            for i in data:
                msg = source_topic.serialize("test", i)
                producer.produce(
                    topic=source_topic.name,
                    key=msg.key,
                    value=msg.value,
                )

        # Topic must exist before commiting to it
        app.setup_topics()

        # Set offset in main kafka
        start_offset = 4
        with Consumer(
            broker_address=app.config.broker_address,
            consumer_group=source.target_customer_group,
            auto_offset_reset=app.config.auto_offset_reset,
            auto_commit_enable=False,
            extra_config=app.config.consumer_extra_config,
        ) as consumer:
            consumer.commit(
                asynchronous=False,
                offsets=[
                    TopicPartition(
                        topic=destination_topic.name,
                        partition=0,
                        offset=start_offset,
                    )
                ],
            )

        # Consume result data from main kafka
        results = []

        def _consume(config):
            time.sleep(2)
            try:
                with Consumer(
                    broker_address=config.broker_address,
                    consumer_group=config.consumer_group,
                    auto_offset_reset="earliest",
                    auto_commit_enable=False,
                    extra_config=config.consumer_extra_config,
                ) as consumer:
                    consumer.subscribe([destination_topic.name])
                    while True:
                        msg = consumer.poll(timeout=10)
                        if msg is None:
                            app.stop()
                            return

                        data = destination_topic.deserialize(msg)
                        results.append(data.value)

                        if data.value == self.NUMBER_OF_MESSAGES - 1:
                            app.stop()
                            return
            except BaseException:
                logger.exception("error")
                raise

        executor.submit(_consume, app.config)
        app._run()

        assert results == data[start_offset:]


class TestQuixEnvironmentSource(Base):

    QUIX_WORKSPACE_ID = "my_workspace_id"

    def source(self, config, topic):
        return QuixEnvironmentSource(
            "test source",
            app_config=config,
            topic=topic.name,
            auto_offset_reset="earliest",
            quix_sdk_token="my_sdk_token",
            quix_workspace_id=self.QUIX_WORKSPACE_ID,
        )

    def _get_librdkafka_connection_config(self, *args, **kwargs):
        return ConnectionConfig(bootstrap_servers=self._external_broker_address)

    def create_source_topic(self, num_partitions=1, quix_workspace_id=None):
        source_topic = Topic(
            str(uuid.uuid4()),
            config=TopicConfig(num_partitions=num_partitions, replication_factor=1),
            key_serializer="bytes",
            value_serializer="json",
        )

        source_topic_with_workspace = Topic(
            f"{quix_workspace_id or self.QUIX_WORKSPACE_ID}-{source_topic.name}",
            config=TopicConfig(num_partitions=num_partitions, replication_factor=1),
            key_serializer="bytes",
            value_serializer="json",
        )

        source_admin = TopicAdmin(broker_address=self._external_broker_address)
        source_admin.create_topics(topics=[source_topic_with_workspace])
        return source_topic, source_topic_with_workspace

    def test_quix_environment_source(self, app, executor):
        destination_topic = app.topic(str(uuid.uuid4()))
        source_topic, source_topic_with_workspace = self.create_source_topic()

        with patch(
            "quixstreams.app.QuixKafkaConfigsBuilder._get_librdkafka_connection_config",
            self._get_librdkafka_connection_config,
        ):
            source = self.source(app.config, source_topic)

        app.add_source(source, destination_topic)

        # Produce data to the external kafka
        data = list(range(self.NUMBER_OF_MESSAGES))
        with Producer(broker_address=self._external_broker_address) as producer:
            for i in data:
                msg = source_topic_with_workspace.serialize("test", i)
                producer.produce(
                    topic=source_topic_with_workspace.name,
                    key=msg.key,
                    value=msg.value,
                )

        # Consume result data from main kafka
        results = []

        def _consume(config):
            try:
                with Consumer(
                    broker_address=config.broker_address,
                    consumer_group=config.consumer_group,
                    auto_offset_reset="earliest",
                    auto_commit_enable=False,
                    extra_config=config.consumer_extra_config,
                ) as consumer:
                    consumer.subscribe([destination_topic.name])
                    while True:
                        msg = consumer.poll(timeout=10)
                        if msg is None:
                            app.stop()
                            return

                        data = destination_topic.deserialize(msg)
                        results.append(data.value)

                        if data.value == self.NUMBER_OF_MESSAGES - 1:
                            app.stop()
                            return
            except BaseException:
                logger.exception("error")
                raise

        app.setup_topics()
        executor.submit(_consume, app.config)
        app._run()

        assert results == data
