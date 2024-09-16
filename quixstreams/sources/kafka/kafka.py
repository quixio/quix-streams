import time
import logging

from typing import Union, Optional, Dict, TYPE_CHECKING

from confluent_kafka import TopicPartition, Message

from quixstreams.kafka import Consumer, ConnectionConfig, AutoOffsetReset
from quixstreams.kafka.exceptions import KafkaConsumerException
from quixstreams.error_callbacks import default_on_consumer_error, ConsumerErrorCallback
from quixstreams.models.topics import TopicConfig, TopicAdmin, Topic
from quixstreams.models.serializers import DeserializerType
from quixstreams.sources import Source

from .checkpoint import Checkpoint

if TYPE_CHECKING:
    from quixstreams.app import ApplicationConfig


logger = logging.getLogger(__name__)

__all__ = ["KafkaSource"]


class KafkaSource(Source):
    """
    Source implementation that replicates a topic from a Kafka broker to your application broker.

    Running multiple instances of this source is supported.

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources.kafka import KafkaSource

    app = Application(
        consumer_group="group",
    )

    source = KafkaSource(
        name="source-second-kafka",
        app_config=app.config,
        topic="second-kafka-topic",
        broker_address="localhost:9092",
    )

    sdf = app.dataframe(source=source)
    sdf = sdf.print()
    app.run(sdf)
    ```
    """

    def __init__(
        self,
        name: str,
        app_config: "ApplicationConfig",
        topic: str,
        broker_address: Union[str, ConnectionConfig],
        consumer_group: Optional[str] = None,
        auto_offset_reset: Optional[AutoOffsetReset] = None,
        consumer_extra_config: Optional[dict] = None,
        consumer_poll_timeout: Optional[float] = None,
        shutdown_timeout: float = 10,
        on_consumer_error: Optional[ConsumerErrorCallback] = default_on_consumer_error,
        value_deserializer: DeserializerType = "json",
        key_deserializer: DeserializerType = "bytes",
    ) -> None:
        """
        :param name: The source unique name. Used to generate the default topic name.
        :param app_config: The configuration of the application. Used by the source to connect to the application kafka broker.
        :param topic: The topic to replicate.
        :param broker_address: The connection settings for the source Kafka.
        :param consumer_group: The source Kafka consumer group
            Default - The source name
        :param auto_offset_reset: Consumer `auto.offset.reset` setting.
            Default - Use the Application `auto_offset_reset` setting.
        :param consumer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
            Default - `None`
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`
            Default - Use the Application `consumer_poll_timeout` setting.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown.
        :param on_consumer_error: Triggered when the source `Consumer` fails to poll Kafka.
        :param value_deserializer: The default topic value deserializer, used by StreamingDataframe connected to the source.
            Default - `json`
        :param key_deserializer: The default topic key deserializer, used by StreamingDataframe connected to the source.
            Default - `json`
        """
        super().__init__(name, shutdown_timeout)

        if consumer_extra_config is None:
            consumer_extra_config = {}

        if consumer_group is None:
            consumer_group = name

        if auto_offset_reset is None:
            auto_offset_reset = app_config.auto_offset_reset

        self._config = app_config
        self._topic = topic
        self._on_consumer_error = on_consumer_error
        self._consumer_poll_timeout = (
            consumer_poll_timeout or self._config.consumer_poll_timeout
        )
        self._flush_timeout = consumer_extra_config.get(
            "max.poll.interval.ms", self._config.flush_timeout
        )
        self._value_deserializer = value_deserializer
        self._key_deserializer = key_deserializer
        self._broker_address = broker_address
        self._consumer_group = consumer_group
        self._auto_offset_reset = auto_offset_reset
        self._consumer_extra_config = consumer_extra_config

        self._running = True
        self._error: Optional[Exception] = None
        self._checkpoint: Optional[Checkpoint] = None

        self._source_cluster_consumer: Optional[Consumer] = None
        self._source_cluster_admin: Optional[TopicAdmin] = None

        self._target_cluster_consumer: Optional[Consumer] = None
        self._target_cluster_admin: Optional[TopicAdmin] = None

    def run(self) -> None:
        self._source_cluster_consumer = Consumer(
            broker_address=self._broker_address,
            consumer_group=self._consumer_group,
            auto_offset_reset=self._auto_offset_reset,
            auto_commit_enable=False,
            extra_config=self._consumer_extra_config,
        )
        self._source_cluster_admin = TopicAdmin(
            broker_address=self._broker_address,
            extra_config=self._consumer_extra_config,
        )

        self._target_cluster_consumer = Consumer(
            broker_address=self._config.broker_address,
            consumer_group=f"{self._config.consumer_group}-offsets",
            auto_offset_reset=self._config.auto_offset_reset,
            auto_commit_enable=False,
            extra_config=self._config.consumer_extra_config,
        )
        self._target_cluster_admin = TopicAdmin(
            broker_address=self._config.broker_address,
            extra_config=self._config.consumer_extra_config,
        )

        self._validate_topics()

        self._source_cluster_consumer.subscribe(
            topics=[self._topic],
            on_assign=self.on_assign,
            on_lost=self.on_lost,
            on_revoke=self.on_revoke,
        )

        super().run()

        self.init_checkpoint()
        try:
            while self._running:
                if self._error:
                    raise self._error

                msg = self.poll_source()
                if msg is None:
                    continue

                self.produce_message(msg)
                self.commit_checkpoint()
        except Exception:
            self._checkpoint.close()
            raise

        self.commit_checkpoint(force=True)

    def produce_message(self, msg: Message):
        topic_name, partition, offset = msg.topic(), msg.partition(), msg.offset()
        self._checkpoint.store_offset(topic_name, partition, offset)
        self.produce(
            value=msg.value(),
            key=msg.key(),
            headers=msg.headers(),
            timestamp=msg.timestamp()[1],
            partition=partition,
        )

    def poll_source(self) -> Optional[Message]:
        try:
            msg = self._source_cluster_consumer.poll(
                timeout=self._consumer_poll_timeout
            )
        except Exception as exc:
            if self._on_consumer_error(exc, None, logger):
                self._producer.poll()
                return
            raise

        if msg is None:
            self._producer.poll()
            return

        try:
            if err := msg.error():
                raise KafkaConsumerException(error=err)
        except Exception as exc:
            if self._on_consumer_error(exc, msg, logger):
                return
            raise

        return msg

    def commit_checkpoint(self, force: bool = False) -> None:
        if not self._checkpoint.expired() and not force:
            return

        if self._checkpoint.empty():
            self._checkpoint.close()
        else:
            logger.debug("Committing checkpoint")
            start = time.monotonic()
            self._checkpoint.commit()
            elapsed = round(time.monotonic() - start, 2)
            logger.debug(f"Checkpoint commited in {elapsed}s")

        self.init_checkpoint()

    def init_checkpoint(self) -> None:
        self._checkpoint = Checkpoint(
            producer=self._producer,
            producer_topic=self._producer_topic,
            consumer=self._target_cluster_consumer,
            commit_every=self._config.commit_every,
            commit_interval=self._config.commit_interval,
            flush_timeout=self._flush_timeout,
            exactly_once=self._config.exactly_once,
        )

    def _validate_topics(self) -> None:
        source_topic_config = self._source_cluster_admin.inspect_topics(
            topic_names=[self._topic], timeout=self._config.request_timeout
        ).get(self._topic)

        if source_topic_config is None:
            raise ValueError(f"Source topic {self._topic} not found")

        logger.debug(
            "source topic %s configuration: %s", self._topic, source_topic_config
        )

        target_topic_config = self._target_cluster_admin.inspect_topics(
            topic_names=[self._producer_topic.name],
            timeout=self._config.request_timeout,
        ).get(self._producer_topic.name)

        if target_topic_config is None:
            raise ValueError(f"Destination topic {self._producer_topic.name} not found")

        logger.debug(
            "destination topic %s configuration: %s",
            self._producer_topic.name,
            target_topic_config,
        )

        if source_topic_config.num_partitions > target_topic_config.num_partitions:
            raise ValueError("Source topic has more partitions than destination topic")
        elif source_topic_config.num_partitions < target_topic_config.num_partitions:
            logger.warning("Source topic has less partitions than destination topic")

    def _target_cluster_offsets(self, partitions) -> Dict[int, int]:
        partitions = [
            TopicPartition(
                topic=self._producer_topic.name, partition=partition.partition
            )
            for partition in partitions
        ]
        partitions_commited = self._target_cluster_consumer.committed(
            partitions, timeout=self._config.request_timeout
        )
        a = {partition.partition: partition.offset for partition in partitions_commited}
        return a

    def on_assign(self, _, source_partitions) -> None:
        try:
            target_cluster_offset = self._target_cluster_offsets(source_partitions)
            for partition in source_partitions:
                partition.offset = target_cluster_offset.get(partition.partition, None)
                logger.debug(
                    "using offset %s for topic partition %s[%s]",
                    partition.offset,
                    partition.topic,
                    partition.partition,
                )

            self._source_cluster_consumer.incremental_assign(source_partitions)
        except Exception as exc:
            logger.exception("Error assigning partitions")
            self._error = exc

    def on_revoke(self, _, partitions) -> None:
        if not self._error:
            self.commit_checkpoint()

    def on_lost(self, _, partitions) -> None:
        pass

    def stop(self) -> None:
        super().stop()
        self._running = False

    def cleanup(self, failed: bool) -> None:
        super().cleanup(failed)
        self._source_cluster_consumer.close()
        self._target_cluster_consumer.close()

    def default_topic(self) -> Topic:
        admin = TopicAdmin(
            broker_address=self._broker_address,
            extra_config=self._consumer_extra_config,
        )

        config = admin.inspect_topics(
            topic_names=[self._topic], timeout=self._config.request_timeout
        ).get(self._topic)
        if config is None:
            config = TopicConfig(num_partitions=1, replication_factor=1)

        return Topic(
            name=self.name,
            value_serializer="bytes",
            key_serializer="bytes",
            value_deserializer=self._value_deserializer,
            key_deserializer=self._key_deserializer,
            config=config,
        )
