import logging
import threading

from typing import Union, Optional, List, TYPE_CHECKING

from confluent_kafka import TopicPartition
from quixstreams.kafka import Consumer, ConnectionConfig, AutoOffsetReset
from quixstreams.models import Topic
from quixstreams.models.messages import KafkaMessage

from quixstreams.platforms.quix import QuixKafkaConfigsBuilder
from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout

from .base import PollingSource

if TYPE_CHECKING:
    from quixstreams.app import Application

logger = logging.getLogger(__name__)


class ExternalKafkaSource(PollingSource):

    def __init__(
        self,
        name: str,
        app: "Application",
        topic: str,
        consumer_group: str,
        broker_address: Optional[Union[str, ConnectionConfig]] = None,
        quix_sdk_token: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        consumer_extra_config: Optional[dict] = None,
        shutdown_timeout: int = 10,
    ) -> None:
        super().__init__(name, shutdown_timeout)

        if not quix_sdk_token and not broker_address:
            raise ValueError("One of `quix_sdk_token` or `broker_address` is required")

        if consumer_extra_config is None:
            consumer_extra_config = {}

        if quix_sdk_token:
            quix_app_config = QuixKafkaConfigsBuilder(
                quix_sdk_token=quix_sdk_token
            ).get_application_config(consumer_group)
            consumer_extra_config.update(quix_app_config.librdkafka_extra_config)
            self._source_cluster_consumer = Consumer(
                broker_address=quix_app_config.librdkafka_connection_config,
                consumer_group=quix_app_config.consumer_group,
                auto_offset_reset=auto_offset_reset,
                extra_config=consumer_extra_config,
                auto_commit_enable=False,
            )
        else:
            self._source_cluster_consumer = Consumer(
                broker_address=broker_address,
                consumer_group=consumer_group,
                auto_offset_reset=auto_offset_reset,
                extra_config=consumer_extra_config,
                auto_commit_enable=False,
            )

        self._topic = topic
        self._consumer_group = consumer_group
        self._target_cluster_consumer = app.get_consumer(
            auto_commit_enable=False, consummer_group=f"_offsets_{self.name}"
        )

        self._lock = threading.Lock()
        self._tp_offsets = {}
        self._assign_error: Optional[Exception] = None

    def deserialize(self, msg):
        return KafkaMessage(
            key=msg.key(),
            value=msg.value(),
            headers=msg.headers(),
            timestamp=msg.timestamp()[1],
        )

    def run(self):
        self._source_cluster_consumer.subscribe(
            topics=[self._topic], on_assign=self.on_assign
        )
        super().run()

    def _target_partitions(self, partitions):
        partitions = [
            TopicPartition(
                topic=self._producer_topic.name, partition=partition.partition
            )
            for partition in partitions
        ]
        partitions_commited = self._target_cluster_consumer.committed(partitions)

        return {partition.partition: partition for partition in partitions_commited}

    def on_assign(self, _, source_partitions):
        try:
            target_partitions = self._target_partitions(source_partitions)
            for partition in source_partitions:
                partition.offset = target_partitions.get(
                    partition.partition, TopicPartition(offset=0, topic="", partition=0)
                ).offset
            self._source_cluster_consumer.incremental_assign(source_partitions)
        except Exception as exc:
            self._assign_error = exc
            raise

    def poll(self):
        if self._assign_error:
            raise self._assign_error

        msg = self._source_cluster_consumer.poll(timeout=1)
        if msg is None:
            return

        if err := msg.error():
            logger.info(err)
            raise ValueError(err)

        topic_name, partition, offset = msg.topic(), msg.partition(), msg.offset()
        with self._lock:
            self._tp_offsets[(topic_name, partition)] = offset

        return self.deserialize(msg)

    def checkpoint(self):
        if not self._tp_offsets:
            return

        with self._lock:
            unproduced_msg_count = self._producer.flush()
            if unproduced_msg_count > 0:
                raise CheckpointProducerTimeout(
                    f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
                )
            offsets = [
                TopicPartition(
                    topic=self._producer_topic.name,
                    partition=partition,
                    offset=offset + 1,
                )
                for (topic, partition), offset in self._tp_offsets.items()
            ]
            self._target_cluster_consumer.commit(offsets=offsets)

    def stop(self):
        super().stop()
        self._source_cluster_consumer.close()
        self._target_cluster_consumer.close()

    def default_topic(self, app) -> Topic:
        return app.topic(
            self.name,
            value_serializer="bytes",
            value_deserializer="json",
        )
