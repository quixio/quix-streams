from typing import List

from confluent_kafka import KafkaException, TopicPartition

from quixstreams.checkpointing import BaseCheckpoint
from quixstreams.checkpointing.exceptions import (
    CheckpointConsumerCommitError,
    CheckpointProducerTimeout,
)
from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka.consumer import BaseConsumer
from quixstreams.models.topics import Topic


class Checkpoint(BaseCheckpoint):
    """
    Checkpoint implementation used by the KafkaReplicatorSource
    """

    def __init__(
        self,
        producer: InternalProducer,
        producer_topic: Topic,
        consumer: BaseConsumer,
        commit_interval: float,
        commit_every: int = 0,
        flush_timeout: float = 10,
        exactly_once: bool = False,
    ):
        super().__init__(commit_interval, commit_every)

        self._producer = producer
        self._producer_topic = producer_topic
        self._consumer = consumer
        self._flush_timeout = flush_timeout
        self._exactly_once = exactly_once

        if self._exactly_once:
            self._producer.begin_transaction()

    def close(self):
        """
        Perform cleanup (when the checkpoint is empty) instead of committing.

        Needed for exactly-once, as Kafka transactions are timeboxed.
        """
        if self._exactly_once:
            self._producer.abort_transaction()

    def commit(self):
        """
        Commit the checkpoint.

        This method will:
         1. Flush the producer to ensure everything is delivered.
         2. Commit topic offsets.
        """
        unproduced_msg_count = self._producer.flush(self._flush_timeout)
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
            for (_, partition), offset in self._tp_offsets.items()
        ]
        self._tp_offsets = {}

        try:
            self._commit(offsets=offsets)
        except KafkaException as e:
            raise CheckpointConsumerCommitError(e.args[0]) from None

    def _commit(self, offsets: List[TopicPartition]):
        if self._exactly_once:
            self._producer.commit_transaction(
                offsets, self._consumer.consumer_group_metadata()
            )
        else:
            partitions = self._consumer.commit(offsets=offsets, asynchronous=False)
            if partitions:
                for partition in partitions:
                    if partition.error:
                        raise CheckpointConsumerCommitError(partition.error)
