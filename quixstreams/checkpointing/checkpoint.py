import logging
import time
from typing import Dict, Tuple

from confluent_kafka import TopicPartition, KafkaException

from quixstreams.kafka import Consumer
from quixstreams.rowproducer import RowProducer
from quixstreams.state import (
    StateStoreManager,
    PartitionTransaction,
    DEFAULT_STATE_STORE_NAME,
)
from quixstreams.state.exceptions import StoreTransactionFailed
from .exceptions import (
    InvalidStoredOffset,
    CheckpointProducerTimeout,
    CheckpointConsumerCommitError,
)

logger = logging.getLogger(__name__)


class Checkpoint:
    """
    Class to keep track of state updates and consumer offsets and to checkpoint these
    updates on schedule.
    """

    def __init__(
        self,
        commit_interval: float,
        producer: RowProducer,
        consumer: Consumer,
        state_manager: StateStoreManager,
    ):
        self._created_at = time.monotonic()
        # A mapping of <(topic, partition): processed offset>
        self._tp_offsets: Dict[Tuple[str, int], int] = {}

        # A mapping of <(topic, partition, store_name): PartitionTransaction>
        self._store_transactions: Dict[(str, int, str), PartitionTransaction] = {}
        # Passing zero or lower will flush the checkpoint after each processed message
        self._commit_interval = max(commit_interval, 0)
        self._state_manager = state_manager
        self._consumer = consumer
        self._producer = producer

    def expired(self) -> bool:
        """
        Returns `True` if checkpoint deadline has expired.
        """
        return (time.monotonic() - self._commit_interval) >= self._created_at

    def empty(self) -> bool:
        """
        Returns `True` if checkpoint doesn't have any offsets stored yet.
        :return:
        """
        return not bool(self._tp_offsets)

    def store_offset(self, topic: str, partition: int, offset: int):
        """
        Store the offset of the processed message to the checkpoint.

        :param topic: topic name
        :param partition: partition number
        :param offset: message offset
        """
        stored_offset = self._tp_offsets.get((topic, partition), -1)
        # A paranoid check to ensure that processed offsets always increase within the
        # same checkpoint.
        # It shouldn't normally happen, but a lot of logic relies on it,
        # and it's better to be safe.
        if offset < stored_offset:
            raise InvalidStoredOffset(
                f"Cannot store offset smaller or equal than already processed"
                f" one: {offset} <= {stored_offset}"
            )
        self._tp_offsets[(topic, partition)] = offset

    def get_store_transaction(
        self, topic: str, partition: int, store_name: str = DEFAULT_STATE_STORE_NAME
    ) -> PartitionTransaction:
        """
        Get a PartitionTransaction for the given store, topic and partition.

        It will return already started transaction if there's one.

        :param topic: topic name
        :param partition: partition number
        :param store_name: store name
        :return: instance of `PartitionTransaction`
        """
        transaction = self._store_transactions.get((topic, partition, store_name))
        if transaction is not None:
            return transaction

        store = self._state_manager.get_store(topic=topic, store_name=store_name)
        transaction = store.start_partition_transaction(partition=partition)

        self._store_transactions[(topic, partition, store_name)] = transaction
        return transaction

    def commit(self):
        """
        Commit the checkpoint.

        This method will:
         1. Produce the changelogs for each state store
         2. Flush the producer to ensure everything is delivered.
         3. Commit topic offsets.
         4. Flush each state store partition to the disk.
        """

        if not self._tp_offsets:
            # No messages have been processed during this checkpoint, return
            return

        # Step 1. Produce the changelogs
        for (
            topic,
            partition,
            store_name,
        ), transaction in self._store_transactions.items():
            offset = self._tp_offsets[(topic, partition)]
            if transaction.failed:
                raise StoreTransactionFailed(
                    f'Detected a failed transaction for store "{store_name}", '
                    f"the checkpoint is aborted"
                )
            transaction.prepare(processed_offset=offset)

        # Step 2. Flush producer to trigger all delivery callbacks and ensure that
        # all messages are produced
        logger.debug("Checkpoint: flushing producer")
        unproduced_msg_count = self._producer.flush()
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
            )

        # Get produced offsets after flushing the producer
        produced_offsets = self._producer.offsets

        # Step 3. Commit offsets to Kafka
        offsets = [
            TopicPartition(topic=topic, partition=partition, offset=offset + 1)
            for (topic, partition), offset in self._tp_offsets.items()
        ]
        logger.debug("Checkpoint: commiting consumer")
        try:
            partitions = self._consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as e:
            raise CheckpointConsumerCommitError(e.args[0]) from None

        for partition in partitions:
            if partition.error:
                raise CheckpointConsumerCommitError(partition.error)

        # Step 4. Flush state store partitions to the disk together with changelog
        # offsets
        for (
            topic,
            partition,
            store_name,
        ), transaction in self._store_transactions.items():
            offset = self._tp_offsets[(topic, partition)]

            # Get the changelog topic-partition for the given transaction
            # It can be None if changelog topics are disabled in the app config
            changelog_tp = transaction.changelog_topic_partition
            # The changelog offset also can be None if no updates happened
            # during transaction
            changelog_offset = (
                produced_offsets.get(changelog_tp) if changelog_tp is not None else None
            )
            if changelog_offset is not None:
                # Increment the changelog offset by one to match the high watermark
                # in Kafka
                changelog_offset += 1
            transaction.flush(
                processed_offset=offset, changelog_offset=changelog_offset
            )
