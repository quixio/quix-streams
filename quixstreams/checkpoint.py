import logging
import time
from typing import Dict, Tuple

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer, Producer
from quixstreams.state import (
    StateStoreManager,
    PartitionTransaction,
    DEFAULT_STATE_STORE_NAME,
)

logger = logging.getLogger(__name__)


# TODO: Tests
class Checkpoint:
    """
    Class to keep track of state updates and consumer offsets and to checkpoint these
    updates on schedule.
    """

    def __init__(
        self,
        commit_interval: float,
        producer: Producer,
        consumer: Consumer,
        state_manager: StateStoreManager,
    ):
        self._created_at = time.monotonic()
        self._tp_offsets: Dict[Tuple[str, int], int] = {}

        # A mapping of <(topic, partition, store_name): PartitionTransaction>
        self._store_transactions: Dict[(str, int, str), PartitionTransaction] = {}
        # Ensure the checkpoint is not negative.
        # Passing zero or lower will flush the checkpoint after each processed message
        self._commit_interval = max(commit_interval, 0)
        self._state_manager = state_manager
        self._consumer = consumer
        self._producer = producer

        # TODO: Can the checkpoint object be reused?
        #  Do we need to validate that it can't?

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
         1. Flush the changelogs for each state store and ensure everything is produced.
         2. Commit topic offsets.
         3. Flush each state store partition to the disk.

        """
        # TODO: Error handling

        # 0. Produce the changelogs
        # for (
        #     topic,
        #     partition,
        #     store_name,
        # ), transaction in self._store_transactions.items():
        #     offset = self._tp_offsets[(topic, partition)]
        #     # TODO: Flush the changelogs. Call it "prepare"?
        #     if transaction.failed:
        #         raise
        #     transaction.prepare(offset=offset)

        # 1. Flush producer
        # TODO: Check if all messages are flushed successfully
        # TODO: Take the produced changelog offsets
        # TODO: Logs
        self._producer.flush()

        # 2. Commit offsets to Kafka
        offsets = [
            TopicPartition(topic=topic, partition=partition, offset=offset + 1)
            for (topic, partition), offset in self._tp_offsets.items()
        ]
        if offsets:
            self._consumer.commit(offsets=offsets, asynchronous=False)

        # 3. Flush state store partitions to the disk
        for (
            topic,
            partition,
            store_name,
        ), transaction in self._store_transactions.items():
            offset = self._tp_offsets.get((topic, partition))
            if offset is not None:
                transaction.maybe_flush(offset=offset)

        # TODO: Remove when the new changelog producer is implemented
        self._producer.flush()
