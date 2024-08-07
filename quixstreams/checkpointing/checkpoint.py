import logging
import time
from abc import abstractmethod
from typing import Dict, Tuple

from confluent_kafka import TopicPartition, KafkaException

from quixstreams.kafka import Consumer
from quixstreams.processing.pausing import PausingManager
from quixstreams.rowproducer import RowProducer
from quixstreams.sinks import SinkManager
from quixstreams.sinks.exceptions import SinkBackpressureError
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


class BaseCheckpoint:
    """
    Base class to keep track of state updates and consumer offsets and to checkpoint these
    updates on schedule.

    Two implementations exist:
        * one for checkpointing the Application in quixstreams/checkpoint/checkpoint.py
        * one for checkpointing the kafka source in quixstreams/sources/kafka/checkpoint.py
    """

    def __init__(
        self,
        commit_interval: float,
        commit_every: int = 0,
    ):
        self._created_at = time.monotonic()
        # A mapping of <(topic, partition): processed offset>
        self._tp_offsets: Dict[Tuple[str, int], int] = {}
        # A mapping of <(topic, partition): starting offset> with the first
        # processed offsets within the checkpoint
        self._starting_tp_offsets: Dict[Tuple[str, int], int] = {}
        # A mapping of <(topic, partition, store_name): PartitionTransaction>
        self._store_transactions: Dict[(str, int, str), PartitionTransaction] = {}
        # Passing zero or lower will flush the checkpoint after each processed message
        self._commit_interval = max(commit_interval, 0)

        self._commit_every = commit_every
        self._total_offsets_processed = 0

    def expired(self) -> bool:
        """
        Returns `True` if checkpoint deadline has expired OR
        if the total number of processed offsets exceeded the "commit_every" limit
        when it's defined.
        """
        return (time.monotonic() - self._commit_interval) >= self._created_at or (
            0 < self._commit_every <= self._total_offsets_processed
        )

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
        tp = (topic, partition)
        stored_offset = self._tp_offsets.get(tp, -1)
        # A paranoid check to ensure that processed offsets always increase within the
        # same checkpoint.
        # It shouldn't normally happen, but a lot of logic relies on it,
        # and it's better to be safe.
        if offset <= stored_offset:
            raise InvalidStoredOffset(
                f"Cannot store offset smaller or equal than already processed"
                f" one: {offset} <= {stored_offset}"
            )
        self._tp_offsets[tp] = offset
        # Track the first processed offset in the transaction to rewind back to it
        # in case of sink backpressure
        if tp not in self._starting_tp_offsets:
            self._starting_tp_offsets[tp] = offset
        self._total_offsets_processed += 1

    @abstractmethod
    def close(self):
        """
        Perform cleanup (when the checkpoint is empty) instead of committing.

        Needed for exactly-once, as Kafka transactions are timeboxed.
        """

    @abstractmethod
    def commit(self):
        """
        Commit the checkpoint.
        """
        pass


class Checkpoint(BaseCheckpoint):
    """
    Checkpoint implementation used by the application
    """

    def __init__(
        self,
        commit_interval: float,
        producer: RowProducer,
        consumer: Consumer,
        state_manager: StateStoreManager,
        sink_manager: SinkManager,
        pausing_manager: PausingManager,
        exactly_once: bool = False,
        commit_every: int = 0,
    ):
        super().__init__(
            commit_interval=commit_interval,
            commit_every=commit_every,
        )

        self._state_manager = state_manager
        self._consumer = consumer
        self._producer = producer
        self._sink_manager = sink_manager
        self._pausing_manager = pausing_manager
        self._exactly_once = exactly_once
        if self._exactly_once:
            self._producer.begin_transaction()

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
         1. Produce the changelogs for each state store
         2. Flush the producer to ensure everything is delivered.
         3. Commit topic offsets.
         4. Flush each state store partition to the disk.
        """

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
                f"'{unproduced_msg_count}' messages failed to be produced before "
                f"the producer flush timeout"
            )

        logger.debug("Checkpoint: flushing sinks")
        sinks = self._sink_manager.sinks
        # Step 3. Flush sinks
        for (topic, partition), offset in self._tp_offsets.items():
            for sink in sinks:
                if self._pausing_manager.is_paused(topic=topic, partition=partition):
                    # The topic-partition is paused, skip flushing other sinks for
                    # this TP.
                    # Note: when flushing multiple sinks for the same TP, some
                    # of them can be flushed before one of the sinks is backpressured.
                    sink.on_paused(topic=topic, partition=partition)
                    continue

                try:
                    sink.flush(topic=topic, partition=partition)
                except SinkBackpressureError as exc:
                    logger.warning(
                        f'Backpressure for sink "{sink}" is detected, '
                        f"the partition will be paused and resumed again "
                        f"in {exc.retry_after}s; "
                        f'partition="{topic}[{partition}]" '
                        f"processed_offset={offset}"
                    )
                    # The backpressure is detected from the sink
                    # Pause the partition to let it cool down and seek it back to
                    # the first processed offset of this Checkpoint (it must be equal
                    # to the last committed offset).
                    offset_to_seek = self._starting_tp_offsets[(topic, partition)]
                    self._pausing_manager.pause(
                        topic=topic,
                        partition=partition,
                        resume_after=exc.retry_after,
                        offset_to_seek=offset_to_seek,
                    )

        # Step 4. Commit offsets to Kafka
        # First, filter out offsets of the paused topic partitions.
        tp_offsets = {
            (topic, partition): offset
            for (topic, partition), offset in self._tp_offsets.items()
            if not self._pausing_manager.is_paused(topic=topic, partition=partition)
        }
        if not tp_offsets:
            # No offsets to commit because every partition is paused, exiting early
            return

        offsets = [
            TopicPartition(topic=topic, partition=partition, offset=offset + 1)
            for (topic, partition), offset in tp_offsets.items()
        ]

        if self._exactly_once:
            self._producer.commit_transaction(
                offsets, self._consumer.consumer_group_metadata()
            )
        else:
            logger.debug("Checkpoint: committing consumer")
            try:
                partitions = self._consumer.commit(offsets=offsets, asynchronous=False)
            except KafkaException as e:
                raise CheckpointConsumerCommitError(e.args[0]) from None

            for partition in partitions:
                if partition.error:
                    raise CheckpointConsumerCommitError(partition.error)

        # Step 5. Flush state store partitions to the disk together with changelog
        # offsets.
        # Get produced offsets after flushing the producer
        produced_offsets = self._producer.offsets
        for (
            topic,
            partition,
            store_name,
        ), transaction in self._store_transactions.items():
            offset = tp_offsets.get((topic, partition))
            # Offset can be None if the partition is paused
            if offset is None:
                continue

            # Get the changelog topic-partition for the given transaction
            # It can be None if changelog topics are disabled in the app config
            changelog_tp = transaction.changelog_topic_partition
            # The changelog offset also can be None if no updates happened
            # during transaction
            changelog_offset = (
                produced_offsets.get(changelog_tp) if changelog_tp is not None else None
            )
            transaction.flush(
                processed_offset=offset, changelog_offset=changelog_offset
            )
