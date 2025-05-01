import logging
import time
from abc import abstractmethod
from typing import Dict, Tuple

from confluent_kafka import KafkaException, TopicPartition

from quixstreams.dataframe import DataFrameRegistry
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.internal_producer import InternalProducer
from quixstreams.sinks import SinkManager
from quixstreams.sinks.base import SinkBackpressureError
from quixstreams.state import (
    DEFAULT_STATE_STORE_NAME,
    PartitionTransaction,
    StateStoreManager,
)
from quixstreams.state.exceptions import StoreTransactionFailed

from .exceptions import (
    CheckpointConsumerCommitError,
    CheckpointProducerTimeout,
    InvalidStoredOffset,
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
        self._store_transactions: Dict[Tuple[str, int, str], PartitionTransaction] = {}
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
        producer: InternalProducer,
        consumer: InternalConsumer,
        state_manager: StateStoreManager,
        sink_manager: SinkManager,
        dataframe_registry: DataFrameRegistry,
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
        self._dataframe_registry = dataframe_registry
        self._exactly_once = exactly_once
        if self._exactly_once:
            self._producer.begin_transaction()

    def get_store_transaction(
        self, stream_id: str, partition: int, store_name: str = DEFAULT_STATE_STORE_NAME
    ) -> PartitionTransaction:
        """
        Get a PartitionTransaction for the given store, topic and partition.

        It will return already started transaction if there's one.

        :param stream_id: stream id
        :param partition: partition number
        :param store_name: store name
        :return: instance of `PartitionTransaction`
        """
        transaction = self._store_transactions.get((stream_id, partition, store_name))
        if transaction is not None:
            return transaction

        store = self._state_manager.get_store(
            stream_id=stream_id, store_name=store_name
        )
        transaction = store.start_partition_transaction(partition=partition)

        self._store_transactions[(stream_id, partition, store_name)] = transaction
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
         1. Flush the registered sinks if any
         2. Produce the changelogs for each state store
         3. Flush the producer to ensure everything is delivered.
         4. Commit topic offsets.
         5. Flush each state store partition to the disk.
        """

        # Step 1. Flush sinks
        logger.debug("Checkpoint: flushing sinks")
        backpressured = False
        for sink in self._sink_manager.sinks:
            if backpressured:
                # Drop the accumulated data for the other sinks
                # if one of them is backpressured to limit the number of duplicates
                # when the data is reprocessed again
                sink.on_paused()
                continue

            try:
                sink.flush()
            except SinkBackpressureError as exc:
                logger.warning(
                    f'Backpressure for sink "{sink}" is detected, '
                    f"all partitions will be paused and resumed again "
                    f"in {exc.retry_after}s"
                )
                # The backpressure is detected from the sink
                # Pause the assignment to let it cool down and seek it back to
                # the first processed offsets of this Checkpoint (it must be equal
                # to the last committed offset).
                self._consumer.trigger_backpressure(
                    resume_after=exc.retry_after,
                    offsets_to_seek=self._starting_tp_offsets.copy(),
                )
                backpressured = True
        if backpressured:
            # Exit early if backpressure is detected
            return

        # Step 2. Produce the changelogs
        for (
            stream_id,
            partition,
            store_name,
        ), transaction in self._store_transactions.items():
            topics = self._dataframe_registry.get_topics_for_stream_id(
                stream_id=stream_id
            )
            processed_offsets = {
                topic: offset
                for (topic, partition_), offset in self._tp_offsets.items()
                if topic in topics and partition_ == partition
            }
            if transaction.failed:
                raise StoreTransactionFailed(
                    f'Detected a failed transaction for store "{store_name}", '
                    f"the checkpoint is aborted"
                )
            transaction.prepare(processed_offsets=processed_offsets)

        # Step 3. Flush producer to trigger all delivery callbacks and ensure that
        # all messages are produced
        logger.debug("Checkpoint: flushing producer")
        unproduced_msg_count = self._producer.flush()
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before "
                f"the producer flush timeout"
            )

        # Step 4. Commit offsets to Kafka
        offsets = [
            TopicPartition(topic=topic, partition=partition, offset=offset + 1)
            for (topic, partition), offset in self._tp_offsets.items()
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
        for transaction in self._store_transactions.values():
            # Get the changelog topic-partition for the given transaction
            # It can be None if changelog topics are disabled in the app config
            changelog_tp = transaction.changelog_topic_partition
            # The changelog offset also can be None if no updates happened
            # during transaction
            changelog_offset = (
                produced_offsets.get(changelog_tp) if changelog_tp is not None else None
            )
            transaction.flush(changelog_offset=changelog_offset)
