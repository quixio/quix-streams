import contextlib
from typing import Optional
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import TopicPartition, KafkaException, KafkaError

from quixstreams.checkpointing import Checkpoint, InvalidStoredOffset
from quixstreams.checkpointing.exceptions import (
    CheckpointProducerTimeout,
    CheckpointConsumerCommitError,
)
from quixstreams.kafka import Consumer
from quixstreams.processing import PausingManager
from quixstreams.rowproducer import RowProducer
from quixstreams.sinks import SinkManager, BatchingSink, SinkBackpressureError
from quixstreams.sinks.base import SinkBatch
from quixstreams.state import StateStoreManager
from quixstreams.state.exceptions import StoreNotRegisteredError, StoreTransactionFailed
from quixstreams.state.rocksdb import RocksDBPartitionTransaction
from tests.utils import DummySink


@pytest.fixture()
def checkpoint_factory(state_manager, consumer, row_producer_factory):
    def factory(
        commit_interval: float = 1,
        commit_every: int = 0,
        consumer_: Optional[Consumer] = None,
        producer_: Optional[RowProducer] = None,
        state_manager_: Optional[StateStoreManager] = None,
        sink_manager_: Optional[SinkManager] = None,
        pausing_manager_: Optional[PausingManager] = None,
        exactly_once: bool = False,
    ):
        consumer_ = consumer_ or consumer
        sink_manager_ = sink_manager_ or SinkManager()
        pausing_manager_ = pausing_manager_ or PausingManager(consumer=consumer)
        producer_ = producer_ or row_producer_factory(transactional=exactly_once)
        state_manager_ = state_manager_ or state_manager
        return Checkpoint(
            commit_interval=commit_interval,
            commit_every=commit_every,
            producer=producer_,
            consumer=consumer_,
            state_manager=state_manager_,
            sink_manager=sink_manager_,
            pausing_manager=pausing_manager_,
            exactly_once=exactly_once,
        )

    return factory


@pytest.fixture()
def rowproducer_mock(request):
    p = MagicMock(spec_set=RowProducer)
    p.flush.return_value = getattr(request, "param", 0)
    return p


class BackpressuredSink(BatchingSink):
    def write(self, batch: SinkBatch):
        raise SinkBackpressureError(
            retry_after=999, topic=batch.topic, partition=batch.partition
        )


class FailingSink(BatchingSink):
    def write(self, batch: SinkBatch):
        raise ValueError("Sink write failed")


class TestCheckpoint:
    def test_empty_true(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        assert checkpoint.empty()

    def test_empty_false(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        checkpoint.store_offset("topic", 0, 0)
        assert not checkpoint.empty()

    def test_exactly_once_init(self, checkpoint_factory):
        mock_producer = MagicMock()
        checkpoint_factory(producer_=mock_producer, exactly_once=True)
        mock_producer.begin_transaction.assert_called()

    @pytest.mark.parametrize("commit_interval, expired", [(0, True), (999, False)])
    def test_expired_with_commit_interval(
        self, commit_interval, expired, checkpoint_factory
    ):
        checkpoint = checkpoint_factory(commit_interval=commit_interval)
        assert checkpoint.expired() == expired

    def test_expired_with_commit_every(self, checkpoint_factory):
        checkpoint = checkpoint_factory(commit_interval=999, commit_every=2)
        checkpoint.store_offset("topic", 0, 0)
        assert not checkpoint.expired()

        checkpoint.store_offset("topic", 0, 1)
        assert checkpoint.expired()

    def test_expired_with_commit_every_and_commit_interval(self, checkpoint_factory):
        checkpoint = checkpoint_factory(commit_interval=0, commit_every=10)
        checkpoint.store_offset("topic", 0, 0)
        assert checkpoint.expired()

    def test_store_already_processed_offset_fails(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        checkpoint.store_offset("topic", 0, 10)
        with pytest.raises(InvalidStoredOffset):
            checkpoint.store_offset("topic", 0, 10)

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_no_state_success(
        self, checkpoint_factory, consumer, state_manager, topic_factory, exactly_once
    ):
        topic_name, _ = topic_factory()
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager, exactly_once=exactly_once
        )
        processed_offset = 999
        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)

        checkpoint.commit()
        tp, *_ = consumer.committed([TopicPartition(topic=topic_name, partition=0)])
        assert tp.offset == processed_offset + 1

    def test_commit_with_state_no_changelog_success(
        self,
        checkpoint_factory,
        consumer,
        state_manager_factory,
        topic_factory,
        rowproducer_mock,
    ):
        topic_name, _ = topic_factory()

        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager, producer_=rowproducer_mock
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        # Do some state updates and store the processed offset to simulate processing
        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the offset is committed
        tp, *_ = consumer.committed([TopicPartition(topic=topic_name, partition=0)])
        assert tp.offset == processed_offset + 1

        # Check the producer is flushed
        assert rowproducer_mock.flush.call_count == 1

        # Check the state is flushed
        assert tx.completed
        new_tx = store.start_partition_transaction(0)
        assert new_tx.get(key=key, prefix=prefix) == value

        # No changelogs should be flushed
        assert not store_partition.get_changelog_offset()
        # Processed offset should be stored
        assert store_partition.get_processed_offset() == processed_offset

    def test_commit_with_state_with_changelog_success(
        self,
        checkpoint_factory,
        row_producer,
        consumer,
        state_manager_factory,
        recovery_manager_factory,
        topic_factory,
    ):
        topic_name, _ = topic_factory()
        recovery_manager = recovery_manager_factory(consumer=consumer)
        state_manager = state_manager_factory(
            producer=row_producer, recovery_manager=recovery_manager
        )
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager, producer_=row_producer
        )
        processed_offset = 999
        value, prefix = "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        # Do a couple of state updates to send more messages to the changelog
        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key="key1", value=value, prefix=prefix)
        tx.set(key="key2", value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the state is flushed
        assert tx.completed

        # Check the changelog offset
        # The changelog offset should increase by number of updated keys
        # Since no offset recorded yet, an increase of 2 from no offset is 1
        assert store_partition.get_changelog_offset() == 1
        assert store_partition.get_processed_offset() == 999

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_with_state_and_changelog_no_updates_success(
        self,
        checkpoint_factory,
        row_producer_factory,
        consumer,
        state_manager_factory,
        recovery_manager_factory,
        topic_factory,
        exactly_once,
    ):
        topic_name, _ = topic_factory()
        row_producer = row_producer_factory(transactional=exactly_once)
        recovery_manager = recovery_manager_factory(consumer=consumer)
        state_manager = state_manager_factory(
            producer=row_producer, recovery_manager=recovery_manager
        )
        checkpoint = checkpoint_factory(
            consumer_=consumer,
            state_manager_=state_manager,
            producer_=row_producer,
            exactly_once=exactly_once,
        )
        processed_offset = 999
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        # Create a transaction but don't update any keys
        tx = checkpoint.get_store_transaction(topic_name, 0)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the transaction is not flushed
        assert tx.completed

        # The changelog and processed offsets should be empty because no updates
        # happened during the transaction
        assert not store_partition.get_changelog_offset()
        assert not store_partition.get_processed_offset()

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_close_no_offsets(
        self,
        checkpoint_factory,
        rowproducer_mock,
        exactly_once,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = MagicMock(spec_set=StateStoreManager)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=rowproducer_mock,
            exactly_once=exactly_once,
        )
        # Commit the checkpoint without processing any messages
        checkpoint.close()

        if exactly_once:
            # transaction should also be aborted
            assert rowproducer_mock.abort_transaction.call_count
        else:
            assert not rowproducer_mock.abort_transaction.call_count

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_has_failed_transactions_fails(
        self,
        checkpoint_factory,
        state_manager_factory,
        topic_factory,
        rowproducer_mock,
        exactly_once,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=rowproducer_mock,
            exactly_once=exactly_once,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store("topic", "default")
        store = state_manager.get_store("topic", "default")
        store.assign_partition(0)

        # Simulate a failed transaction
        tx = checkpoint.get_store_transaction("topic", 0)
        with contextlib.suppress(ValueError), patch.object(
            RocksDBPartitionTransaction,
            "_serialize_key",
            side_effect=ValueError("test"),
        ):
            tx.set(key=key, value=value, prefix=prefix)
        assert tx.failed

        # Store offset to simulate processing
        checkpoint.store_offset("topic", 0, processed_offset)

        # Checkpoint commit should fail if any of the transaction is failed
        # but the original exception was swallowed by an error callback
        with pytest.raises(StoreTransactionFailed):
            checkpoint.commit()

        # The producer should not flush
        assert not rowproducer_mock.flush.call_count

        # Check nothing is committed
        assert not rowproducer_mock.commit_transaction.call_count
        assert not consumer_mock.commit.call_count

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_producer_flush_fails(
        self,
        checkpoint_factory,
        state_manager_factory,
        topic_factory,
        rowproducer_mock,
        exactly_once,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=rowproducer_mock,
            exactly_once=exactly_once,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store("topic", "default")
        store = state_manager.get_store("topic", "default")
        store.assign_partition(0)

        # Do some state updates and store the processed offset to simulate processing
        tx = checkpoint.get_store_transaction("topic", 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset("topic", 0, processed_offset)

        rowproducer_mock.flush.side_effect = ValueError("Flush failure")
        # Checkpoint commit should fail if producer failed to flush
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Nothing should commit
        assert not rowproducer_mock.commit_transaction.call_count
        assert not consumer_mock.commit.call_count
        # The transaction should remain prepared, but not completed
        assert tx.prepared
        assert not tx.completed

    def test_commit_consumer_commit_fails(
        self, checkpoint_factory, state_manager_factory, topic_factory, rowproducer_mock
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=rowproducer_mock,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store("topic", "default")
        store = state_manager.get_store("topic", "default")
        store.assign_partition(0)

        # Do some state updates and store the processed offset to simulate processing
        tx = checkpoint.get_store_transaction("topic", 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset("topic", 0, processed_offset)

        consumer_mock.commit.side_effect = ValueError("Commit failure")
        # Checkpoint commit should fail if consumer failed to commit
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Producer should flush
        assert rowproducer_mock.flush.call_count
        # The transaction should remain prepared, but not completed
        assert tx.prepared
        assert not tx.completed

    def test_get_store_transaction_store_not_registered_fails(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        with pytest.raises(StoreNotRegisteredError):
            with checkpoint.get_store_transaction("topic", 0, "default"):
                ...

    def test_get_store_transaction_success(self, checkpoint_factory, state_manager):
        state_manager.register_store("topic", "default")
        store = state_manager.get_store("topic", "default")
        store.assign_partition(0)

        checkpoint = checkpoint_factory(state_manager_=state_manager)
        tx = checkpoint.get_store_transaction("topic", 0, "default")
        assert tx
        tx2 = checkpoint.get_store_transaction("topic", 0, "default")
        assert tx2 is tx

    @pytest.mark.parametrize("rowproducer_mock", [1], indirect=True)
    def test_incomplete_flush(
        self, checkpoint_factory, consumer, state_manager_factory, rowproducer_mock
    ):

        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager, producer_=rowproducer_mock
        )
        checkpoint.store_offset("topic", 0, 0)

        with pytest.raises(CheckpointProducerTimeout) as err:
            checkpoint.commit()

        assert (
            str(err.value)
            == "'1' messages failed to be produced before the producer flush timeout"
        )

    def test_failed_commit(
        self, checkpoint_factory, state_manager_factory, rowproducer_mock
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        consumer_mock.commit.side_effect = KafkaException(KafkaError(1, "test error"))

        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=rowproducer_mock,
        )
        checkpoint.store_offset("topic", 0, 0)

        with pytest.raises(CheckpointConsumerCommitError) as err:
            checkpoint.commit()

        assert (
            str(err.value)
            == '<CheckpointConsumerCommitError code="1" description="test error">'
        )

    def test_failed_commit_partition(
        self, checkpoint_factory, state_manager_factory, rowproducer_mock
    ):
        consumer_mock = MagicMock(spec_set=Consumer)

        topic_partition = MagicMock(spec=TopicPartition)
        topic_partition.error = KafkaError(1, "test error")

        consumer_mock.commit.return_value = [topic_partition]
        state_manager = state_manager_factory(producer=rowproducer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=rowproducer_mock,
        )
        checkpoint.store_offset("topic", 0, 0)

        with pytest.raises(CheckpointConsumerCommitError) as err:
            checkpoint.commit()

        assert (
            str(err.value)
            == '<CheckpointConsumerCommitError code="1" description="test error">'
        )

    def test_commit_with_sink_success(
        self,
        topic_factory,
        consumer,
        state_manager,
        checkpoint_factory,
        state_manager_factory,
        rowproducer_mock,
    ):
        topic_name, _ = topic_factory()
        sink_manager = SinkManager()
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager, sink_manager_=sink_manager
        )

        processed_offset = 999
        value, key, timestamp, headers = "value", "key", 1, []
        # Create two dummy sinks
        sink1 = DummySink()
        sink2 = DummySink()
        # Register sinks and add messages to them
        for sink in (sink1, sink2):
            sink_manager.register(sink)
            sink.add(
                value=value,
                key=key,
                timestamp=timestamp,
                topic=topic_name,
                partition=0,
                headers=headers,
                offset=processed_offset,
            )

        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)
        checkpoint.commit()

        # Ensure that both sinks has been flushed
        for sink in (sink1, sink2):
            assert len(sink.results) == 1
            sink_result = sink.results[0]
            assert sink_result.value == value
            assert sink_result.key == key
            assert sink_result.timestamp == timestamp
            assert sink_result.headers == headers

    def test_commit_with_sink_fails(
        self,
        topic_factory,
        consumer,
        state_manager,
        checkpoint_factory,
        state_manager_factory,
        rowproducer_mock,
    ):
        topic_name, _ = topic_factory()
        sink_manager = SinkManager()
        checkpoint = checkpoint_factory(
            consumer_=consumer,
            state_manager_=state_manager,
            sink_manager_=sink_manager,
        )

        # Create a failing sink and register it
        sink = FailingSink()
        sink_manager.register(sink)

        processed_offset = 999
        value, key, timestamp, headers = "value", "key", 1, []
        sink.add(
            value=value,
            key=key,
            timestamp=timestamp,
            topic=topic_name,
            partition=0,
            headers=headers,
            offset=processed_offset,
        )

        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Ensure that the error in Sink is propagated and fails the checkpoint
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Ensure that the offset has not been committed
        committed, *_ = consumer.committed(
            [TopicPartition(topic=topic_name, partition=0)]
        )
        assert committed.offset == -1001
