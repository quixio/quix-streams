import contextlib
from typing import Optional
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import TopicPartition

from quixstreams.checkpointing import Checkpoint, InvalidStoredOffset
from quixstreams.kafka import Consumer
from quixstreams.rowproducer import RowProducer
from quixstreams.state import StateStoreManager
from quixstreams.state.exceptions import StoreNotRegisteredError, StoreTransactionFailed
from quixstreams.state.rocksdb import RocksDBPartitionTransaction


@pytest.fixture()
def checkpoint_factory(state_manager, consumer, row_producer):
    def factory(
        commit_interval: float = 1,
        consumer_: Optional[Consumer] = None,
        producer_: Optional[RowProducer] = None,
        state_manager_: Optional[StateStoreManager] = None,
    ):
        return Checkpoint(
            commit_interval=commit_interval,
            producer=producer_ or row_producer,
            consumer=consumer_ or consumer,
            state_manager=state_manager_ or state_manager,
        )

    return factory


class TestCheckpoint:
    def test_empty_true(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        assert checkpoint.empty()

    def test_empty_false(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        checkpoint.store_offset("topic", 0, 0)
        assert not checkpoint.empty()

    @pytest.mark.parametrize("commit_interval, expired", [(0, True), (999, False)])
    def test_expired(self, commit_interval, expired, checkpoint_factory):
        checkpoint = checkpoint_factory(commit_interval=commit_interval)
        assert checkpoint.expired() == expired

    def test_store_already_processed_offset_fails(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        checkpoint.store_offset("topic", 0, 10)
        with pytest.raises(InvalidStoredOffset):
            checkpoint.store_offset("topic", 0, 9)

    def test_commit_no_state_success(
        self, checkpoint_factory, consumer, state_manager, topic_factory
    ):
        topic_name, _ = topic_factory()
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager
        )
        processed_offset = 999
        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)

        checkpoint.commit()
        tp, *_ = consumer.committed([TopicPartition(topic=topic_name, partition=0)])
        assert tp.offset == processed_offset + 1

    def test_commit_with_state_no_changelog_success(
        self, checkpoint_factory, consumer, state_manager_factory, topic_factory
    ):
        topic_name, _ = topic_factory()
        producer_mock = MagicMock(spec_set=RowProducer)
        state_manager = state_manager_factory(producer=producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer, state_manager_=state_manager, producer_=producer_mock
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
        assert producer_mock.flush.call_count == 1

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
        # The changelog offset must be equal to a number of updated keys
        assert store_partition.get_changelog_offset() == 2
        assert store_partition.get_processed_offset() == 999

    def test_commit_with_state_and_changelog_no_updates_success(
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

        # Create a transaction but don't update any keys
        tx = checkpoint.get_store_transaction(topic_name, 0)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the transaction is not flushed
        assert tx.completed

        # The changelog and processed offsets should be empty because no updates
        # happend during the transaction
        assert not store_partition.get_changelog_offset()
        assert not store_partition.get_processed_offset()

    def test_commit_no_offsets_stored_noop(
        self, checkpoint_factory, state_manager_factory, topic_factory
    ):
        topic_name, _ = topic_factory()
        producer_mock = MagicMock(spec_set=RowProducer)
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=producer_mock,
        )
        # Commit the checkpoint without processing any messages
        checkpoint.commit()

        # Check nothing is committed
        assert not consumer_mock.commit.call_count
        assert not producer_mock.flush.call_count

    def test_commit_has_failed_transactions_fails(
        self, checkpoint_factory, state_manager_factory, topic_factory
    ):
        producer_mock = MagicMock(spec_set=RowProducer)
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=producer_mock,
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
        assert not producer_mock.flush.call_count
        # Consumer should not commit
        assert not consumer_mock.commit.call_count

    def test_commit_producer_flush_fails(
        self, checkpoint_factory, state_manager_factory, topic_factory
    ):
        producer_mock = MagicMock(spec_set=RowProducer)
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=producer_mock,
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

        producer_mock.flush.side_effect = ValueError("Flush failure")
        # Checkpoint commit should fail if producer failed to flush
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Consumer should not commit
        assert not consumer_mock.commit.call_count
        # The transaction should remain prepared, but not completed
        assert tx.prepared
        assert not tx.completed

    def test_commit_consumer_commit_fails(
        self, checkpoint_factory, state_manager_factory, topic_factory
    ):
        producer_mock = MagicMock(spec_set=RowProducer)
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=producer_mock,
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
        assert producer_mock.flush.call_count
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
