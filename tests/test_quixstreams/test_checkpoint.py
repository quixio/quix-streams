import contextlib
from unittest.mock import patch

import pytest

from quixstreams.state.exceptions import (
    StoreNotRegisteredError,
    InvalidStoreTransactionStateError,
)
from quixstreams.state.rocksdb import RocksDBPartitionTransaction
from tests.utils import TopicPartitionStub


@pytest.mark.skip("Checkpoint tests")
class TestCheckpoint:
    def test_get_store_transaction_store_not_registered_fails(self, state_manager):
        with pytest.raises(StoreNotRegisteredError):
            with state_manager.start_store_transaction("topic", 0, 0):
                ...

    def test_get_store_transaction_not_started(self, state_manager):
        with pytest.raises(InvalidStoreTransactionStateError):
            state_manager.get_store_transaction("store")

    def test_store_transaction_success(self, state_manager):
        state_manager.register_store("topic", "store")
        tp = TopicPartitionStub("topic", 0)
        state_manager.on_partition_assign(tp)

        store = state_manager.get_store("topic", "store")
        store_partition = store.partitions[0]

        assert store_partition.get_processed_offset() is None

        with state_manager.start_store_transaction("topic", partition=0, offset=1):
            tx = state_manager.get_store_transaction("store")
            tx.set("some_key", "some_value", prefix=b"__key__")

        state_manager.on_partition_assign(tp)

        store = state_manager.get_store("topic", "store")
        store_partition = store.partitions[0]

        assert store_partition.get_processed_offset() == 1

    def test_store_transaction_no_flush_if_partition_transaction_failed(
        self, state_manager
    ):
        """
        Ensure that no PartitionTransactions are flushed to the DB if
        any of them fails
        """
        state_manager.register_store("topic", "store1")
        state_manager.register_store("topic", "store2")
        state_manager.on_partition_assign(TopicPartitionStub("topic", 0))
        store1 = state_manager.get_store("topic", "store1")
        store2 = state_manager.get_store("topic", "store2")

        with state_manager.start_store_transaction("topic", partition=0, offset=1):
            tx_store1 = state_manager.get_store_transaction("store1")
            tx_store2 = state_manager.get_store_transaction("store2")
            # Simulate exception in one of the transactions
            with contextlib.suppress(ValueError), patch.object(
                RocksDBPartitionTransaction,
                "_serialize_key",
                side_effect=ValueError("test"),
            ):
                tx_store1.set("some_key", "some_value")
            tx_store2.set("some_key", "some_value")

        assert store1.partitions[0].get_processed_offset() is None
        assert store2.partitions[0].get_processed_offset() is None

    def test_start_store_transaction_already_started(self, state_manager):
        state_manager.register_store("topic", "store")
        with state_manager.start_store_transaction("topic", partition=0, offset=0):
            with pytest.raises(InvalidStoreTransactionStateError):
                with state_manager.start_store_transaction(
                    "topic", partition=0, offset=0
                ):
                    ...

    def test_store_transaction_no_flush_on_exception(self, state_manager):
        state_manager.register_store("topic", "store")
        state_manager.on_partition_assign(TopicPartitionStub("topic", 0))
        store = state_manager.get_store("topic", "store")

        with contextlib.suppress(Exception):
            with state_manager.start_store_transaction("topic", partition=0, offset=1):
                tx = state_manager.get_store_transaction("store")
                tx.set("some_key", "some_value")
                raise ValueError()

        store_partition = store.partitions[0]
        assert store_partition.get_processed_offset() is None


@pytest.mark.skip("Checkpoint tests")
class TestCheckpointChangelog:
    def test_store_transaction_no_flush_on_exception(
        self,
        state_manager_changelogs,
    ):
        state_manager = state_manager_changelogs
        recovery_manager = state_manager._recovery_manager
        topic_manager = recovery_manager._topic_manager
        producer = state_manager._producer
        consumer = recovery_manager._consumer

        consumer.get_watermark_offsets.return_value = (0, 10)
        topic_manager.topic(name="topic")
        state_manager.register_store("topic", store_name="store")
        state_manager.on_partition_assign(TopicPartitionStub("topic", 0))
        store = state_manager.get_store("topic", "store")

        with contextlib.suppress(Exception):
            with state_manager.start_store_transaction("topic", partition=0, offset=1):
                tx = state_manager.get_store_transaction("store")
                tx.set("some_key", "some_value")
                raise ValueError()

        store_partition = store.partitions[0]
        assert store_partition.get_processed_offset() is None
        assert store_partition.get_changelog_offset() is None
        producer.produce.assert_not_called()

    def test_store_transaction_no_flush_if_partition_transaction_failed(
        self,
        state_manager_changelogs,
    ):
        """
        Ensure that no PartitionTransactions are flushed to the DB if
        any of them fails
        """
        state_manager = state_manager_changelogs
        recovery_manager = state_manager._recovery_manager
        topic_manager = recovery_manager._topic_manager
        producer = state_manager._producer
        consumer = recovery_manager._consumer

        consumer.get_watermark_offsets.return_value = (0, 10)
        topic_manager.topic(name="topic")
        state_manager.register_store("topic", store_name="store1")
        state_manager.register_store("topic", store_name="store2")
        state_manager.on_partition_assign(TopicPartitionStub("topic", 0))

        store1 = state_manager.get_store("topic", "store1")
        store2 = state_manager.get_store("topic", "store2")

        with state_manager.start_store_transaction("topic", partition=0, offset=1):
            tx_store1 = state_manager.get_store_transaction("store1")
            tx_store2 = state_manager.get_store_transaction("store2")
            # Simulate exception in one of the transactions
            with contextlib.suppress(ValueError), patch.object(
                RocksDBPartitionTransaction,
                "_serialize_key",
                side_effect=ValueError("test"),
            ):
                tx_store1.set("some_key", "some_value")
            tx_store2.set("some_key", "some_value")

        assert store1.partitions[0].get_processed_offset() is None
        assert store1.partitions[0].get_changelog_offset() is None
        assert store2.partitions[0].get_processed_offset() is None
        assert store2.partitions[0].get_changelog_offset() is None
        producer.produce.assert_not_called()
