import os
import contextlib
import uuid
from unittest.mock import patch

import pytest
import rocksdict
from tests.utils import TopicPartitionStub

from quixstreams.state.exceptions import (
    StoreNotRegisteredError,
    InvalidStoreTransactionStateError,
    PartitionStoreIsUsed,
    WindowedStoreAlreadyRegisteredError,
)


class TestStateStoreManager:
    def test_init_close(self, state_manager_factory):
        with state_manager_factory():
            ...

    def test_init_state_dir_exists_success(self, state_manager_factory, tmp_path):
        group_id = str(uuid.uuid4())
        base_dir_path = tmp_path / "state"
        base_dir_path.mkdir(parents=True)
        (base_dir_path / group_id).mkdir()

        with state_manager_factory(group_id=group_id, state_dir=str(base_dir_path)):
            ...

    def test_init_state_dir_exists_not_a_dir_fails(
        self, state_manager_factory, tmp_path
    ):
        group_id = str(uuid.uuid4())
        base_dir_path = tmp_path / "state"
        base_dir_path.mkdir()
        (base_dir_path / group_id).touch()

        with pytest.raises(FileExistsError):
            with state_manager_factory(group_id=group_id, state_dir=str(base_dir_path)):
                ...

    def test_rebalance_partitions_stores_not_registered(self, state_manager):
        tp = TopicPartitionStub("topic", 0)
        # It's ok to rebalance partitions when there are no stores registered
        state_manager.on_partition_assign(tp)
        state_manager.on_partition_revoke(tp)
        state_manager.on_partition_lost(tp)

    def test_assign_revoke_partitions_stores_registered(self, state_manager):
        state_manager.register_store("topic1", store_name="store1")
        state_manager.register_store("topic1", store_name="store2")
        state_manager.register_store("topic2", store_name="store1")

        stores_list = [s for d in state_manager.stores.values() for s in d.values()]
        assert len(stores_list) == 3

        partitions = [
            TopicPartitionStub("topic1", 0),
            TopicPartitionStub("topic2", 0),
        ]

        store_partitions = []
        for tp in partitions:
            store_partitions.extend(state_manager.on_partition_assign(tp))
        assert len(store_partitions) == 3

        assert len(state_manager.get_store("topic1", "store1").partitions) == 1
        assert len(state_manager.get_store("topic1", "store2").partitions) == 1
        assert len(state_manager.get_store("topic2", "store1").partitions) == 1

        for tp in partitions:
            state_manager.on_partition_revoke(tp)

        assert not state_manager.get_store("topic1", "store1").partitions
        assert not state_manager.get_store("topic1", "store2").partitions
        assert not state_manager.get_store("topic2", "store1").partitions

    def test_assign_lose_partitions_stores_registered(self, state_manager):
        state_manager.register_store("topic1", store_name="store1")
        state_manager.register_store("topic1", store_name="store2")
        state_manager.register_store("topic2", store_name="store1")

        stores_list = [s for d in state_manager.stores.values() for s in d.values()]
        assert len(stores_list) == 3

        partitions = [
            TopicPartitionStub("topic1", 0),
            TopicPartitionStub("topic2", 0),
        ]

        for tp in partitions:
            state_manager.on_partition_assign(tp)
        assert len(state_manager.get_store("topic1", "store1").partitions) == 1
        assert len(state_manager.get_store("topic1", "store2").partitions) == 1
        assert len(state_manager.get_store("topic2", "store1").partitions) == 1

        for tp in partitions:
            state_manager.on_partition_lost(tp)

        assert not state_manager.get_store("topic1", "store1").partitions
        assert not state_manager.get_store("topic1", "store2").partitions
        assert not state_manager.get_store("topic2", "store1").partitions

    def test_register_store_twice(self, state_manager):
        state_manager.register_store("topic", "store")
        state_manager.register_store("topic", "store")

    def test_register_windowed_store_twice(self, state_manager):
        state_manager.register_windowed_store("topic", "store")
        with pytest.raises(WindowedStoreAlreadyRegisteredError):
            state_manager.register_windowed_store("topic", "store")

    def test_get_store_not_registered(self, state_manager):
        with pytest.raises(StoreNotRegisteredError):
            state_manager.get_store("topic", "store")

    def test_clear_stores_when_empty(self, state_manager):
        state_manager.clear_stores()
        assert not state_manager.stores

    def test_clear_stores(self, state_manager):
        # Register stores
        state_manager.register_store("topic1", store_name="store1")
        state_manager.register_store("topic1", store_name="extra_store")
        state_manager.register_store("topic2", store_name="store1")

        # Define partitions
        partitions = [
            TopicPartitionStub("topic1", 0),
            TopicPartitionStub("topic1", 1),
            TopicPartitionStub("topic2", 0),
        ]

        # Assign partitions
        for tp in partitions:
            state_manager.on_partition_assign(tp)

        # Collect paths of stores to be deleted
        stores_to_delete = [
            store_partition.path
            for topic_stores in state_manager.stores.values()
            for store in topic_stores.values()
            for store_partition in store.partitions.values()
        ]

        # Revoke partitions
        for tp in partitions:
            state_manager.on_partition_revoke(tp)

        # Act - Delete stores
        state_manager.clear_stores()

        # Assert store paths are deleted
        for path in stores_to_delete:
            assert not os.path.exists(path), f"RocksDB store at {path} was not deleted"

    def test_clear_stores_fails(self, state_manager):
        # Register stores
        state_manager.register_store("topic1", store_name="store1")

        # Define the partition
        partition = TopicPartitionStub("topic1", 0)

        # Assign the partition
        state_manager.on_partition_assign(partition)

        # Act - Delete stores
        with pytest.raises(PartitionStoreIsUsed):
            state_manager.clear_stores()

    def test_store_transaction_success(self, state_manager):
        state_manager.register_store("topic", "store")
        tp = TopicPartitionStub("topic", 0)
        state_manager.on_partition_assign(tp)

        store = state_manager.get_store("topic", "store")
        store_partition = store.partitions[0]

        assert store_partition.get_processed_offset() is None

        with state_manager.start_store_transaction("topic", partition=0, offset=1):
            tx = state_manager.get_store_transaction("store")
            tx.set("some_key", "some_value")

        state_manager.on_partition_assign(tp)

        store = state_manager.get_store("topic", "store")
        store_partition = store.partitions[0]

        assert store_partition.get_processed_offset() == 1

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
                rocksdict.WriteBatch, "put", side_effect=ValueError("test")
            ):
                tx_store1.set("some_key", "some_value")
            tx_store2.set("some_key", "some_value")

        assert store1.partitions[0].get_processed_offset() is None
        assert store2.partitions[0].get_processed_offset() is None

    def test_get_store_transaction_store_not_registered_fails(self, state_manager):
        with pytest.raises(StoreNotRegisteredError):
            with state_manager.start_store_transaction("topic", 0, 0):
                ...

    def test_get_store_transaction_not_started(self, state_manager):
        with pytest.raises(InvalidStoreTransactionStateError):
            state_manager.get_store_transaction("store")

    def test_start_store_transaction_already_started(self, state_manager):
        state_manager.register_store("topic", "store")
        with state_manager.start_store_transaction("topic", partition=0, offset=0):
            with pytest.raises(InvalidStoreTransactionStateError):
                with state_manager.start_store_transaction(
                    "topic", partition=0, offset=0
                ):
                    ...
