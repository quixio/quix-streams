import os
import shutil
import uuid
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer
from quixstreams.models import TopicConfig
from quixstreams.state.exceptions import (
    PartitionStoreIsUsed,
    StoreNotRegisteredError,
    WindowedStoreAlreadyRegisteredError,
)
from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.rocksdb import RocksDBStore
from tests.utils import TopicPartitionStub


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
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
        # It's ok to rebalance partitions when there are no stores registered
        state_manager.on_partition_assign(
            state_id="topic", partition=0, committed_offsets={"topic": -1001}
        )
        state_manager.on_partition_revoke(state_id="topic", partition=0)

    def test_register_store(self, state_manager):
        state_manager = state_manager
        state_manager.register_store("my_topic", store_name="default")
        assert "default" in state_manager.stores["my_topic"]

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
        committed_offsets = {"topic1": -1001, "topic2": -1001}
        for tp in partitions:
            store_partitions.extend(
                state_manager.on_partition_assign(
                    state_id=tp.topic,
                    partition=tp.partition,
                    committed_offsets=committed_offsets,
                )
            )
        assert len(store_partitions) == 3

        assert len(state_manager.get_store("topic1", "store1").partitions) == 1
        assert len(state_manager.get_store("topic1", "store2").partitions) == 1
        assert len(state_manager.get_store("topic2", "store1").partitions) == 1

        for tp in partitions:
            state_manager.on_partition_revoke(state_id=tp.topic, partition=tp.partition)

        assert not state_manager.get_store("topic1", "store1").partitions
        assert not state_manager.get_store("topic1", "store2").partitions
        assert not state_manager.get_store("topic2", "store1").partitions

    def test_register_store_twice(self, state_manager):
        state_manager.register_store("topic", "store")
        state_manager.register_store("topic", "store")

    def test_register_windowed_store_twice(self, state_manager):
        state_manager.register_windowed_store("state_id", "store")
        with pytest.raises(WindowedStoreAlreadyRegisteredError):
            state_manager.register_windowed_store("state_id", "store")

    def test_get_store_not_registered(self, state_manager):
        with pytest.raises(StoreNotRegisteredError):
            state_manager.get_store("topic", "store")

    def test_clear_stores_when_empty(self, state_manager):
        state_manager.clear_stores()
        assert not state_manager.stores

    def test_clear_stores(self, state_manager):
        # Register stores
        state_manager.register_store(
            "topic1", store_name="store1", store_type=RocksDBStore
        )
        state_manager.register_store(
            "topic1", store_name="extra_store", store_type=RocksDBStore
        )
        state_manager.register_store(
            "topic2", store_name="store1", store_type=RocksDBStore
        )

        # Define partitions
        partitions = [
            TopicPartitionStub("topic1", 0),
            TopicPartitionStub("topic1", 1),
            TopicPartitionStub("topic2", 0),
        ]

        # Assign partitions
        for tp in partitions:
            state_manager.on_partition_assign(
                state_id=tp.topic,
                partition=tp.partition,
                committed_offsets={"topic1": -1001, "topic2": -1001},
            )

        # Collect paths of stores to be deleted
        stores_to_delete = [
            store_partition.path
            for topic_stores in state_manager.stores.values()
            for store in topic_stores.values()
            for store_partition in store.partitions.values()
        ]

        # Revoke partitions
        for tp in partitions:
            state_manager.on_partition_revoke(state_id=tp.topic, partition=tp.partition)

        # Act - Delete stores
        state_manager.clear_stores()

        # Assert store paths are deleted
        for path in stores_to_delete:
            assert not os.path.exists(path), f"RocksDB store at {path} was not deleted"

    def test_clear_stores_fails(self, state_manager):
        # Register stores
        state_manager.register_store("topic1", store_name="store1")

        # Assign the partition
        state_manager.on_partition_assign(
            state_id="topic1", partition=0, committed_offsets={"topic1": -1001}
        )

        # Act - Delete stores
        with pytest.raises(PartitionStoreIsUsed):
            state_manager.clear_stores()

    def test_clear_stores_deleted_dir(self, state_manager_factory, tmp_path):
        group_id = str(uuid.uuid4())
        base_dir_path = tmp_path / "state"

        with state_manager_factory(
            group_id=group_id, state_dir=str(base_dir_path)
        ) as st:
            shutil.rmtree(base_dir_path, ignore_errors=True)
            st.clear_stores()


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestStateStoreManagerWithRecovery:
    def test_rebalance_partitions_stores_not_registered(
        self,
        state_manager_factory,
        recovery_manager_factory,
        producer,
    ):
        state_manager = state_manager_factory(
            recovery_manager=recovery_manager_factory(),
            producer=producer,
        )
        # It's ok to rebalance partitions when there are no stores registered
        state_manager.on_partition_assign(
            state_id="topic", partition=0, committed_offsets={"topic": -1001}
        )
        state_manager.on_partition_revoke(state_id="topic", partition=0)

    def test_register_store(
        self,
        state_manager_factory,
        recovery_manager_factory,
        topic_manager_factory,
        producer,
    ):
        topic_manager = topic_manager_factory()
        recovery_manager = recovery_manager_factory(topic_manager=topic_manager)
        state_manager = state_manager_factory(
            recovery_manager=recovery_manager, producer=producer
        )

        # Create a topic
        topic = topic_manager.topic(name="topic1")

        # Register a store
        store_name = "default"
        state_manager.register_store(
            topic.name,
            store_name=store_name,
            changelog_config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        # Check that the store is registered
        assert store_name in state_manager.stores[topic.name]
        # Check that changelog topic is created
        assert store_name in topic_manager.changelog_topics[topic.name]

    def test_assign_revoke_partitions_stores_registered(
        self,
        state_manager_factory,
        recovery_manager_factory,
        topic_manager_factory,
        producer,
    ):
        topic_manager = topic_manager_factory()
        consumer = MagicMock(spec_set=Consumer)
        consumer.get_watermark_offsets.return_value = (0, 10)

        recovery_manager = recovery_manager_factory(
            topic_manager=topic_manager, consumer=consumer
        )
        state_manager = state_manager_factory(
            recovery_manager=recovery_manager, producer=producer
        )
        topic_name = "topic1"
        partition = 0
        store_name = "store1"

        # Register a store
        state_manager.register_store(
            topic_name,
            store_name=store_name,
            changelog_config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        # Mock the Consumer assignment with changelog topic-partition
        changelog_topic = topic_manager.changelog_topics[topic_name][store_name]
        changelog_tp = TopicPartition(topic=changelog_topic.name, partition=0)
        consumer.assignment.return_value = [changelog_tp]

        # Assign a topic partition
        state_manager.on_partition_assign(
            state_id=topic_name,
            partition=partition,
            committed_offsets={"topic1": -1001},
        )

        # Check that RecoveryManager has a partition assigned
        assert recovery_manager.partitions

        # Revoke a topic partition
        state_manager.on_partition_revoke(state_id=topic_name, partition=partition)

        # Check that RecoveryManager has a partition revoked too
        assert not recovery_manager.partitions
