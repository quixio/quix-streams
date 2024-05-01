from unittest.mock import patch, MagicMock

from confluent_kafka import TopicPartition as ConfluentPartition

from quixstreams.kafka import Consumer
from quixstreams.models import TopicManager, TopicConfig
from quixstreams.state import RecoveryPartition
from quixstreams.state.rocksdb import RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import CHANGELOG_CF_MESSAGE_HEADER
from tests.utils import ConfluentKafkaMessageStub


class TestRecoveryManager:
    def test_register_changelog(self, recovery_manager_factory):
        recovery_manager = recovery_manager_factory()

        store_name = "my_store"
        kwargs = dict(
            topic_name="my_topic_name",
            consumer_group="my_group",
        )
        with patch.object(TopicManager, "changelog_topic") as make_changelog:
            recovery_manager.register_changelog(**kwargs, store_name=store_name)

        make_changelog.assert_called_with(**kwargs, store_name=store_name)

    def test_assign_partition(
        self, state_manager_factory, recovery_manager_factory, topic_manager_factory
    ):
        """
        Check that RecoveryManager.assign_partition() assigns proper changelog topic
        partition and pauses the consumer.
        """

        store_name = "default"
        # Stored changelog offset is between lowwater and highwater, so the
        # given store partition needs to be recovered.
        lowwater, highwater = 0, 20
        stored_changelog_offset = 15

        topic_name = "topic_name"
        partition_num = 0

        consumer = MagicMock(spec_set=Consumer)
        topic_manager = topic_manager_factory()
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )
        state_manager = state_manager_factory(recovery_manager=recovery_manager)

        # Create a topic
        topic_manager.topic(topic_name)
        # Mock the topic watermarks
        consumer.get_watermark_offsets.side_effect = [(lowwater, highwater)]
        # Mock the current assignment with some values
        assignment = [1, 2, 3]
        consumer.assignment.return_value = assignment

        # Create Store and assign a StorePartition (which also sets up changelog topics)
        store_partitions = {}
        state_manager.register_store(topic_name=topic_name, store_name=store_name)
        store = state_manager.get_store(topic=topic_name, store_name=store_name)
        partition = store.assign_partition(partition_num)
        store_partitions[store_name] = partition

        # Assign a RecoveryPartition
        with patch.object(
            RocksDBStorePartition,
            "get_changelog_offset",
            return_value=stored_changelog_offset,
        ):
            recovery_manager.assign_partition(
                topic=topic_name,
                partition=partition_num,
                store_partitions=store_partitions,
                committed_offset=-1001,
            )

        # Check the changelog topic partition is assigned to the consumer
        assert consumer.incremental_assign.call_count == 1
        assigned_changelog_partitions = consumer.incremental_assign.call_args[0][0]
        assert len(assigned_changelog_partitions) == 1

        # Check the changelog topic partition properties
        changelog_partition = assigned_changelog_partitions[0]
        changelog_topic_name = topic_manager.changelog_topics[topic_name][
            store_name
        ].name
        assert changelog_partition.topic == changelog_topic_name
        assert changelog_partition.partition == partition_num
        assert changelog_partition.offset == stored_changelog_offset

        # Check that RecoveryPartition is assigned to RecoveryManager
        assert len(recovery_manager.partitions[partition_num]) == 1

        # Check that consumer paused all assigned partitions
        consumer.pause.assert_called_with(assignment)

    def test_assign_partition_fix_offset_only(
        self,
        recovery_manager_factory,
        recovery_partition_factory,
        topic_manager_factory,
    ):
        """
        Try to recover store partition with changelog offset AHEAD of the watermark.
        The offset should be adjusted in this case, but recovery should not be triggered
        """

        topic_name = "topic_name"
        partition_num = 0
        store_name = "default"
        consumer_group = "group"
        lowwater, highwater = 0, 20

        # Register a source topic and a changelog topic with one partition
        topic_manager = topic_manager_factory()
        topic_manager.topic(topic_name)
        topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name, consumer_group=consumer_group
        )

        # Mock Consumer
        consumer = MagicMock(spec_set=Consumer)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        consumer.assignment.return_value = "assignments"

        # Mock StorePartition
        changelog_offset = 22
        store_partition = MagicMock(spec_set=RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = changelog_offset

        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        with patch.object(RecoveryPartition, "update_offset") as update_offset:
            recovery_manager.assign_partition(
                topic=topic_name,
                partition=partition_num,
                store_partitions={store_name: store_partition},
                committed_offset=-1001,
            )

        # "update_offset()" should be called
        update_offset.assert_called()

        # No pause or assignments should happen
        consumer.pause.assert_not_called()
        consumer.incremental_assign.assert_not_called()

    def test_assign_partitions_during_recovery(
        self,
        recovery_manager_factory,
        recovery_partition_factory,
        topic_manager_factory,
    ):
        """
        Check that RecoveryManager pauses only the source topic partition if
        another partition is already recovering.
        """

        topic_name = "topic_name"
        consumer_group = "group"
        store_name = "default"
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"
        changelog_offset = 5
        lowwater, highwater = 0, 10
        assignment = [0, 1]

        # Register a source topic and a changelog topic with 2 partitions
        topic_manager = topic_manager_factory()
        topic_manager.topic(
            topic_name, config=TopicConfig(num_partitions=2, replication_factor=1)
        )
        topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name, consumer_group=consumer_group
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign first partition that needs recovery
        store_partition = MagicMock(spec_set=RocksDBStorePartition)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        store_partition.get_changelog_offset.return_value = changelog_offset
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offset=-1001,
            store_partitions={store_name: store_partition},
        )
        assert recovery_manager.partitions
        assert recovery_manager.partitions[0][changelog_name].needs_recovery

        # Put a RecoveryManager into "recovering" state
        recovery_manager._running = True
        assert recovery_manager.recovering

        # Assign second partition that also needs recovery
        store_partition = MagicMock(spec_set=RocksDBStorePartition)
        store_partition.get_changelog_offset.return_value = 5
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=1,
            committed_offset=-1001,
            store_partitions={store_name: store_partition},
        )
        assert recovery_manager.partitions
        assert recovery_manager.partitions[1][changelog_name].needs_recovery

        # Check that consumer first paused all partitions
        assert consumer.pause.call_args_list[0].args[0] == assignment

        # Check that consumer paused only source topic partition when the second
        # recovery partition was assigned
        assert consumer.pause.call_args_list[1].args[0] == [
            ConfluentPartition(
                topic=topic_name,
                partition=1,
                offset=-1001,
            )
        ]

    def test_revoke_partition(self, recovery_manager_factory, topic_manager_factory):
        """
        Revoke a topic partition's respective recovery partitions.
        """
        topic_name = "topic_name"
        consumer_group = "group"
        store_name = "default"
        changelog_offset = 5
        lowwater, highwater = 0, 10
        assignment = [0, 1]
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"

        # Register a source topic and a changelog topic with two partitions
        topic_manager = topic_manager_factory()
        topic_manager.topic(
            topic_name, config=TopicConfig(num_partitions=2, replication_factor=1)
        )
        topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name, consumer_group=consumer_group
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign partitions that need recovery
        store_partition = MagicMock(spec_set=RocksDBStorePartition)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        store_partition.get_changelog_offset.return_value = changelog_offset
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offset=-1001,
            store_partitions={store_name: store_partition},
        )
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=1,
            committed_offset=-1001,
            store_partitions={store_name: store_partition},
        )
        assert len(recovery_manager.partitions) == 2

        # Revoke one partition
        recovery_manager.revoke_partition(0)
        assert len(recovery_manager.partitions) == 1
        # Check that consumer unassigned the changelog topic partition as well
        assert consumer.incremental_unassign.call_args.args[0] == [
            ConfluentPartition(topic=changelog_name, partition=0)
        ]

        # Revoke second partition
        recovery_manager.revoke_partition(1)
        # Check that consumer unassigned the changelog topic partition as well
        assert consumer.incremental_unassign.call_args.args[0] == [
            ConfluentPartition(topic=changelog_name, partition=1)
        ]
        # Check that no partitions are assigned
        assert not recovery_manager.partitions

    def test_revoke_partition_no_partitions_assigned(self, recovery_manager_factory):
        """
        Skip revoking any recovery partitions for a given partition since none are
        currently assigned (due to not needing recovery).
        """
        consumer = MagicMock(spec_set=Consumer)
        recovery_manager = recovery_manager_factory(consumer=consumer)
        recovery_manager.revoke_partition(partition_num=0)
        assert not consumer.incremental_unassign.call_count

    def test_do_recovery(
        self, recovery_manager_factory, topic_manager_factory, rocksdb_partition
    ):
        """
        Test that RecoveryManager.do_recovery():
         - resumes the recovering changelog partition
         - applies changes to the StorePartition
         - revokes the RecoveryPartition after recovery is done
         - unassigns the changelog partition
         - unpauses source topic partitions
        """
        topic_name = "topic_name"
        consumer_group = "group"
        store_name = "default"
        lowwater, highwater = 0, 10
        assignment = [0, 1]
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"

        changelog_message = ConfluentKafkaMessageStub(
            topic=changelog_name,
            partition=0,
            offset=highwater - 1,
            key=b"key",
            value=b"value",
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
        )

        # Register a source topic and a changelog topic with one partition
        topic_manager = topic_manager_factory()
        topic_manager.topic(topic_name)
        topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name, consumer_group=consumer_group
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        consumer.poll.return_value = changelog_message
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign a partition that needs recovery
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offset=-1001,
            store_partitions={store_name: rocksdb_partition},
        )

        # Trigger a recovery
        recovery_manager.do_recovery()

        # Check that consumer first resumed the changelog topic partition
        consumer_resume_calls = consumer.resume.call_args_list
        assert consumer_resume_calls[0].args[0] == [
            ConfluentPartition(topic=changelog_name, partition=0)
        ]
        # Check that consumer resumed all assigned partitions after recovery is done
        assert consumer_resume_calls[1].args[0] == assignment

        # Check that RecoveryPartitions are unassigned
        assert not recovery_manager.partitions

    def test_do_recovery_no_partitions_assigned(self, recovery_manager_factory):
        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        recovery_manager = recovery_manager_factory(consumer=consumer)
        # Trigger a recovery
        recovery_manager.do_recovery()

        # Check that consumer.poll() is not called
        assert not consumer.poll.called
