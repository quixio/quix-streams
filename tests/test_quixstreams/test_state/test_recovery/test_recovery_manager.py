import logging
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import OFFSET_INVALID, TopicPartition
from confluent_kafka import TopicPartition as ConfluentPartition

from quixstreams.kafka import Consumer
from quixstreams.models import TopicConfig, TopicManager
from quixstreams.state.base import StorePartition
from quixstreams.state.exceptions import (
    ChangelogTopicPartitionNotAssigned,
    InvalidStoreChangelogOffset,
)
from quixstreams.state.manager import SUPPORTED_STORES
from quixstreams.state.metadata import CHANGELOG_CF_MESSAGE_HEADER, SEPARATOR
from quixstreams.state.recovery import RecoveryManager
from tests.utils import ConfluentKafkaMessageStub, Timeout


class RecoveryStalledAfterHighwater(AssertionError):
    """Recovery kept polling after reaching the end of the grown changelog."""


# Remove these strict expected failures when recovery accepts positions past its target.
# Use pytest --runxfail to expose the current bug as an ordinary failure.
_RECOVERY_OVERSHOOT = pytest.mark.xfail(
    strict=True,
    raises=RecoveryStalledAfterHighwater,
    reason="Recovery requires position == captured highwater even if the log grows",
)


class TestRecoveryManager:
    def test_register_changelog(self, recovery_manager_factory):
        recovery_manager = recovery_manager_factory()
        store_name = "my_store"
        topic = "my_topic_name"
        config = TopicConfig(num_partitions=1, replication_factor=1)
        with patch.object(TopicManager, "changelog_topic") as make_changelog:
            recovery_manager.register_changelog(
                stream_id=topic,
                store_name=store_name,
                topic_config=config,
            )

        make_changelog.assert_called_with(
            stream_id=topic, store_name=store_name, config=config
        )

    def test_assign_partition_invalid_offset(
        self,
        recovery_manager_factory,
        recovery_partition_factory,
        topic_manager_factory,
    ):
        """
        Try to recover store partition with changelog offset AHEAD of the watermark.
        This is invalid and should raise exception.
        """

        topic_name = str(uuid.uuid4())
        partition_num = 0
        store_name = "default"
        lowwater, highwater = 0, 20

        # Register a source topic and a changelog topic with one partition
        topic_manager = topic_manager_factory()
        topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=topic_manager.topic_config(),
        )

        # Mock Consumer
        consumer = MagicMock(spec_set=Consumer)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        # Mock the Consumer assignment with changelog topic-partition
        changelog_topic = topic_manager.changelog_topics[topic_name][store_name]
        changelog_tp = TopicPartition(topic=changelog_topic.name, partition=0)
        consumer.assignment.return_value = [changelog_tp]

        # Mock StorePartition
        changelog_offset = 22
        store_partition = MagicMock(spec_set=StorePartition)
        store_partition.get_changelog_offset.return_value = changelog_offset
        # No flipped-but-unfinished TTL migration: the invalid-offset
        # branch must be reached, not the forced-recovery migration path.
        store_partition.has_incomplete_ttl_migration.return_value = False

        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        with pytest.raises(InvalidStoreChangelogOffset):
            recovery_manager.assign_partition(
                topic=topic_name,
                partition=partition_num,
                store_partitions={store_name: store_partition},
                committed_offsets={topic_name: -1001},
            )

        # No pause or assignments should happen
        consumer.pause.assert_not_called()

    def test_single_changelog_message_recovery(
        self,
        recovery_manager_factory,
        recovery_partition_factory,
        topic_manager_factory,
    ):
        """
        RecoveryManager properly recognizes a recovery for when a single
        changelog message was successfully produced, and then state was deleted after.

        This covers an edge case around the default return value of -1001 for
        `recovery_partition` where store_partition has no offset stored yet
        AND only 1 changelog message was produced.
        """

        topic_name = str(uuid.uuid4())
        store_name = "default"
        lowwater, highwater = 0, 1

        # make topics
        topic_manager = topic_manager_factory()
        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=topic_manager.topic_config(),
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        # Mock the Consumer assignment with changelog topic-partition
        assignment = [
            TopicPartition(topic=changelog_topic.name, partition=0),
        ]
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Attempt to assign partition that should need recovery
        store_partition = MagicMock(spec_set=StorePartition)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        store_partition.get_changelog_offset.return_value = None  # will return -1001
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )

        # the partition should have been recognized as needing recovery,
        # thus it should have a recovery partition assigned
        assert recovery_manager.partitions
        assert recovery_manager.partitions[0][changelog_topic.name].needs_recovery_check

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

        topic_name = str(uuid.uuid4())
        store_name = "default"
        changelog_offset = 5
        lowwater, highwater = 0, 10

        # Register a source topic and a changelog topic with 2 partitions
        topic_manager = topic_manager_factory()

        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=TopicConfig(num_partitions=2, replication_factor=1),
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        # Mock the Consumer assignment with changelog topic-partition
        assignment = [
            TopicPartition(topic=changelog_topic.name, partition=0),
            TopicPartition(topic=changelog_topic.name, partition=1),
        ]
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign first partition that needs recovery
        store_partition = MagicMock(spec_set=StorePartition)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        store_partition.get_changelog_offset.return_value = changelog_offset
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )
        assert recovery_manager.partitions
        assert recovery_manager.partitions[0][changelog_topic.name].needs_recovery_check

        # Put a RecoveryManager into "recovering" state
        recovery_manager._running = True
        assert recovery_manager.recovering

        # Assign second partition that also needs recovery
        store_partition = MagicMock(spec_set=StorePartition)
        store_partition.get_changelog_offset.return_value = 5
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=1,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )
        assert recovery_manager.partitions
        assert recovery_manager.partitions[1][changelog_topic.name].needs_recovery_check

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

    def test_assign_partition_changelog_tp_is_missing(
        self,
        recovery_manager_factory,
        recovery_partition_factory,
        topic_manager_factory,
    ):
        """
        Test that `RecoveryManager.assign_partition()` fails if the changelog topic
        partition is not assigned to Consumer
        """
        topic_name = str(uuid.uuid4())
        store_name = "default"
        changelog_offset = 5
        lowwater, highwater = 0, 10

        # Register a source topic and a changelog topic with 2 partitions
        topic_manager = topic_manager_factory()
        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=TopicConfig(num_partitions=2, replication_factor=1),
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign first partition that needs recovery
        store_partition = MagicMock(spec_set=StorePartition)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        store_partition.get_changelog_offset.return_value = changelog_offset
        # Mock the Consumer assignment with only one changelog topic-partition
        assignment = [
            TopicPartition(topic=changelog_topic.name, partition=0),
        ]
        consumer.assignment.return_value = assignment
        # Assign another recovery partitio
        with pytest.raises(ChangelogTopicPartitionNotAssigned):
            recovery_manager.assign_partition(
                topic=topic_name,
                partition=1,
                committed_offsets={topic_name: -1001},
                store_partitions={store_name: store_partition},
            )

    def test_revoke_partition(self, recovery_manager_factory, topic_manager_factory):
        """
        Revoke a topic partition's respective recovery partitions.
        """
        topic_name = str(uuid.uuid4())
        store_name = "default"
        changelog_offset = 5
        lowwater, highwater = 0, 10

        # Register a source topic and a changelog topic with two partitions
        topic_manager = topic_manager_factory()
        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=TopicConfig(num_partitions=2, replication_factor=1),
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        # Mock the Consumer assignment with changelog topic-partition
        assignment = [
            TopicPartition(topic=changelog_topic.name, partition=0),
            TopicPartition(topic=changelog_topic.name, partition=1),
        ]
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign partitions that need recovery
        store_partition = MagicMock(spec_set=StorePartition)
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        store_partition.get_changelog_offset.return_value = changelog_offset
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=1,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )
        assert len(recovery_manager.partitions) == 2

        # Revoke one partition
        recovery_manager.revoke_partition(0)
        assert len(recovery_manager.partitions) == 1
        # Check that consumer unassigned the changelog topic partition as well
        assert consumer.pause.call_args.args[0] == [
            ConfluentPartition(topic=changelog_topic.name, partition=0)
        ]

        # Revoke second partition
        recovery_manager.revoke_partition(1)
        # Check that consumer unassigned the changelog topic partition as well
        assert consumer.pause.call_args.args[0] == [
            ConfluentPartition(topic=changelog_topic.name, partition=1)
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

    def test_do_recovery_no_partitions_assigned(self, recovery_manager_factory):
        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        recovery_manager = recovery_manager_factory(consumer=consumer)
        # Trigger a recovery
        recovery_manager.do_recovery()

        # Check that consumer.poll() is not called
        assert not consumer.poll.called

    def test_do_recovery_handles_invalid_offset_during_rebalance(
        self,
        recovery_manager_factory,
        topic_manager_factory,
    ):
        """
        Test that RecoveryManager handles OFFSET_INVALID gracefully when
        consumer.position() returns invalid offset during rebalancing.

        This reproduces GitHub issue #1067 where recovery gets stuck in infinite
        loop when partition stays assigned through rebalance but position becomes
        temporarily invalid.
        """
        topic_name = str(uuid.uuid4())
        store_name = "default"
        lowwater, highwater = 0, 10

        # Setup topics
        topic_manager = topic_manager_factory()
        data_topic = topic_manager.topic(topic_name)
        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name,
            store_name=store_name,
            config=data_topic.broker_config,
        )

        data_tp = TopicPartition(topic=data_topic.name, partition=0)
        changelog_tp = TopicPartition(topic=changelog_topic.name, partition=0)
        assignment = [data_tp, changelog_tp]

        # Create changelog message for recovery
        # Message at offset (highwater - 1) means after consuming it,
        # position will be at highwater, completing recovery
        changelog_message = ConfluentKafkaMessageStub(
            topic=changelog_topic.name,
            partition=0,
            offset=highwater - 1,
            key=b"key",
            value=b"value",
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
        )

        # Create mocked consumer
        consumer = MagicMock(spec_set=Consumer)
        consumer.assignment.return_value = assignment

        # Simulate rebalancing scenario:
        # 1. First poll returns None → OFFSET_INVALID detected and skipped
        # 2. Second poll returns message → processes it
        # 3. Third poll returns None → position check shows recovery complete
        consumer.poll.side_effect = [None, changelog_message, None]

        # Simulate consumer.position() behavior during rebalance:
        # 1. First call returns OFFSET_INVALID (mid-rebalance - gets skipped)
        # 2. Subsequent calls return highwater (recovery complete after message)
        position_call_count = 0

        def position_side_effect(partitions):
            nonlocal position_call_count
            position_call_count += 1
            if position_call_count == 1:
                # Mid-rebalance: return OFFSET_INVALID (will be skipped by fix)
                return [ConfluentPartition(changelog_topic.name, 0, OFFSET_INVALID)]
            else:
                # After OFFSET_INVALID resolved, position is at highwater
                return [ConfluentPartition(changelog_topic.name, 0, highwater)]

        consumer.position.side_effect = position_side_effect

        # Mock store partition
        store_partition = MagicMock(spec_set=StorePartition)
        # Stored offset is one before the message we'll recover
        store_partition.get_changelog_offset.return_value = highwater - 2

        # Setup recovery manager
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )

        # Mock time.monotonic to simulate time advancing between position calls
        # This ensures the position cache expires between calls (cache is 1 second)
        mock_time = 0.0

        def monotonic_side_effect():
            nonlocal mock_time
            mock_time += 2.0  # Advance time by 2 seconds each call
            return mock_time

        with patch(
            "quixstreams.state.recovery.time.monotonic",
            side_effect=monotonic_side_effect,
        ):
            # Trigger recovery - should complete successfully despite OFFSET_INVALID
            recovery_manager.do_recovery()

        # Verify recovery completed successfully
        assert (
            not recovery_manager.partitions
        ), "Recovery should complete and unassign all partitions"
        assert consumer.poll.call_count == 3, (
            "Should poll three times: "
            "1) None (OFFSET_INVALID detected and skipped), "
            "2) message consumed, "
            "3) None (position==highwater, completes recovery)"
        )
        assert position_call_count == 2, (
            "Should call position twice: "
            "first returns OFFSET_INVALID (skipped), "
            "second returns highwater (recovery complete)"
        )

        # Verify the changelog message was processed
        store_partition.recover_from_changelog_message.assert_called_once()

    def test_recovery_paused_partition_is_resumed_when_reassigned(
        self,
    ):
        """
        Regression test for a data partition staying paused after recovery.

        Recovery pauses the whole assignment before consuming changelogs. If a
        paused data partition is temporarily missing from the assignment when
        recovery completes, it must still be resumed when it is assigned again.
        """

        class TrackingConsumer:
            def __init__(self, assignments):
                self._assignments = assignments
                self._assignment_call_count = 0
                self.paused = set()
                self.pause_calls = []
                self.resume_calls = []

            def assignment(self):
                index = min(self._assignment_call_count, len(self._assignments) - 1)
                self._assignment_call_count += 1
                return self._assignments[index]

            def pause(self, partitions):
                self.pause_calls.append(partitions)
                for tp in partitions:
                    self.paused.add((tp.topic, tp.partition))

            def resume(self, partitions):
                self.resume_calls.append(partitions)
                for tp in partitions:
                    self.paused.discard((tp.topic, tp.partition))

            def get_watermark_offsets(self, *_args, **_kwargs):
                return 0, 10

            def seek(self, *_args, **_kwargs):
                return None

            def poll(self, *_args, **_kwargs):
                return None

            def position(self, partitions):
                return [
                    ConfluentPartition(
                        topic=tp.topic,
                        partition=tp.partition,
                        offset=10,
                    )
                    for tp in partitions
                ]

            def _broker_available(self):
                return None

        topic_name = str(uuid.uuid4())
        store_name = "default"

        def broker_topic(topic):
            topic.broker_config = topic.create_config
            return topic

        topic_manager = TopicManager(
            topic_admin=MagicMock(),
            consumer_group=str(uuid.uuid4()),
        )
        with patch.object(
            topic_manager, "_get_or_create_broker_topic", side_effect=broker_topic
        ):
            data_topic = topic_manager.topic(topic_name)
            changelog_topic = topic_manager.changelog_topic(
                stream_id=topic_name,
                store_name=store_name,
                config=TopicConfig(num_partitions=2, replication_factor=1),
            )

        data_tp_0 = TopicPartition(data_topic.name, 0)
        data_tp_1 = TopicPartition(data_topic.name, 1)
        changelog_tp_0 = TopicPartition(changelog_topic.name, 0)
        changelog_tp_1 = TopicPartition(changelog_topic.name, 1)

        consumer = TrackingConsumer(
            assignments=[
                [data_tp_0, data_tp_1, changelog_tp_0, changelog_tp_1],
                [data_tp_0, data_tp_1, changelog_tp_0, changelog_tp_1],
                [data_tp_0, changelog_tp_0],
                [data_tp_1, changelog_tp_1],
            ]
        )
        recovery_manager = RecoveryManager(
            consumer=consumer, topic_manager=topic_manager
        )

        recovering_store_partition = MagicMock(spec_set=StorePartition)
        recovering_store_partition.get_changelog_offset.return_value = 5
        recovering_store_partition.has_incomplete_ttl_migration.return_value = False
        caught_up_store_partition = MagicMock(spec_set=StorePartition)
        caught_up_store_partition.get_changelog_offset.return_value = 9
        # Caught-up partition has no incomplete TTL migration, so it must
        # NOT be forced into a recovery check.
        caught_up_store_partition.has_incomplete_ttl_migration.return_value = False

        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: recovering_store_partition},
        )
        assert (data_topic.name, 1) in consumer.paused

        recovery_manager.do_recovery()

        # Simulate the data partition being assigned again after recovery
        # completed while it was missing from the assignment snapshot.
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=1,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: caught_up_store_partition},
        )

        assert (data_topic.name, 1) not in consumer.paused

    @staticmethod
    def _topic_manager_with_data_topic(topic_name: str) -> TopicManager:
        def broker_topic(topic):
            topic.broker_config = topic.create_config
            return topic

        topic_manager = TopicManager(
            topic_admin=MagicMock(),
            consumer_group=str(uuid.uuid4()),
        )
        with patch.object(
            topic_manager, "_get_or_create_broker_topic", side_effect=broker_topic
        ):
            topic_manager.topic(topic_name)
        return topic_manager

    def test_resume_reassigned_data_partitions_resumes_orphaned_pause(self):
        """
        A data partition paused by a previous recovery generation and later
        reassigned in a rebalance that triggers no stateful assignment must be
        resumed; partitions never paused by recovery are left alone.
        """
        topic_name = str(uuid.uuid4())
        consumer = MagicMock()
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=self._topic_manager_with_data_topic(topic_name),
        )
        recovery_manager._recovery_paused_data_tps.add((topic_name, 0))

        recovery_manager.resume_reassigned_data_partitions(
            [
                ConfluentPartition(topic_name, 0),
                ConfluentPartition(topic_name, 1),
            ]
        )

        consumer.resume.assert_called_once()
        resumed = consumer.resume.call_args.args[0]
        assert [(tp.topic, tp.partition) for tp in resumed] == [(topic_name, 0)]
        assert not recovery_manager._recovery_paused_data_tps

    def test_resume_reassigned_data_partitions_noop_during_pending_recovery(self):
        """
        While a recovery is pending/active, recovery-paused partitions must
        stay paused: `do_recovery`/`assign_partition` own resuming them.
        """
        topic_name = str(uuid.uuid4())
        consumer = MagicMock()
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=self._topic_manager_with_data_topic(topic_name),
        )
        recovery_manager._recovery_paused_data_tps.add((topic_name, 0))
        # Simulate a pending recovery assignment
        recovery_manager._recovery_partitions[0] = {"changelog": MagicMock()}

        recovery_manager.resume_reassigned_data_partitions(
            [ConfluentPartition(topic_name, 0)]
        )

        consumer.resume.assert_not_called()
        assert (topic_name, 0) in recovery_manager._recovery_paused_data_tps

    def test_resume_reassigned_data_partitions_noop_without_paused_partitions(self):
        topic_name = str(uuid.uuid4())
        consumer = MagicMock()
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=self._topic_manager_with_data_topic(topic_name),
        )

        recovery_manager.resume_reassigned_data_partitions(
            [ConfluentPartition(topic_name, 0)]
        )

        consumer.resume.assert_not_called()

    def test_resume_reassigned_data_partitions_ignores_unassigned_partitions(self):
        """
        A partition that is still paused but was NOT part of this assignment
        must not be resumed (it belongs to another consumer now).
        """
        topic_name = str(uuid.uuid4())
        consumer = MagicMock()
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=self._topic_manager_with_data_topic(topic_name),
        )
        recovery_manager._recovery_paused_data_tps.add((topic_name, 1))

        recovery_manager.resume_reassigned_data_partitions(
            [ConfluentPartition(topic_name, 0)]
        )

        consumer.resume.assert_not_called()
        assert (topic_name, 1) in recovery_manager._recovery_paused_data_tps

    def test_resume_reassigned_data_partitions_logs_resumed_tps(self, caplog):
        """
        Finding 4: resuming recovery-paused data partitions emits an INFO log
        listing the resumed (topic, partition) pairs.
        """
        topic_name = str(uuid.uuid4())
        consumer = MagicMock()
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=self._topic_manager_with_data_topic(topic_name),
        )
        recovery_manager._recovery_paused_data_tps.add((topic_name, 0))

        with caplog.at_level(logging.INFO):
            recovery_manager.resume_reassigned_data_partitions(
                [ConfluentPartition(topic_name, 0)]
            )

        consumer.resume.assert_called_once()
        assert "Resuming data partitions paused for recovery" in caplog.text
        assert f"('{topic_name}', 0)" in caplog.text


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestRecoveryManagerRecover:
    def test_assign_partition(
        self,
        state_manager_factory,
        recovery_manager_factory,
        topic_manager_factory,
        producer,
    ):
        """
        Check that RecoveryManager.assign_partition() resumes proper changelog topic
        partition and pauses the consumer.
        """

        store_name = "default"
        # Stored changelog offset is between lowwater and highwater, so the
        # given store partition needs to be recovered.
        lowwater, highwater = 0, 20
        stored_changelog_offset = 15

        topic_name = str(uuid.uuid4())
        partition_num = 0

        consumer = MagicMock(spec_set=Consumer)
        topic_manager = topic_manager_factory()
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )
        state_manager = state_manager_factory(
            recovery_manager=recovery_manager, producer=producer
        )

        # Mock the topic watermarks
        consumer.get_watermark_offsets.side_effect = [(lowwater, highwater)]

        # Create Store and assign a StorePartition (which also sets up changelog topics)
        store_partitions = {}
        state_manager.register_store(
            stream_id=topic_name,
            store_name=store_name,
            changelog_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        store = state_manager.get_store(stream_id=topic_name, store_name=store_name)

        # Mock the Consumer assignment with changelog topic-partition
        changelog_topic = topic_manager.changelog_topics[topic_name][store_name]
        changelog_tp = TopicPartition(topic=changelog_topic.name, partition=0)
        consumer.assignment.return_value = [changelog_tp]

        partition = store.assign_partition(partition_num)
        store_partitions[store_name] = partition

        # Assign a RecoveryPartition
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=partition_num,
            store_partitions=store_partitions,
            committed_offsets={topic_name: -1001},
        )

        # Check that RecoveryPartition is assigned to RecoveryManager
        assert len(recovery_manager.partitions[partition_num]) == 1
        recovery_partition = list(recovery_manager.partitions[partition_num].values())[
            0
        ]
        changelog_topic_name = topic_manager.changelog_topics[topic_name][
            store_name
        ].name
        assert recovery_partition.changelog_name == changelog_topic_name
        assert recovery_partition.partition_num == partition_num
        with patch.object(
            partition,
            "get_changelog_offset",
            return_value=stored_changelog_offset,
        ):
            assert recovery_partition.offset == stored_changelog_offset

    def test_do_recovery(
        self, recovery_manager_factory, topic_manager_factory, store_partition
    ):
        """
        Test that RecoveryManager.do_recovery():
         - resumes the recovering changelog partition
         - applies the 1 missing changelog recovery message to the StorePartition
         - handles a None consumer poll to check for finished recovery (which it is)
         - revokes the RecoveryPartition
         - unassigns the changelog partition
         - unpauses source topic partitions
        """
        topic_name = str(uuid.uuid4())
        store_name = "default"
        lowwater, highwater = 0, 10

        # Register a source topic and a changelog topic with one partition
        topic_manager = topic_manager_factory()
        data_topic = topic_manager.topic(topic_name)
        changelog_topic = topic_manager.changelog_topic(
            stream_id=topic_name, store_name=store_name, config=data_topic.broker_config
        )

        data_tp = TopicPartition(topic=data_topic.name, partition=0)
        changelog_tp = TopicPartition(topic=changelog_topic.name, partition=0)
        assignment = [data_tp, changelog_tp]

        changelog_message = ConfluentKafkaMessageStub(
            topic=changelog_topic.name,
            partition=0,
            offset=highwater - 2,  # <highwater-1 ensures recovery check will be made
            key=b"key",
            value=b"value",
            headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
        )

        # Create a RecoveryManager
        consumer = MagicMock(spec_set=Consumer)
        # note how the poll returns None, which signifies no more messages to recover
        consumer.poll.side_effect = [changelog_message, None]
        consumer.assignment.return_value = assignment
        recovery_manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign a partition that needs recovery
        consumer.get_watermark_offsets.return_value = (lowwater, highwater)
        # this will get called after a "None" consumer poll result
        consumer.position.return_value = [
            ConfluentPartition(changelog_topic.name, 0, highwater)
        ]
        recovery_manager.assign_partition(
            topic=topic_name,
            partition=0,
            committed_offsets={topic_name: -1001},
            store_partitions={store_name: store_partition},
        )

        # Trigger a recovery
        recovery_manager.do_recovery()

        # Check that consumer first resumed the changelog topic partition
        consumer_resume_calls = consumer.resume.call_args_list

        assert len(consumer.poll.call_args_list) == 2
        assert consumer_resume_calls[0].args[0] == [
            ConfluentPartition(topic=changelog_topic.name, partition=0)
        ]
        # Check that consumer resumed all non-changelog partitions after recovery is done
        assert consumer_resume_calls[1].args[0] == [data_tp]

        # Check that RecoveryPartitions are unassigned
        assert not recovery_manager.partitions

    @pytest.mark.parametrize("isolation", ["read_committed", "read_uncommitted"])
    @pytest.mark.parametrize(
        "scenario",
        [
            "control",
            pytest.param("late_commit", marks=_RECOVERY_OVERSHOOT),
            pytest.param("late_abort", marks=_RECOVERY_OVERSHOOT),
            pytest.param("late_records", marks=_RECOVERY_OVERSHOOT),
        ],
    )
    def test_do_recovery_after_changelog_highwater(
        self,
        scenario,
        isolation,
        store_partition,
        topic_manager_factory,
        recovery_manager_factory,
        consumer_factory,
        producer_factory,
    ):
        """Resolve a transaction after assignment freezes the recovery target.

        Real broker offsets/markers, store writes, and recovery code are used. Only
        polling/status calls are wrapped to bound the test if recovery never exits.
        The late-records case reproduces the reported log numerator six above target.
        """
        topic_manager = topic_manager_factory()
        source = topic_manager.topic(
            str(uuid.uuid4()),
            create_config=topic_manager.topic_config(num_partitions=1),
        )
        changelog = topic_manager.changelog_topic(
            stream_id=source.name, store_name="default", config=source.broker_config
        )
        source_tp = TopicPartition(source.name, 0, 0)
        changelog_tp = TopicPartition(changelog.name, 0, 0)
        consumer = consumer_factory(
            auto_commit_enable=False,
            auto_offset_reset="earliest",
            extra_config={"isolation.level": isolation},
        )
        producer = producer_factory(transactional=True)
        manager = recovery_manager_factory(
            consumer=consumer, topic_manager=topic_manager
        )
        transaction_open = False

        def wait_for_highwater(expected):
            # Commit/abort can return before the marker is visible to a watermark
            # query. Synchronize setup with the broker, not an arbitrary sleep.
            timeout = Timeout(15)
            while timeout:
                _, highwater = consumer.get_watermark_offsets(changelog_tp, timeout=5)
                if highwater == expected:
                    return
                time.sleep(0.1)

        def produce_changelog(start, end):
            for value in range(start, end):
                producer.produce(
                    changelog.name,
                    partition=0,
                    key=b"key" + SEPARATOR + b'"count"',
                    value=str(value).encode(),
                    headers=[(CHANGELOG_CF_MESSAGE_HEADER, b"default")],
                )
            assert producer.flush(10) == 0

        try:
            producer.begin_transaction()
            transaction_open = True
            produce_changelog(0, 10)
            producer.produce(source.name, partition=0, value=b"pending-source-message")
            producer.commit_transaction(15)
            transaction_open = False
            # Ten records plus their commit marker. A committed prefix ensures
            # read_committed recovery is needed even with an open transaction.
            wait_for_highwater(11)

            if scenario in ("late_commit", "late_abort"):
                producer.begin_transaction()
                transaction_open = True
                produce_changelog(10, 11)

            consumer.assign([source_tp, changelog_tp])
            manager.assign_partition(
                topic=source.name,
                partition=0,
                store_partitions={"default": store_partition},
                committed_offsets={source.name: 0},
            )
            rp = manager.partitions[0][changelog.name]
            expected_target = (
                12
                if isolation == "read_uncommitted"
                and scenario in ("late_commit", "late_abort")
                else 11
            )
            assert rp.changelog_highwater == expected_target

            # Change the log AFTER the real manager has captured its highwater.
            if scenario == "late_records":
                producer.begin_transaction()
                transaction_open = True
                produce_changelog(10, 16)
            if scenario == "late_abort":
                producer.abort_transaction(15)
                transaction_open = False
            elif transaction_open:
                producer.commit_transaction(15)
                transaction_open = False

            final_highwater = (
                11
                if scenario == "control"
                else 18
                if scenario == "late_records"
                else 13
            )
            wait_for_highwater(final_highwater)
            caught_up_checks = 0
            timeout = Timeout(30)
            original_poll = consumer.poll
            original_update = manager._update_recovery_status

            def bounded_poll(poll_timeout):
                assert timeout  # Infrastructure failures remain errors, not xfails.
                return original_poll(poll_timeout)

            def bounded_status_update():
                nonlocal caught_up_checks
                original_update()
                if (
                    manager.has_assignments
                    and rp.recovery_consume_position == final_highwater
                ):
                    caught_up_checks += 1
                    if caught_up_checks == 3:
                        manager.stop_recovery()

            with (
                patch.object(consumer, "poll", side_effect=bounded_poll),
                patch.object(
                    manager,
                    "_update_recovery_status",
                    side_effect=bounded_status_update,
                ),
            ):
                manager.do_recovery()

            assert rp.recovery_consume_position == final_highwater
            expected_value = (
                9
                if scenario == "control"
                or (scenario == "late_abort" and isolation == "read_committed")
                else 15
                if scenario == "late_records"
                else 10
            )
            with store_partition.begin() as tx:
                assert tx.get("count", prefix=b"key") == expected_value

            if manager.has_assignments:
                assert caught_up_checks == 3
                raise RecoveryStalledAfterHighwater(
                    f"Recovery still assigned after {caught_up_checks} caught-up checks: "
                    f"position={rp.recovery_consume_position}, target={rp.changelog_highwater}"
                )

            assert not manager.recovering
            # Successful recovery must actually release the source partition.
            timeout = Timeout(10)
            while timeout:
                if (message := consumer.poll(1)) is not None:
                    assert message.error() is None
                    assert message.topic() == source.name
                    assert message.offset() == 0
                    assert message.value() == b"pending-source-message"
                    break
        finally:
            try:
                if transaction_open:
                    producer.abort_transaction(15)
            finally:
                consumer.close()
