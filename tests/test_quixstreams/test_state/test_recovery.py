from unittest.mock import patch, create_autospec

import logging
import uuid

from quixstreams.state.recovery import (
    ChangelogWriter,
    RecoveryPartition,
    ConfluentPartition,
    OffsetUpdate,
)
from quixstreams.state.types import StorePartition
from ..utils import ConfluentKafkaMessageStub


class TestRecoveryPartition:
    def test_set_watermarks(self, recovery_partition_store_mock):
        recovery_partition = recovery_partition_store_mock
        recovery_partition.set_watermarks(50, 100)
        assert recovery_partition._changelog_lowwater == 50
        assert recovery_partition._changelog_highwater == 100

    def test_needs_recovery(self, recovery_partition_store_mock):
        recovery_partition = recovery_partition_store_mock
        assert recovery_partition.needs_recovery

    def test_needs_recovery_caught_up(self, recovery_partition_store_mock):
        recovery_partition = recovery_partition_store_mock
        recovery_partition.store_partition.get_changelog_offset.return_value = 20
        assert not recovery_partition_store_mock.needs_recovery

    def test_needs_recovery_no_valid_offsets(self, recovery_partition_store_mock):
        recovery_partition = recovery_partition_store_mock
        recovery_partition.set_watermarks(100, 100)
        assert not recovery_partition.needs_recovery
        assert recovery_partition.needs_offset_update

    def test_recover(self, recovery_partition_store_mock):
        recovery_partition = recovery_partition_store_mock
        msg = ConfluentKafkaMessageStub()
        recovery_partition.recover(msg)
        recovery_partition.store_partition.recover.assert_called_with(
            changelog_message=msg
        )

    def test_update_offset(self, recovery_partition_store_mock):
        recovery_partition = recovery_partition_store_mock
        with patch(
            "quixstreams.state.recovery.OffsetUpdate", spec=OffsetUpdate
        ) as offset_update:
            recovery_partition.update_offset()
        offset_update.assert_called_with(recovery_partition.offset)
        assert isinstance(
            recovery_partition.store_partition.set_changelog_offset.call_args_list[
                0
            ].kwargs["changelog_message"],
            OffsetUpdate,
        )

    def test_update_offset_warn(self, recovery_partition_store_mock, caplog):
        """
        A warning is thrown if the stored changelog offset is higher than the highwater
        """
        recovery_partition = recovery_partition_store_mock
        recovery_partition.store_partition.get_changelog_offset.return_value = (
            recovery_partition._changelog_highwater + 1
        )
        with caplog.at_level(level=logging.WARNING):
            recovery_partition.update_offset()
        assert caplog.text != ""


class TestChangelogWriter:
    def test_produce(
        self, topic_manager_factory, row_producer_factory, consumer_factory
    ):
        p_num = 2
        cf_header = "my_cf_header"
        cf = "my_cf"
        expected = {
            "key": b"my_key",
            "value": b"10",
            "headers": [(cf_header, cf.encode())],
            "partition": p_num,
        }
        topic_manager = topic_manager_factory()
        changelog = topic_manager.topic(
            name=str(uuid.uuid4()),
            key_serializer="bytes",
            value_serializer="bytes",
            config=topic_manager.topic_config(num_partitions=3),
        )
        topic_manager.create_topics([changelog])

        writer = ChangelogWriter(
            changelog=changelog, partition_num=p_num, producer=row_producer_factory()
        )
        writer.produce(
            **{k: v for k, v in expected.items() if k in ["key", "value"]},
            headers={cf_header: cf},
        )
        writer._producer.flush(5)

        consumer = consumer_factory(auto_offset_reset="earliest")
        consumer.subscribe([changelog.name])
        message = consumer.poll(10)

        for k in expected:
            assert getattr(message, k)() == expected[k]


class TestChangelogManager:
    def test_add_changelog(self, changelog_manager_factory):
        changelog_manager = changelog_manager_factory()
        topic_manager = changelog_manager._topic_manager
        store_name = "my_store"
        kwargs = dict(
            topic_name="my_topic_name",
            consumer_group="my_group",
        )
        with patch.object(topic_manager, "changelog_topic") as make_changelog:
            changelog_manager.register_changelog(**kwargs, store_name=store_name)
        make_changelog.assert_called_with(**kwargs, store_name=store_name)

    def test_get_writer(self, row_producer_factory, changelog_manager_factory):
        producer = row_producer_factory()
        changelog_manager = changelog_manager_factory(producer=producer)
        topic_manager = changelog_manager._topic_manager

        topic_name = "my_topic"
        store_name = "my_store"
        p_num = 1
        topic_manager.topic(topic_name)  # changelogs depend on topic objects existing
        changelog = topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name, consumer_group="group"
        )

        writer = changelog_manager.get_writer(
            topic_name=topic_name, store_name=store_name, partition_num=p_num
        )
        assert writer._changelog == changelog
        assert writer._partition_num == p_num
        assert writer._producer == producer

    def test_assign_partition(self, changelog_manager_mock_recovery):
        topic_name = "topic_a"
        store_names = ["default", "rolling_10s"]
        partition_num = 1
        changelog_manager = changelog_manager_mock_recovery
        topic_manager = changelog_manager._topic_manager
        recovery_manager = changelog_manager._recovery_manager

        topic_manager.topic(topic_name)
        store_partitions = {}
        changelog_partitions = {}
        for store_name in store_names:
            changelog = topic_manager.changelog_topic(
                topic_name=topic_name, store_name=store_name, consumer_group="group"
            )
            store_partitions[store_name] = create_autospec(StorePartition)()
            changelog_partitions[changelog.name] = store_partitions[store_name]

        changelog_manager.assign_partition(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions=store_partitions,
        )

        recovery_manager.assign_partitions.assert_called_with(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions=changelog_partitions,
        )

    def test_revoke_partition(self, changelog_manager_mock_recovery):
        topic_name = "topic_a"
        store_names = ["default", "rolling-10s"]
        partition_num = 1
        changelog_manager = changelog_manager_mock_recovery
        topic_manager = changelog_manager._topic_manager
        recovery_manager = changelog_manager._recovery_manager

        topic_manager.topic(topic_name)
        for store_name in store_names:
            topic_manager.changelog_topic(
                topic_name=topic_name, store_name=store_name, consumer_group="group"
            )

        changelog_manager.revoke_partition(
            partition_num=partition_num,
        )
        recovery_manager.revoke_partitions.assert_called_with(
            partition_num=partition_num
        )

    def test_do_recovery(self, changelog_manager_mock_recovery):
        changelog_manager = changelog_manager_mock_recovery
        recovery_manager = changelog_manager._recovery_manager

        changelog_manager.do_recovery()
        recovery_manager.do_recovery.assert_called()


class TestRecoveryManager:
    def test_assign_partitions(self, recovery_manager_mock_consumer):
        """
        Assign a topic partition and queue up its respective changelog partitions for
        assignment.
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_num = 1
        store_partition = create_autospec(StorePartition)()
        recovery_manager.assign_partitions(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions={changelog_name: store_partition},
        )

        assert len(recovery_manager._pending_assigns) == 1
        recovery_partition = recovery_manager._pending_assigns[0]
        expected_confluent_partition = ConfluentPartition(
            recovery_partition.topic_name, recovery_partition.partition_num
        )
        assert isinstance(recovery_partition, RecoveryPartition)
        assert recovery_partition.topic_name == topic_name
        assert recovery_partition.changelog_name == changelog_name
        assert recovery_partition.partition_num == partition_num
        assert recovery_partition.store_partition == store_partition

        pause_call = consumer.pause.call_args_list[0].args
        assert len(pause_call) == 1
        assert isinstance(pause_call[0], list)
        assert len(pause_call[0]) == 1
        assert isinstance(pause_call[0][0], ConfluentPartition)
        assert expected_confluent_partition.topic == pause_call[0][0].topic
        assert expected_confluent_partition.partition == pause_call[0][0].partition

    def test__revoke_recovery_partitions(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        changelog_names = [
            f"changelog__{topic_name}__{store_name}"
            for store_name in ["store_a", "store_b"]
        ]

        partition_num = 1
        recovery_manager._recovery_partitions = {
            partition_num: {
                changelog_name: RecoveryPartition(
                    topic_name=topic_name,
                    changelog_name=changelog_name,
                    partition_num=partition_num,
                    store_partition=create_autospec(StorePartition)(),
                )
                for changelog_name in changelog_names
            }
        }

        recovery_manager.revoke_partitions(partition_num=partition_num)

        unassign_call = consumer.incremental_unassign.call_args_list[0].args
        assert len(unassign_call) == 1
        assert isinstance(unassign_call[0], list)
        assert len(unassign_call[0]) == 2
        for idx, confluent_partition in enumerate(unassign_call[0]):
            assert isinstance(confluent_partition, ConfluentPartition)
            assert changelog_names[idx] == confluent_partition.topic
            assert partition_num == confluent_partition.partition
        assert not recovery_manager._recovery_partitions

    def test_revoke_partitions(self, recovery_manager_mock_consumer):
        """
        Revoke a topic partition's respective recovery partitions.
        """
        recovery_manager = recovery_manager_mock_consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_num = 1
        recovery_partition = (
            RecoveryPartition(
                topic_name=topic_name,
                changelog_name=changelog_name,
                partition_num=partition_num,
                store_partition=create_autospec(StorePartition)(),
            ),
        )
        recovery_manager._recovery_partitions = {
            partition_num: {changelog_name: recovery_partition}
        }

        with patch.object(recovery_manager, "_revoke_recovery_partitions") as revoke:
            recovery_manager.revoke_partitions(partition_num=partition_num)

        revoke.assert_called_with([recovery_partition], partition_num)

    def test_revoke_partition_not_assigned(self, recovery_manager_mock_consumer):
        """
        Skip revoking any recovery partitions for a given partition since none are
        currently assigned (due to not needing recovery).
        """
        recovery_manager = recovery_manager_mock_consumer
        with patch.object(recovery_manager, "_revoke_recovery_partitions") as revoke:
            recovery_manager.revoke_partitions(partition_num=1)

        revoke.assert_not_called()

    def test__handle_pending_assigns(self, recovery_manager_mock_consumer):
        """
        Two changelog partitions (partition numbers 3 and 7) are pending assignment; p3
        has no offsets to recover, and p7 does, so only p7 should end up assigned.
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_nums = [3, 7]
        recovery_manager._pending_assigns = [
            RecoveryPartition(
                topic_name=topic_name,
                changelog_name=changelog_name,
                partition_num=partition_num,
                store_partition=create_autospec(StorePartition)(),
            )
            for partition_num in partition_nums
        ]
        side_effects = []
        for idx, p in enumerate(recovery_manager._pending_assigns):
            p.store_partition.get_changelog_offset.return_value = 10 * idx
            side_effects.append((0, 20 * idx))
        side_effects.reverse()
        consumer.get_watermark_offsets.side_effect = side_effects
        not_recover = recovery_manager._pending_assigns[0]
        should_recover = recovery_manager._pending_assigns[1]

        recovery_manager._handle_pending_assigns()

        assign_call = consumer.incremental_assign.call_args_list[0].args
        assert len(assign_call) == 1
        assert isinstance(assign_call[0], list)
        assert len(assign_call[0]) == 1
        assert isinstance(assign_call[0][0], ConfluentPartition)
        assert should_recover.changelog_name == assign_call[0][0].topic
        assert should_recover.partition_num == assign_call[0][0].partition
        assert should_recover.offset == assign_call[0][0].offset

        assert (
            recovery_manager._recovery_partitions[should_recover.partition_num][
                should_recover.changelog_name
            ]
            == should_recover
        )
        assert not_recover.partition_num not in recovery_manager._recovery_partitions
        assert not recovery_manager._pending_assigns

    def test__handle_pending_assigns_no_assigns(self, recovery_manager_mock_consumer):
        """
        Handle pending assigns of changelog partitions where none of them actually
        needed assigning since they were up-to-date.
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_num = 3
        recovery_manager._pending_assigns = [
            RecoveryPartition(
                topic_name=topic_name,
                changelog_name=changelog_name,
                partition_num=partition_num,
                store_partition=create_autospec(StorePartition)(),
            )
        ]
        consumer.get_watermark_offsets.side_effect = [(0, 10)]
        not_recover = recovery_manager._pending_assigns[0]
        not_recover.store_partition.get_changelog_offset.return_value = 10

        recovery_manager._handle_pending_assigns()

        consumer.incremental_assign.assert_not_called()
        assert not_recover.partition_num not in recovery_manager._recovery_partitions
        assert not recovery_manager._pending_assigns

    def test__handle_pending_assigns_update_offset(
        self, recovery_manager_mock_consumer
    ):
        """
        Handle pending assigns of changelog partitions where the partition does not
        actually need recovery, but instead a simple offset update (due to some
        processing error, or there being no offset to read).
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_num = 3
        recovery_manager._pending_assigns = [
            RecoveryPartition(
                topic_name=topic_name,
                changelog_name=changelog_name,
                partition_num=partition_num,
                store_partition=create_autospec(StorePartition)(),
            )
        ]
        consumer.get_watermark_offsets.side_effect = [(0, 10)]
        not_recover = recovery_manager._pending_assigns[0]
        not_recover.store_partition.get_changelog_offset.return_value = 20

        with patch.object(
            recovery_manager._pending_assigns[0], "update_offset"
        ) as update_offset:
            recovery_manager._handle_pending_assigns()

        consumer.incremental_assign.assert_not_called()
        update_offset.assert_called()
        assert not_recover.partition_num not in recovery_manager._recovery_partitions
        assert not recovery_manager._pending_assigns

    def test__update_partition_offsets(self, recovery_manager_mock_consumer):
        """
        Partition offset updates are handled correctly.
        """
        recovery_manager = recovery_manager_mock_consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_nums = [3, 7]
        expected_pending_revokes = []
        for partition_num in partition_nums:
            rp = RecoveryPartition(
                topic_name=topic_name,
                changelog_name=changelog_name,
                partition_num=partition_num,
                store_partition=create_autospec(StorePartition)(),
            )
            rp.set_watermarks(0, 20)
            rp.store_partition.get_changelog_offset.return_value = 18
            recovery_manager._recovery_partitions.setdefault(partition_num, {})[
                changelog_name
            ] = rp
            expected_pending_revokes += [rp]

        with patch.object(recovery_manager, "_revoke_recovery_partitions") as revoke:
            recovery_manager._update_partition_offsets()

        revoke.assert_called_with(expected_pending_revokes)
        for partition in expected_pending_revokes:
            partition.store_partition.set_changelog_offset.assert_called()

    def test__finalize_recovery(self, recovery_manager_mock_consumer):
        """
        Finalize recovery.

        Also tests when we have a remaining _partition with some offset
        issues, updating it to current and revoking it before finishing recovery.
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        assignment_result = "assignments_list"
        consumer.assignment.return_value = assignment_result
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_num = 1
        rp = RecoveryPartition(
            topic_name=topic_name,
            changelog_name=changelog_name,
            partition_num=partition_num,
            store_partition=create_autospec(StorePartition)(),
        )
        rp.set_watermarks(0, 20)
        rp.store_partition.get_changelog_offset.return_value = 18
        recovery_manager._recovery_partitions.setdefault(partition_num, {})[
            changelog_name
        ] = rp

        recovery_manager._finalize_recovery()

        consumer.resume.assert_called_with(assignment_result)
        consumer.incremental_unassign.assert_called()
        assert recovery_manager._polls_remaining == recovery_manager._poll_attempts
        assert not recovery_manager._recovery_partitions

    def test__recover(self, recovery_manager_mock_consumer):
        """
        Successfully recover from a changelog message, which is also the last one
        for the partition, so revoke it afterward.
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        highwater = 20
        partition_num = 1
        msg = ConfluentKafkaMessageStub(
            topic=changelog_name, partition=partition_num, offset=highwater - 1
        )
        consumer.poll.return_value = msg
        rp = RecoveryPartition(
            topic_name=topic_name,
            changelog_name=changelog_name,
            partition_num=partition_num,
            store_partition=create_autospec(StorePartition)(),
        )
        rp.set_watermarks(0, highwater)
        rp.store_partition.get_changelog_offset.return_value = highwater
        recovery_manager._recovery_partitions.setdefault(partition_num, {})[
            changelog_name
        ] = rp

        recovery_manager._recovery_loop()

        rp.store_partition.recover.assert_called_with(changelog_message=msg)
        assert not recovery_manager._recovery_partitions
        consumer.incremental_unassign.assert_called()

    def test__recovery_loop_no_partitions(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer

        recovery_manager._recovery_loop()
        consumer.poll.assert_not_called()

    def test__recovery_loop_empty_polls(self, recovery_manager_mock_consumer):
        """
        Handle empty poll attempts, which ends recovery.
        """
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        consumer.poll.return_value = None
        topic_name = "topic_name"
        changelog_name = f"changelog__{topic_name}__default"
        partition_num = 1

        # won't poll without a partition assigned
        rp = RecoveryPartition(
            topic_name=topic_name,
            changelog_name=changelog_name,
            partition_num=partition_num,
            store_partition=create_autospec(StorePartition)(),
        )
        recovery_manager._recovery_partitions.setdefault(partition_num, {})[
            changelog_name
        ] = rp

        recovery_manager._recovery_loop()

        assert recovery_manager._polls_remaining == 0
        assert consumer.poll.call_count == recovery_manager._poll_attempts
        rp.store_partition.recover.assert_not_called()
