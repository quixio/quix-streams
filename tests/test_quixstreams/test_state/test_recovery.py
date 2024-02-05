from unittest.mock import patch, create_autospec, call

import logging
import uuid

from quixstreams.state.recovery import (
    ChangelogWriter,
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
        offset_update.assert_called_with(recovery_partition._changelog_highwater - 1)
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
        recovery_partition_calls = []
        for store_name in store_names:
            changelog = topic_manager.changelog_topic(
                topic_name=topic_name, store_name=store_name, consumer_group="group"
            )
            store_partitions[store_name] = create_autospec(StorePartition)()
            recovery_partition_calls.append(
                call(
                    changelog_name=changelog.name,
                    partition_num=partition_num,
                    store_partition=store_partitions[store_name],
                )
            )
        with patch("quixstreams.state.recovery.RecoveryPartition") as rp:
            rp.return_value = "rp"
            changelog_manager.assign_partition(
                topic_name=topic_name,
                partition_num=partition_num,
                store_partitions=store_partitions,
            )

        rp.assert_has_calls(recovery_partition_calls)
        assert rp.call_count == len(recovery_partition_calls)
        recovery_manager.assign_partitions.assert_called_with(
            topic_name=topic_name,
            partition_num=partition_num,
            recovery_partitions=["rp", "rp"],
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
    def test_do_recovery_skip_recovery(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        recovery_manager.do_recovery()

        consumer.resume.assert_not_called()

    def test_do_recovery(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        recovery_manager = recovery_manager_mock_consumer
        topic_name = "topic_name"
        partition_num = 1
        store_names = ["default", "window"]
        changelog_names = [f"changelog__{topic_name}__{store}" for store in store_names]
        watermarks = [(0, 10), (0, 20)]
        changelog_offsets = [0, 0]

        consumer = recovery_manager._consumer
        consumer.get_watermark_offsets.side_effect = watermarks
        consumer.assignment.return_value = ["assignments"]

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
            )
            for i in range(len(store_names))
        ]

        recovery_manager.assign_partitions(
            topic_name=topic_name,
            partition_num=partition_num,
            recovery_partitions=recovery_partitions,
        )
        with patch.object(recovery_manager, "_recovery_loop") as recovery_loop:
            recovery_manager.do_recovery()

        changelog_resume_args = consumer.resume.call_args_list[0].args[0]
        print(changelog_resume_args)
        assert len(changelog_resume_args) == 2
        for idx, tp in enumerate(changelog_resume_args):
            assert recovery_partitions[idx].changelog_name == tp.topic
            assert recovery_partitions[idx].partition_num == tp.partition
        recovery_loop.assert_called()
        assert consumer.resume.call_args_list[1].args[0] == ["assignments"]
        assert consumer.resume.call_count == 2

    def test_do_recovery_skip(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer

        with patch.object(recovery_manager, "_recovery_loop") as recovery_loop:
            recovery_manager.do_recovery()

        consumer.resume.assert_not_called()
        recovery_loop.assert_not_called()

    def test_assign_partitions(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        From two RecoveryPartitions, assign the one ("window") that needs recovery.

        No recovery underway yet, so should pause all partitions.
        """
        recovery_manager = recovery_manager_mock_consumer
        topic_name = "topic_name"
        partition_num = 1
        store_names = ["default", "window"]
        changelog_names = [f"changelog__{topic_name}__{store}" for store in store_names]
        watermarks = [(0, 10), (0, 20)]
        changelog_offsets = [10, 15]

        consumer = recovery_manager._consumer
        consumer.get_watermark_offsets.side_effect = watermarks
        consumer.assignment.return_value = "assignments"

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
            )
            for i in range(len(store_names))
        ]
        skip_recover_partition = recovery_partitions[0]
        should_recover_partition = recovery_partitions[1]

        recovery_manager.assign_partitions(
            topic_name=topic_name,
            partition_num=partition_num,
            recovery_partitions=recovery_partitions,
        )

        # should pause ALL partitions
        consumer.pause.assert_called_with("assignments")

        # should only assign the partition that needs recovery
        assign_call = consumer.incremental_assign.call_args_list[0].args
        assert len(assign_call) == 1
        assert isinstance(assign_call[0], list)
        assert len(assign_call[0]) == 1
        assert isinstance(assign_call[0][0], ConfluentPartition)
        assert should_recover_partition.changelog_name == assign_call[0][0].topic
        assert should_recover_partition.partition_num == assign_call[0][0].partition
        assert should_recover_partition.offset == assign_call[0][0].offset
        assert (
            recovery_manager._recovery_partitions[partition_num][
                should_recover_partition.changelog_name
            ]
            == should_recover_partition
        )
        assert (
            skip_recover_partition.changelog_name
            not in recovery_manager._recovery_partitions[partition_num]
        )

    def test_assign_partitions_fix_offset_only(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        From two RecoveryPartitions, fix the one ("window") that has a bad offset.

        No recovery was previously going, and an offset fix will not trigger one.
        """
        recovery_manager = recovery_manager_mock_consumer
        topic_name = "topic_name"
        partition_num = 1
        store_names = ["default", "window"]
        changelog_names = [f"changelog__{topic_name}__{store}" for store in store_names]
        watermarks = [(0, 10), (0, 20)]
        changelog_offsets = [10, 22]

        consumer = recovery_manager._consumer
        consumer.get_watermark_offsets.side_effect = watermarks
        consumer.assignment.return_value = "assignments"

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
            )
            for i in range(len(store_names))
        ]

        with patch.object(recovery_partitions[1], "update_offset") as update_offset:
            recovery_manager.assign_partitions(
                topic_name=topic_name,
                partition_num=partition_num,
                recovery_partitions=recovery_partitions,
            )

        # no pause or assignments should be called
        consumer.pause.assert_not_called()
        consumer.incremental_assign.assert_not_called()
        update_offset.assert_called()

    def test_assign_partitions_fix_offset_during_recovery(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        From two RecoveryPartitions, fix the one ("window") that has a bad offset.

        Recovery was previously going, so must pause the source topic.
        """
        recovery_manager = recovery_manager_mock_consumer
        recovery_manager._recovery_initialized = True
        topic_name = "topic_name"
        partition_num = 1
        store_names = ["default", "window"]
        changelog_names = [f"changelog__{topic_name}__{store}" for store in store_names]
        watermarks = [(0, 10), (0, 20)]
        changelog_offsets = [10, 22]

        consumer = recovery_manager._consumer
        consumer.get_watermark_offsets.side_effect = watermarks

        # already in the middle of recovering
        recovery_manager._recovery_partitions.setdefault(2, {})[
            changelog_offsets[0]
        ] = recovery_partition_factory(
            changelog_name=changelog_names[0],
            partition_num=2,
        )
        assert recovery_manager.recovering

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
            )
            for i in range(len(store_names))
        ]

        with patch.object(recovery_partitions[1], "update_offset") as update_offset:
            recovery_manager.assign_partitions(
                topic_name=topic_name,
                partition_num=partition_num,
                recovery_partitions=recovery_partitions,
            )

        pause_call = consumer.pause.call_args_list[0].args
        assert len(pause_call) == 1
        assert isinstance(pause_call[0], list)
        assert len(pause_call[0]) == 1
        assert isinstance(pause_call[0][0], ConfluentPartition)
        assert topic_name == pause_call[0][0].topic
        assert partition_num == pause_call[0][0].partition

        consumer.incremental_assign.assert_not_called()
        update_offset.assert_called()

    def test_assign_partitions_during_recovery(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        From two RecoveryPartitions, assign the one ("window") that needs recovery.

        RecoveryManager is currently recovering, so should only pause source topic.
        """
        recovery_manager = recovery_manager_mock_consumer
        recovery_manager._recovery_initialized = True
        topic_name = "topic_name"
        partition_num = 1
        store_names = ["default", "window"]
        changelog_names = [f"changelog__{topic_name}__{store}" for store in store_names]
        watermarks = [(0, 10), (0, 20)]
        changelog_offsets = [10, 15]

        consumer = recovery_manager._consumer
        consumer.get_watermark_offsets.side_effect = watermarks

        # already in the middle of recovering
        recovery_manager._recovery_partitions.setdefault(2, {})[
            changelog_offsets[0]
        ] = recovery_partition_factory(
            changelog_name=changelog_names[0],
            partition_num=2,
        )
        assert recovery_manager.recovering

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
            )
            for i in range(len(store_names))
        ]
        skip_recover_partition = recovery_partitions[0]
        should_recover_partition = recovery_partitions[1]
        consumer.get_watermark_offsets.side_effect = watermarks

        recovery_manager.assign_partitions(
            topic_name=topic_name,
            partition_num=partition_num,
            recovery_partitions=recovery_partitions,
        )

        # should only pause the source topic partition since currently recovering
        pause_call = consumer.pause.call_args_list[0].args
        assert len(pause_call) == 1
        assert isinstance(pause_call[0], list)
        assert len(pause_call[0]) == 1
        assert isinstance(pause_call[0][0], ConfluentPartition)
        assert topic_name == pause_call[0][0].topic
        assert partition_num == pause_call[0][0].partition

        # should only assign the partition that needs recovery
        assign_call = consumer.incremental_assign.call_args_list[0].args
        assert len(assign_call) == 1
        assert isinstance(assign_call[0], list)
        assert len(assign_call[0]) == 1
        assert isinstance(assign_call[0][0], ConfluentPartition)
        assert should_recover_partition.changelog_name == assign_call[0][0].topic
        assert should_recover_partition.partition_num == assign_call[0][0].partition
        assert should_recover_partition.offset == assign_call[0][0].offset
        assert (
            recovery_manager._recovery_partitions[partition_num][
                should_recover_partition.changelog_name
            ]
            == should_recover_partition
        )
        assert (
            skip_recover_partition.changelog_name
            not in recovery_manager._recovery_partitions[partition_num]
        )

    def test__revoke_recovery_partitions(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer
        topic_name = "topic_name"
        partition_num = 1
        changelog_names = [
            f"changelog__{topic_name}__{store_name}"
            for store_name in ["default", "window"]
        ]

        recovery_manager._recovery_partitions = {
            partition_num: {
                changelog_name: recovery_partition_factory(
                    changelog_name=changelog_name,
                    partition_num=partition_num,
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

    def test_revoke_partitions(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        Revoke a topic partition's respective recovery partitions.
        """
        recovery_manager = recovery_manager_mock_consumer
        topic_name = "topic_name"
        partition_num = 1
        changelog_name = f"changelog__{topic_name}__default"
        recovery_partition = (
            recovery_partition_factory(
                changelog_name=changelog_name,
                partition_num=partition_num,
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

    def test__recovery_loop(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
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
        rp = recovery_partition_factory(
            changelog_name=changelog_name,
            partition_num=partition_num,
            mocked_changelog_offset=highwater,  # referenced AFTER recovering from msg
            lowwater=0,
            highwater=highwater,
        )
        recovery_manager._recovery_partitions.setdefault(partition_num, {})[
            changelog_name
        ] = rp

        recovery_manager._recovery_loop()

        rp.store_partition.recover.assert_called_with(changelog_message=msg)
        consumer.incremental_unassign.assert_called()

    def test__recovery_loop_no_partitions(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer

        recovery_manager._recovery_loop()
        consumer.poll.assert_not_called()
