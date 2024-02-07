from unittest.mock import patch, MagicMock
import logging
import uuid

from quixstreams.state.recovery import (
    ChangelogProducer,
    ConfluentPartition,
    OffsetUpdate,
)
from quixstreams.state.recovery import ChangelogProducerFactory
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
        recovery_partition.recover_from_changelog_message(msg)
        recovery_partition.store_partition.recover_from_changelog_message.assert_called_with(
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


class TestChangelogProducer:
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

        writer = ChangelogProducer(
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


class TestChangelogProducerFactory:
    def test_get_partition_producer(self, row_producer_factory):
        changelog_topic = MagicMock()
        changelog_topic.name = "changelog__topic"
        producer = row_producer_factory()

        p_num = 1

        changelog_producer = ChangelogProducerFactory(
            changelog=changelog_topic, producer=producer
        ).get_partition_producer(partition_num=p_num)
        assert changelog_producer._changelog == changelog_topic
        assert changelog_producer._partition_num == p_num
        assert changelog_producer._producer == producer


class TestRecoveryManager:
    def test_register_changelog(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        topic_manager = recovery_manager._topic_manager
        store_name = "my_store"
        kwargs = dict(
            topic_name="my_topic_name",
            consumer_group="my_group",
        )
        with patch.object(topic_manager, "changelog_topic") as make_changelog:
            recovery_manager.register_changelog(**kwargs, store_name=store_name)
        make_changelog.assert_called_with(**kwargs, store_name=store_name)

    def test_assign_partition(self, state_manager_changelogs):
        """
        From two `Store`s `StorePartition`s (partition 1), assign the partition
        ("window") that needs recovery.

        No recovery underway yet, so should pause all partitions.
        """
        state_manager = state_manager_changelogs
        recovery_manager = state_manager._recovery_manager
        topic_manager = recovery_manager._topic_manager
        consumer = recovery_manager._consumer
        expected_store_name = "window"
        expected_offset = 15

        topic_name = "topic_name"
        topic_manager.topic(topic_name)
        partition_num = 1
        consumer.get_watermark_offsets.side_effect = [(0, 10), (0, 20)]
        consumer.assignment.return_value = "assignments"

        # setup state_managers `StorePartitions` (which also sets up changelog topics)
        store_partitions = {}
        for store_name, offset in [
            ("default", 10),
            (expected_store_name, expected_offset),
        ]:
            state_manager.register_store(topic_name=topic_name, store_name=store_name)
            partition = state_manager.get_store(
                topic=topic_name, store_name=store_name
            ).assign_partition(partition_num)
            patch.object(partition, "get_changelog_offset", return_value=offset).start()
            store_partitions[store_name] = partition

        recovery_manager.assign_partition(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions=store_partitions,
        )

        # expected changelog topic's partition was subscribed to
        expected_changelog_name = topic_manager.changelog_topics[topic_name][
            expected_store_name
        ].name
        assign_calls = consumer.incremental_assign.call_args_list[0].args
        assert len(assign_calls) == 1
        partition_list = assign_calls[0]
        assert isinstance(partition_list, list)
        assert len(assign_calls) == 1
        confluent_partition = partition_list[0]
        assert isinstance(confluent_partition, ConfluentPartition)
        assert expected_changelog_name == confluent_partition.topic
        assert partition_num == confluent_partition.partition
        assert expected_offset == confluent_partition.offset

        # recovery manager should also store respective RecoveryPartition
        assert recovery_manager._recovery_partitions[partition_num][
            expected_changelog_name
        ]
        assert len(recovery_manager._recovery_partitions[partition_num]) == 1

        # should pause ALL partitions
        consumer.pause.assert_called_with("assignments")

    def test_assign_partition_fix_offset_only(
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
        consumer.assignment.return_value = "assignments"

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
                lowwater=watermarks[i][0],
                highwater=watermarks[i][1],
            )
            for i in range(len(store_names))
        ]
        patch.object(
            recovery_manager,
            "_generate_recovery_partitions",
            return_value=recovery_partitions,
        ).start()
        with patch.object(recovery_partitions[1], "update_offset") as update_offset:
            recovery_manager.assign_partition(
                topic_name=topic_name,
                partition_num=partition_num,
                store_partitions="mocked_out",
            )

        # no pause or assignments should be called
        consumer.pause.assert_not_called()
        consumer.incremental_assign.assert_not_called()
        update_offset.assert_called()

    def test_assign_partition_fix_offset_during_recovery(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        From two RecoveryPartitions, fix the one ("window") that has a bad offset.

        Recovery was previously going, so must pause the source topic.
        """
        recovery_manager = recovery_manager_mock_consumer
        recovery_manager._running = True
        topic_name = "topic_name"
        partition_num = 1
        store_names = ["default", "window"]
        changelog_names = [f"changelog__{topic_name}__{store}" for store in store_names]
        watermarks = [(0, 10), (0, 20)]
        changelog_offsets = [10, 22]

        consumer = recovery_manager._consumer

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
                lowwater=watermarks[i][0],
                highwater=watermarks[i][1],
            )
            for i in range(len(store_names))
        ]

        patch.object(
            recovery_manager,
            "_generate_recovery_partitions",
            return_value=recovery_partitions,
        ).start()

        with patch.object(recovery_partitions[1], "update_offset") as update_offset:
            recovery_manager.assign_partition(
                topic_name=topic_name,
                partition_num=partition_num,
                store_partitions="mocked",
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
        recovery_manager._running = True
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
                lowwater=watermarks[i][0],
                highwater=watermarks[i][1],
            )
            for i in range(len(store_names))
        ]
        skip_recover_partition = recovery_partitions[0]
        should_recover_partition = recovery_partitions[1]

        patch.object(
            recovery_manager,
            "_generate_recovery_partitions",
            return_value=recovery_partitions,
        ).start()
        recovery_manager.assign_partition(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions="mocked_out",
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

    def test__revoke_recovery_partition(
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

        recovery_manager.revoke_partition(partition_num=partition_num)

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
            recovery_manager.revoke_partition(partition_num=partition_num)

        revoke.assert_called_with([recovery_partition], partition_num)

    def test_revoke_partition_not_assigned(self, recovery_manager_mock_consumer):
        """
        Skip revoking any recovery partitions for a given partition since none are
        currently assigned (due to not needing recovery).
        """
        recovery_manager = recovery_manager_mock_consumer
        with patch.object(recovery_manager, "_revoke_recovery_partitions") as revoke:
            recovery_manager.revoke_partition(partition_num=1)

        revoke.assert_not_called()

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
        consumer.assignment.return_value = ["assignments"]

        recovery_partitions = [
            recovery_partition_factory(
                changelog_name=changelog_names[i],
                partition_num=partition_num,
                mocked_changelog_offset=changelog_offsets[i],
                lowwater=watermarks[i][0],
                highwater=watermarks[i][1],
            )
            for i in range(len(store_names))
        ]

        patch.object(
            recovery_manager,
            "_generate_recovery_partitions",
            return_value=recovery_partitions,
        ).start()

        recovery_manager.assign_partition(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions="mocked_out",
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

    def test_do_recovery_no_recovery_needed(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer

        with patch.object(recovery_manager, "_recovery_loop") as recovery_loop:
            recovery_manager.do_recovery()

        consumer.resume.assert_not_called()
        recovery_loop.assert_not_called()

    def test__recovery_loop(
        self, recovery_manager_mock_consumer, recovery_partition_factory
    ):
        """
        Successfully recover from a changelog message, which is also the last one
        for the partition, so revoke it afterward.
        """
        recovery_manager = recovery_manager_mock_consumer
        recovery_manager._running = True
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

        rp.store_partition.recover_from_changelog_message.assert_called_with(
            changelog_message=msg
        )
        consumer.incremental_unassign.assert_called()

    def test__recovery_loop_no_partitions(self, recovery_manager_mock_consumer):
        recovery_manager = recovery_manager_mock_consumer
        consumer = recovery_manager._consumer

        recovery_manager._recovery_loop()
        consumer.poll.assert_not_called()
