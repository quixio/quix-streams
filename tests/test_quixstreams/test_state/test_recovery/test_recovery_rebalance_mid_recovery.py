import uuid
from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer
from quixstreams.models import TopicConfig, TopicManager
from quixstreams.state.base import StorePartition
from quixstreams.state.recovery import RecoveryManager
from tests.utils import ConfluentKafkaMessageStub


def _broker_topic_passthrough(topic):
    """Stand in for TopicAdmin: never touches a real broker."""
    topic.broker_config = topic.create_config
    return topic


def _build_recovery_topics(stream_id: str, store_name: str, num_partitions: int = 1):
    """
    Build a TopicManager with one data topic and its matching changelog topic,
    without touching a real broker (patches ``_get_or_create_broker_topic``, the
    same technique already used by
    ``TestRecoveryManager.test_recovery_paused_partition_is_resumed_when_reassigned``
    in test_recovery_manager.py).
    """
    topic_manager = TopicManager(
        topic_admin=MagicMock(), consumer_group=str(uuid.uuid4())
    )
    with patch.object(
        topic_manager,
        "_get_or_create_broker_topic",
        side_effect=_broker_topic_passthrough,
    ):
        data_topic = topic_manager.topic(stream_id)
        changelog_topic = topic_manager.changelog_topic(
            stream_id=stream_id,
            store_name=store_name,
            config=TopicConfig(num_partitions=num_partitions, replication_factor=1),
        )
    return topic_manager, data_topic, changelog_topic


def _add_stateless_topic(topic_manager: TopicManager, name: str):
    with patch.object(
        topic_manager,
        "_get_or_create_broker_topic",
        side_effect=_broker_topic_passthrough,
    ):
        return topic_manager.topic(name)


def _needs_recovery_store_partition(offset: int) -> MagicMock:
    store_partition = MagicMock(spec_set=StorePartition)
    store_partition.get_changelog_offset.return_value = offset
    store_partition.has_incomplete_ttl_migration.return_value = False
    return store_partition


class TestRecoveryRebalanceMidRecovery:
    """
    Bug A and Bug B (recovery.py): a rebalance that assigns/revokes/reassigns
    partitions WHILE a recovery is already in progress. Live evidence:
    pathb_cold_r1_pre_restart_9.log (Bug A, "Recovery stuck" RuntimeError),
    pathb_cold_r2_pre_restart_11.log / pathb_cold_r3_pre_restart_5.log
    (Bug B, KeyError in ``_recovery_loop``).
    """

    def test_partition_assigned_mid_recovery_is_seeked_and_resumed(self):
        """
        Bug A: validates spec/architecture invariant that recovery.py:535
        ``do_recovery`` documents as its own job -- seeking+resuming a
        changelog partition's saved offset -- must also happen for a
        partition assigned WHILE recovery is already running (recovery.py
        :671 ``assign_partition``, the ``self._running`` branch at :733).
        Currently only ``do_recovery`` seeks/resumes, once, before the loop
        starts (:552-557); a partition assigned mid-recovery via a rebalance
        never gets seeked/resumed, so its consumer position stays
        OFFSET_INVALID (-1001) forever and the 60-attempt watchdog
        (:472, :927) raises "Recovery stuck", exactly as seen live in
        pathb_cold_r1_pre_restart_9.log at 17:56:41.
        """
        stream_id = str(uuid.uuid4())
        store_name = "default"
        topic_manager, data_topic, changelog_topic = _build_recovery_topics(
            stream_id, store_name, num_partitions=2
        )

        data_tp0 = TopicPartition(data_topic.name, 0)
        data_tp1 = TopicPartition(data_topic.name, 1)
        changelog_tp0 = TopicPartition(changelog_topic.name, 0)
        changelog_tp1 = TopicPartition(changelog_topic.name, 1)

        consumer = MagicMock(spec_set=Consumer)
        consumer.assignment.return_value = [
            data_tp0,
            changelog_tp0,
            data_tp1,
            changelog_tp1,
        ]
        consumer.get_watermark_offsets.return_value = (0, 10)

        recovery_manager = RecoveryManager(
            consumer=consumer, topic_manager=topic_manager
        )

        # Assign partition 0 before recovery has started; it needs recovery.
        recovery_manager.assign_partition(
            topic=stream_id,
            partition=0,
            committed_offsets={stream_id: -1001},
            store_partitions={store_name: _needs_recovery_store_partition(offset=3)},
        )
        assert recovery_manager.partitions[0][changelog_topic.name].needs_recovery_check

        # Put the manager into the "recovery already running" state the way
        # `do_recovery` does (`_running = True`), WITHOUT driving the blocking
        # `_recovery_loop()` to completion: this isolates the `assign_partition`
        # code path under test from the (separately tested) loop behavior.
        recovery_manager._running = True
        assert recovery_manager.recovering

        consumer.seek.reset_mock()
        consumer.resume.reset_mock()

        # A rebalance now assigns partition 1, which also needs recovery,
        # while recovery for partition 0 is still active.
        recovery_manager.assign_partition(
            topic=stream_id,
            partition=1,
            committed_offsets={stream_id: -1001},
            store_partitions={store_name: _needs_recovery_store_partition(offset=3)},
        )
        rp1 = recovery_manager.partitions[1][changelog_topic.name]
        assert rp1.needs_recovery_check

        expected_seek_tp = TopicPartition(changelog_topic.name, 1, offset=rp1.offset)
        expected_resume_tps = [TopicPartition(changelog_topic.name, 1)]

        seek_calls = [c.args[0] for c in consumer.seek.call_args_list]
        resume_calls = [c.args[0] for c in consumer.resume.call_args_list]

        assert expected_seek_tp in seek_calls, (
            "changelog partition assigned during an active recovery is never "
            "seeked/resumed; only do_recovery does that, so its position "
            "stays OFFSET_INVALID and the 60-attempt watchdog raises "
            "'Recovery stuck'"
        )
        assert expected_resume_tps in resume_calls, (
            "changelog partition assigned during an active recovery is never "
            "seeked/resumed; only do_recovery does that, so its position "
            "stays OFFSET_INVALID and the 60-attempt watchdog raises "
            "'Recovery stuck'"
        )

    def test_recovery_loop_ignores_message_for_partition_not_under_recovery(self):
        """
        Bug B (loop): validates that `_recovery_loop` (recovery.py:811) must
        not assume every polled message belongs to a currently-tracked
        RecoveryPartition. On HEAD, `rp = self._recovery_partitions[msg
        .partition()][msg.topic()]` (:824) has no membership check, so a
        message for a topic/partition combination that isn't (or isn't yet)
        under recovery raises an uncaught KeyError and crashes the app --
        exactly as seen live: KeyError on the SOURCE topic name in
        pathb_cold_r2_pre_restart_11.log, and KeyError on a not-yet-tracked
        partition number in pathb_cold_r3_pre_restart_5.log.
        """
        stream_id = str(uuid.uuid4())
        store_name = "default"
        highwater = 10
        topic_manager, data_topic, changelog_topic = _build_recovery_topics(
            stream_id, store_name, num_partitions=2
        )

        consumer = MagicMock(spec_set=Consumer)
        changelog_tp0 = TopicPartition(changelog_topic.name, 0)
        consumer.assignment.return_value = [
            TopicPartition(data_topic.name, 0),
            changelog_tp0,
        ]
        consumer.get_watermark_offsets.return_value = (0, highwater)

        recovery_manager = RecoveryManager(
            consumer=consumer, topic_manager=topic_manager
        )
        store_partition = _needs_recovery_store_partition(offset=3)
        recovery_manager.assign_partition(
            topic=stream_id,
            partition=0,
            committed_offsets={stream_id: -1001},
            store_partitions={store_name: store_partition},
        )
        assert recovery_manager.partitions

        # A message from the SOURCE (non-changelog) topic on a partition that
        # DOES have a recovery check (0), followed by a changelog message for
        # a partition that has NO recovery check at all (1). Neither should
        # ever reach `RecoveryPartition.recover_from_changelog_message`.
        foreign_source_message = ConfluentKafkaMessageStub(
            topic=data_topic.name, partition=0, offset=0
        )
        foreign_changelog_message = ConfluentKafkaMessageStub(
            topic=changelog_topic.name, partition=1, offset=0
        )
        consumer.poll.side_effect = [
            foreign_source_message,
            foreign_changelog_message,
            None,
        ]
        # Once the loop reaches the (correctly) empty poll, report the tracked
        # partition as caught up so `_update_recovery_status` finishes it and
        # the loop exits on its own.
        consumer.position.return_value = [
            TopicPartition(changelog_topic.name, 0, highwater)
        ]

        # See Bug A test docstring: sets the "recovery already running" state
        # directly instead of driving `do_recovery`'s seek/resume loop.
        recovery_manager._running = True

        recovery_manager._recovery_loop()

        assert not recovery_manager.partitions, (
            "recovery loop crashes with an uncaught KeyError instead of "
            "skipping a message for a topic/partition that is not under "
            "recovery (see pathb_cold_r2_pre_restart_11.log / "
            "pathb_cold_r3_pre_restart_5.log)"
        )
        store_partition.recover_from_changelog_message.assert_not_called()

    def test_rebalance_sequence_keeps_data_partitions_paused_while_recovery_continues(
        self,
    ):
        """
        Bug B (root cause): reproduces the exact ordering `app.py
        _assign_partitions` (~:1170-1300) produces during one eager rebalance
        inside an active recovery: `consumer.assign`, then
        `on_partition_assign` per non-changelog tp in assignment order (a
        stateless source topic sorts before the stateful/repartition topic).
        `RecoveryManager.assign_partition` (recovery.py:671) only resumes
        recovery-paused data partitions when `self._recovery_partitions` is
        EMPTY (:743-744) with NO check on `self._running`; a stateless assign
        that momentarily observes an empty `_recovery_partitions` (because the
        stateful partition was just revoked and hasn't been reassigned yet)
        wrongly resumes ALL recovery-paused data partitions, including the
        one that belongs to the stream still under active recovery. The
        recovery loop then polls that now-unpaused source topic and crashes
        with KeyError (Bug B, loop test above) -- this is the mechanism
        behind pathb_cold_r2_pre_restart_11.log /
        pathb_cold_r3_pre_restart_5.log.
        """
        stateless_topic_name = f"raw-events-{uuid.uuid4()}"
        stateful_stream_id = f"repartition-{uuid.uuid4()}"
        store_name = "default"

        topic_manager, stateful_topic, changelog_topic = _build_recovery_topics(
            stateful_stream_id, store_name, num_partitions=1
        )
        stateless_topic = _add_stateless_topic(topic_manager, stateless_topic_name)

        stateless_tp0 = TopicPartition(stateless_topic.name, 0)
        stateful_tp0 = TopicPartition(stateful_topic.name, 0)
        changelog_tp0 = TopicPartition(changelog_topic.name, 0)

        class _PauseResumeTrackingConsumer:
            """Minimal consumer stub tracking net pause/resume state, in the
            style of TestRecoveryManager.
            test_recovery_paused_partition_is_resumed_when_reassigned's
            TrackingConsumer in test_recovery_manager.py."""

            def __init__(self, assignment):
                self._assignment = assignment
                self.paused: set[tuple[str, int]] = set()

            def assignment(self):
                return list(self._assignment)

            def pause(self, partitions):
                for tp in partitions:
                    self.paused.add((tp.topic, tp.partition))

            def resume(self, partitions):
                for tp in partitions:
                    self.paused.discard((tp.topic, tp.partition))

            def get_watermark_offsets(self, *_args, **_kwargs):
                return 0, 10

        consumer = _PauseResumeTrackingConsumer(
            assignment=[stateless_tp0, stateful_tp0, changelog_tp0]
        )
        recovery_manager = RecoveryManager(
            consumer=consumer, topic_manager=topic_manager
        )

        # 1) Recovery starts for the stateful partition: pauses the WHOLE
        # current assignment (both data tps + the changelog), and remembers
        # the two data tps as recovery-paused.
        recovery_manager.assign_partition(
            topic=stateful_stream_id,
            partition=0,
            committed_offsets={stateful_stream_id: -1001},
            store_partitions={store_name: _needs_recovery_store_partition(offset=3)},
        )
        assert (stateless_topic.name, 0) in recovery_manager._recovery_paused_data_tps
        assert (stateful_topic.name, 0) in recovery_manager._recovery_paused_data_tps

        # `do_recovery` would seek/resume the changelog and enter the
        # blocking loop; see Bug A test docstring for why we set `_running`
        # directly instead.
        recovery_manager._running = True

        # 2) An eager rebalance revokes partition 0 first (its store is
        # about to be closed and reopened elsewhere / reassigned).
        recovery_manager.revoke_partition(0)
        assert not recovery_manager._recovery_partitions

        # 3) `app._assign_partitions` assigns the STATELESS source topic
        # first (it sorts before the stateful/repartition topic).
        recovery_manager.assign_partition(
            topic=stateless_topic_name,
            partition=0,
            committed_offsets={},
            store_partitions={},
        )

        # 4) Then the STATEFUL stream is (re)assigned, needing recovery again.
        recovery_manager.assign_partition(
            topic=stateful_stream_id,
            partition=0,
            committed_offsets={stateful_stream_id: -1001},
            store_partitions={store_name: _needs_recovery_store_partition(offset=3)},
        )

        assert recovery_manager.recovering

        data_tps = {(stateless_topic.name, 0), (stateful_topic.name, 0)}
        assert data_tps <= consumer.paused, (
            "a stateless assign during a rebalance resumes recovery-paused "
            "data partitions while recovery is still running; the recovery "
            "loop then polls source-topic messages and crashes with KeyError"
        )

    def test_assign_before_recovery_starts_pauses_all_and_do_recovery_resumes_changelogs(
        self,
    ):
        """
        Control (must stay GREEN): the healthy, non-rebalance path. Before
        recovery has started (`_running == False`), `assign_partition`
        (recovery.py:671, :739-742) pauses the ENTIRE current assignment, and
        `do_recovery` (:535) seeks+resumes the changelog partition before
        entering the loop, then resumes the data partition once recovery
        completes. Pins this already-passing behavior so a fix for Bug A/B
        cannot regress it.
        """
        stream_id = str(uuid.uuid4())
        store_name = "default"
        highwater = 10
        topic_manager, data_topic, changelog_topic = _build_recovery_topics(
            stream_id, store_name, num_partitions=1
        )

        data_tp = TopicPartition(data_topic.name, 0)
        changelog_tp = TopicPartition(changelog_topic.name, 0)

        consumer = MagicMock(spec_set=Consumer)
        consumer.assignment.return_value = [data_tp, changelog_tp]
        consumer.get_watermark_offsets.return_value = (0, highwater)
        consumer.poll.return_value = None
        consumer.position.return_value = [
            TopicPartition(changelog_topic.name, 0, highwater)
        ]

        recovery_manager = RecoveryManager(
            consumer=consumer, topic_manager=topic_manager
        )
        recovery_manager.assign_partition(
            topic=stream_id,
            partition=0,
            committed_offsets={stream_id: -1001},
            store_partitions={store_name: _needs_recovery_store_partition(offset=3)},
        )
        assert consumer.pause.call_args_list[0].args[0] == [data_tp, changelog_tp]

        recovery_manager.do_recovery()

        seek_calls = [c.args[0] for c in consumer.seek.call_args_list]
        assert seek_calls == [TopicPartition(changelog_topic.name, 0, offset=3)]

        resume_calls = [c.args[0] for c in consumer.resume.call_args_list]
        assert resume_calls[0] == [changelog_tp]
        assert resume_calls[-1] == [data_tp]
        assert not recovery_manager.partitions
