from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from quixstreams.app import Application
from quixstreams.checkpointing.exceptions import CheckpointConsumerCommitError
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.internal_producer import InternalProducer
from quixstreams.processing.context import ProcessingContext
from quixstreams.sinks import SinkBackpressureError, SinkManager
from quixstreams.state import StateStoreManager
from quixstreams.state.base import PartitionTransaction


@pytest.fixture(params=[False, True], ids=["at-least-once", "exactly-once"])
def reset_context(request):
    producer = MagicMock(spec=InternalProducer)
    producer.flush.return_value = 0
    producer.offsets = {}
    consumer = MagicMock(spec=InternalConsumer)
    consumer.commit.return_value = []
    state_manager = MagicMock(spec=StateStoreManager)
    state_manager.recovery_required = False
    sink_manager = MagicMock(spec=SinkManager)
    sink_manager.sinks = []
    return ProcessingContext(
        commit_interval=3600,
        commit_every=1,
        producer=producer,
        consumer=consumer,
        state_manager=state_manager,
        sink_manager=sink_manager,
        dataframe_registry=DataFrameRegistry(),
        exactly_once=request.param,
    )


def committed_offsets(context):
    if context.exactly_once:
        context.consumer.commit.assert_not_called()
        call = context.producer.commit_transaction.call_args
        offsets = call.args[0]
    else:
        context.producer.commit_transaction.assert_not_called()
        call = context.consumer.commit.call_args
        offsets = call.kwargs["offsets"]
        assert call.kwargs["asynchronous"] is False
    return {(tp.topic, tp.partition): tp.offset for tp in offsets}


def test_reset_only_checkpoint_commits_without_counting_messages(reset_context):
    context = reset_context
    # Assignment can record the reset before a checkpoint exists.
    context.store_offset_reset("quiet", 2, 5)
    context.init_checkpoint()
    # Also exercise a reset recorded after the checkpoint was created.
    context.store_offset_reset("quiet", 7, 9)
    assert not context.checkpoint.empty()
    assert not context.checkpoint.expired()

    context.commit_checkpoint(force=True)

    assert committed_offsets(context) == {("quiet", 2): 5, ("quiet", 7): 9}
    assert context.checkpoint.empty()


def test_reset_waits_for_recovery_even_when_forced(reset_context):
    context = reset_context
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    context.state_manager.recovery_required = True

    context.commit_checkpoint(force=True, revoking=True)

    context.consumer.commit.assert_not_called()
    context.producer.commit_transaction.assert_not_called()
    context.state_manager.recovery_required = False
    assert not context.checkpoint.empty()
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 2): 5}


def test_reset_does_not_become_a_processed_state_offset(reset_context):
    context = reset_context
    context.dataframe_registry.register_stream_id("stream", ["quiet", "active"])
    transaction = MagicMock(spec=PartitionTransaction)
    transaction.failed = False
    transaction.changelog_topic_partition = None
    context.state_manager.get_store.return_value.start_partition_transaction.return_value = transaction
    context.init_checkpoint()
    context.checkpoint.get_store_transaction("stream", 2)
    context.store_offset_reset("quiet", 2, 5)
    context.store_offset("active", 2, 10)

    context.commit_checkpoint(force=True)

    transaction.prepare.assert_called_once_with(processed_offsets={"active": 10})
    assert committed_offsets(context) == {("quiet", 2): 5, ("active", 2): 11}


def test_checkpoint_with_messages_still_defers_unrecovered_reset(reset_context):
    context = reset_context
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    context.store_offset("active", 7, 10)
    context.state_manager.recovery_required = True

    context.commit_checkpoint(force=True)

    assert committed_offsets(context) == {("active", 7): 11}
    context.state_manager.recovery_required = False
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 2): 5}


def test_processed_message_advances_beyond_pending_reset(reset_context):
    context = reset_context
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    context.store_offset("quiet", 2, 5)
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 2): 6}
    assert context.checkpoint.empty()


def test_backpressure_retains_reset_for_next_checkpoint(reset_context):
    context = reset_context
    sink = MagicMock()
    sink.flush.side_effect = [SinkBackpressureError(retry_after=0), None]
    context.sink_manager.sinks = [sink]
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    context.store_offset("active", 2, 10)

    context.commit_checkpoint(force=True)

    context.consumer.trigger_backpressure.assert_called_once_with(
        resume_after=0, offsets_to_seek={("active", 2): 10}
    )
    context.consumer.commit.assert_not_called()
    context.producer.commit_transaction.assert_not_called()
    assert not context.checkpoint.empty()
    # Replay the actual message, retaining the pending reset alongside it.
    context.store_offset("active", 2, 10)
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 2): 5, ("active", 2): 11}
    assert context.checkpoint.empty()


def test_failed_commit_retains_reset(reset_context):
    context = reset_context
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    commit = (
        context.producer.commit_transaction
        if context.exactly_once
        else context.consumer.commit
    )
    commit.side_effect = KafkaException(KafkaError(KafkaError._TIMED_OUT))

    with pytest.raises((KafkaException, CheckpointConsumerCommitError)):
        context.commit_checkpoint(force=True)

    assert not context.checkpoint.empty()
    commit.side_effect = None
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 2): 5}
    assert context.checkpoint.empty()


def test_aborted_revoke_flush_retains_reset(reset_context):
    context = reset_context
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    context.producer.flush.return_value = 1

    context.commit_checkpoint(force=True, revoking=True)

    context.consumer.commit.assert_not_called()
    context.producer.commit_transaction.assert_not_called()
    assert not context.checkpoint.empty()
    context.producer.flush.return_value = 0
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 2): 5}


@pytest.mark.parametrize("callback", ["_on_revoke", "_on_lost"])
def test_revoke_or_loss_discards_unrecovered_resets(reset_context, callback, tmp_path):
    context = reset_context
    context.init_checkpoint()
    context.store_offset_reset("quiet", 2, 5)
    context.store_offset_reset("quiet", 7, 9)
    context.state_manager.recovery_required = True
    app = Application(broker_address="localhost:9092", state_dir=tmp_path)
    app._processing_context = context
    app._state_manager = context.state_manager
    app._state_manager.stores = {}
    app._topic_manager = MagicMock()
    app._topic_manager.non_changelog_topics = {"quiet": object()}

    getattr(app, callback)(None, [TopicPartition("quiet", 2)])

    context.consumer.commit.assert_not_called()
    context.producer.commit_transaction.assert_not_called()
    context.state_manager.recovery_required = False
    context.commit_checkpoint(force=True)
    assert committed_offsets(context) == {("quiet", 7): 9}


def test_context_exit_discards_pending_resets(reset_context):
    context = reset_context
    context.init_checkpoint()
    with context:
        context.store_offset_reset("quiet", 2, 5)
    context.init_checkpoint()
    assert context.checkpoint.empty()
