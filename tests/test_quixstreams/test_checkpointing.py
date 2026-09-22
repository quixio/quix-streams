import contextlib
import time
from threading import Event
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from quixstreams.app import Application
from quixstreams.checkpointing import Checkpoint, InvalidStoredOffset
from quixstreams.checkpointing.checkpoint import _REVOKE_FLUSH_FLOOR
from quixstreams.checkpointing.exceptions import (
    CheckpointConsumerCommitError,
    CheckpointProducerTimeout,
)
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka import Consumer
from quixstreams.kafka.consumer import BaseConsumer
from quixstreams.kafka.exceptions import KafkaProducerDeliveryError
from quixstreams.models import TopicConfig
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkManager
from quixstreams.sinks.base import SinkBatch
from quixstreams.sources.core.kafka.checkpoint import Checkpoint as SourceCheckpoint
from quixstreams.state import StateStoreManager
from quixstreams.state.base import PartitionTransaction
from quixstreams.state.exceptions import StoreNotRegisteredError, StoreTransactionFailed
from quixstreams.state.manager import SUPPORTED_STORES
from tests.utils import DummySink, make_kafka_exception


@pytest.fixture()
def checkpoint_factory(
    state_manager, internal_consumer, internal_producer_factory, topic_manager_factory
):
    def factory(
        commit_interval: float = 1,
        commit_every: int = 0,
        consumer_: Optional[InternalConsumer] = None,
        producer_: Optional[InternalProducer] = None,
        state_manager_: Optional[StateStoreManager] = None,
        sink_manager_: Optional[SinkManager] = None,
        dataframe_registry_: Optional[DataFrameRegistry] = None,
        exactly_once: bool = False,
        # -1.0 = librdkafka "infinite", matching the Checkpoint default and the
        # pre-existing bare-flush behavior so real-broker revoke tests confirm
        # changelog delivery (and the fast-revoke skip fires). Timeout-specific
        # tests set their own small value (see TestCheckpointFastRevoke).
        revoke_flush_timeout: float = -1.0,
    ):
        consumer_ = consumer_ or internal_consumer
        sink_manager_ = sink_manager_ or SinkManager()
        producer_ = producer_ or internal_producer_factory(transactional=exactly_once)
        state_manager_ = state_manager_ or state_manager
        dataframe_registry_ = dataframe_registry_ or DataFrameRegistry()
        return Checkpoint(
            commit_interval=commit_interval,
            commit_every=commit_every,
            producer=producer_,
            consumer=consumer_,
            state_manager=state_manager_,
            sink_manager=sink_manager_,
            dataframe_registry=dataframe_registry_,
            exactly_once=exactly_once,
            revoke_flush_timeout=revoke_flush_timeout,
        )

    return factory


@pytest.fixture()
def internal_producer_mock(request):
    p = MagicMock(spec_set=InternalProducer)
    p.flush.return_value = getattr(request, "param", 0)
    return p


class BackpressuredSink(BatchingSink):
    def write(self, batch: SinkBatch):
        raise SinkBackpressureError(retry_after=999)


class FailingSink(BatchingSink):
    def write(self, batch: SinkBatch):
        raise ValueError("Sink write failed")


class BlockingSink(BatchingSink):
    """A sink whose flush blocks until an external event is set (or times out)."""

    def __init__(self, release: Event):
        super().__init__()
        self._release = release

    def write(self, batch: SinkBatch):
        # Block the flush; the bounded revoke flush should time out well before
        # this returns. Released by the test in a finally to free the daemon.
        self._release.wait(30)


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestCheckpoint:
    def test_empty_true(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        assert checkpoint.empty()

    def test_empty_false(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        checkpoint.store_offset("topic", 0, 0)
        assert not checkpoint.empty()

    def test_exactly_once_init(self, checkpoint_factory):
        mock_producer = MagicMock()
        checkpoint_factory(producer_=mock_producer, exactly_once=True)
        mock_producer.begin_transaction.assert_called()

    @pytest.mark.parametrize("commit_interval, expired", [(0, True), (999, False)])
    def test_expired_with_commit_interval(
        self, commit_interval, expired, checkpoint_factory
    ):
        checkpoint = checkpoint_factory(commit_interval=commit_interval)
        assert checkpoint.expired() == expired

    def test_expired_with_commit_every(self, checkpoint_factory):
        checkpoint = checkpoint_factory(commit_interval=999, commit_every=2)
        checkpoint.store_offset("topic", 0, 0)
        assert not checkpoint.expired()

        checkpoint.store_offset("topic", 0, 1)
        assert checkpoint.expired()

    def test_expired_with_commit_every_and_commit_interval(self, checkpoint_factory):
        checkpoint = checkpoint_factory(commit_interval=0, commit_every=10)
        checkpoint.store_offset("topic", 0, 0)
        assert checkpoint.expired()

    def test_store_already_processed_offset_fails(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        checkpoint.store_offset("topic", 0, 10)
        with pytest.raises(InvalidStoredOffset):
            checkpoint.store_offset("topic", 0, 10)

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_no_state_success(
        self,
        checkpoint_factory,
        internal_consumer,
        state_manager,
        topic_factory,
        exactly_once,
    ):
        topic_name, _ = topic_factory()
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            exactly_once=exactly_once,
        )
        processed_offset = 999
        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)

        checkpoint.commit()
        tp, *_ = internal_consumer.committed(
            [TopicPartition(topic=topic_name, partition=0)]
        )
        assert tp.offset == processed_offset + 1

    def test_commit_with_state_no_changelog_success(
        self,
        checkpoint_factory,
        internal_consumer,
        state_manager_factory,
        topic_factory,
        internal_producer_mock,
    ):
        topic_name, _ = topic_factory()

        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])
        state_manager = state_manager_factory(producer=internal_producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
            dataframe_registry_=dataframe_registry,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        # Do some state updates and store the processed offset to simulate processing
        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the offset is committed
        tp, *_ = internal_consumer.committed(
            [TopicPartition(topic=topic_name, partition=0)]
        )
        assert tp.offset == processed_offset + 1

        # Check the producer is flushed
        assert internal_producer_mock.flush.call_count == 1

        # Check the state is flushed
        assert tx.completed
        new_tx = store.start_partition_transaction(0)
        assert new_tx.get(key=key, prefix=prefix) == value

        # No changelogs should be flushed
        assert not store_partition.get_changelog_offset()

    def test_commit_with_state_with_changelog_success(
        self,
        checkpoint_factory,
        internal_producer,
        internal_consumer,
        state_manager_factory,
        recovery_manager_factory,
        topic_factory,
    ):
        topic_name, _ = topic_factory()
        recovery_manager = recovery_manager_factory(consumer=internal_consumer)
        state_manager = state_manager_factory(
            producer=internal_producer, recovery_manager=recovery_manager
        )
        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])

        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            producer_=internal_producer,
            dataframe_registry_=dataframe_registry,
        )
        processed_offset = 999
        value, prefix = "value", b"__key__"
        state_manager.register_store(
            topic_name,
            "default",
            changelog_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        # Do a couple of state updates to send more messages to the changelog
        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key="key1", value=value, prefix=prefix)
        tx.set(key="key2", value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the state is flushed
        assert tx.completed

        # Check the changelog offset
        # The changelog offset should increase by number of updated keys
        # Since no offset recorded yet, an increase of 2 from no offset is 1
        assert store_partition.get_changelog_offset() == 1

    def test_commit_revoking_skips_local_flush_with_changelog(
        self,
        checkpoint_factory,
        internal_producer,
        internal_consumer,
        state_manager_factory,
        recovery_manager_factory,
        topic_factory,
    ):
        """
        Fast revoke: when committing a checkpoint during a partition revoke and
        changelogs are enabled, the local state flush must be skipped. The
        changelog holds the delta and the new owner replays it, so the outgoing
        instance releases its RocksDB lock as fast as possible.
        """
        topic_name, _ = topic_factory()
        recovery_manager = recovery_manager_factory(consumer=internal_consumer)
        state_manager = state_manager_factory(
            producer=internal_producer, recovery_manager=recovery_manager
        )
        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])

        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            producer_=internal_producer,
            dataframe_registry_=dataframe_registry,
        )
        value, prefix = "value", b"__key__"
        state_manager.register_store(
            topic_name,
            "default",
            changelog_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key="key1", value=value, prefix=prefix)
        tx.set(key="key2", value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, 999)

        # Commit the checkpoint as part of a revoke
        checkpoint.commit(revoking=True)

        # The changelog was produced (prepared) but the local flush was skipped:
        # nothing was written to disk, so no changelog offset is persisted.
        assert tx.prepared
        assert not tx.completed
        assert not store_partition.get_changelog_offset()

    def test_commit_revoking_still_flushes_without_changelog(
        self,
        checkpoint_factory,
        internal_consumer,
        state_manager_factory,
        topic_factory,
        internal_producer_mock,
    ):
        """
        Fast revoke is only safe when changelogs are enabled. With changelogs
        disabled, skipping the flush would be state loss, so a revoking commit
        must fall back to the full local flush.
        """
        topic_name, _ = topic_factory()

        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])
        state_manager = state_manager_factory(producer=internal_producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
            dataframe_registry_=dataframe_registry,
        )
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store.assign_partition(0)

        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, 999)

        checkpoint.commit(revoking=True)

        # No changelog -> full flush must still happen, state is persisted
        assert tx.completed
        new_tx = store.start_partition_transaction(0)
        assert new_tx.get(key=key, prefix=prefix) == value

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_with_state_and_changelog_no_updates_success(
        self,
        checkpoint_factory,
        internal_producer_factory,
        internal_consumer,
        state_manager_factory,
        recovery_manager_factory,
        topic_factory,
        exactly_once,
    ):
        topic_name, _ = topic_factory()
        internal_producer = internal_producer_factory(transactional=exactly_once)
        recovery_manager = recovery_manager_factory(consumer=internal_consumer)
        state_manager = state_manager_factory(
            producer=internal_producer, recovery_manager=recovery_manager
        )
        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])

        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            producer_=internal_producer,
            dataframe_registry_=dataframe_registry,
            exactly_once=exactly_once,
        )
        processed_offset = 999
        state_manager.register_store(
            topic_name,
            "default",
            changelog_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        store = state_manager.get_store(topic_name, "default")
        store_partition = store.assign_partition(0)

        # Create a transaction but don't update any keys
        tx = checkpoint.get_store_transaction(topic_name, 0)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Commit the checkpoint
        checkpoint.commit()

        # Check the transaction is not flushed
        assert tx.completed

        # The changelog and processed offsets should be empty because no updates
        # happened during the transaction
        assert not store_partition.get_changelog_offset()

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_close_no_offsets(
        self,
        checkpoint_factory,
        internal_producer_mock,
        exactly_once,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = MagicMock(spec_set=StateStoreManager)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
            exactly_once=exactly_once,
        )
        # Commit the checkpoint without processing any messages
        checkpoint.close()

        if exactly_once:
            # transaction should also be aborted
            assert internal_producer_mock.abort_transaction.call_count
        else:
            assert not internal_producer_mock.abort_transaction.call_count

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_has_failed_transactions_fails(
        self,
        checkpoint_factory,
        state_manager_factory,
        topic_factory,
        internal_producer_mock,
        exactly_once,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=internal_producer_mock)
        dataframe_registry = DataFrameRegistry()
        topic_name = "topic"
        dataframe_registry.register_stream_id(topic_name, [topic_name])
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
            dataframe_registry_=dataframe_registry,
            exactly_once=exactly_once,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store.assign_partition(0)

        # Simulate a failed transaction
        tx = checkpoint.get_store_transaction("topic", 0)
        with (
            contextlib.suppress(ValueError),
            patch.object(
                PartitionTransaction,
                "_serialize_key",
                side_effect=ValueError("test"),
            ),
        ):
            tx.set(key=key, value=value, prefix=prefix)
        assert tx.failed

        # Store offset to simulate processing
        checkpoint.store_offset("topic", 0, processed_offset)

        # Checkpoint commit should fail if any of the transaction is failed
        # but the original exception was swallowed by an error callback
        with pytest.raises(StoreTransactionFailed):
            checkpoint.commit()

        # The producer should not flush
        assert not internal_producer_mock.flush.call_count

        # Check nothing is committed
        assert not internal_producer_mock.commit_transaction.call_count
        assert not consumer_mock.commit.call_count

    @pytest.mark.parametrize("exactly_once", [False, True])
    def test_commit_producer_flush_fails(
        self,
        checkpoint_factory,
        state_manager_factory,
        topic_factory,
        internal_producer_mock,
        exactly_once,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=internal_producer_mock)
        topic_name = "topic"
        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
            exactly_once=exactly_once,
            dataframe_registry_=dataframe_registry,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store.assign_partition(0)

        # Do some state updates and store the processed offset to simulate processing
        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        internal_producer_mock.flush.side_effect = ValueError("Flush failure")
        # Checkpoint commit should fail if producer failed to flush
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Nothing should commit
        assert not internal_producer_mock.commit_transaction.call_count
        assert not consumer_mock.commit.call_count
        # The transaction should remain prepared, but not completed
        assert tx.prepared
        assert not tx.completed

    def test_commit_consumer_commit_fails(
        self,
        checkpoint_factory,
        state_manager_factory,
        topic_factory,
        internal_producer_mock,
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        state_manager = state_manager_factory(producer=internal_producer_mock)
        topic_name = "topic"
        dataframe_registry = DataFrameRegistry()
        dataframe_registry.register_stream_id(topic_name, [topic_name])

        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            dataframe_registry_=dataframe_registry,
            producer_=internal_producer_mock,
        )
        processed_offset = 999
        key, value, prefix = "key", "value", b"__key__"
        state_manager.register_store(topic_name, "default")
        store = state_manager.get_store(topic_name, "default")
        store.assign_partition(0)

        # Do some state updates and store the processed offset to simulate processing
        tx = checkpoint.get_store_transaction(topic_name, 0)
        tx.set(key=key, value=value, prefix=prefix)
        checkpoint.store_offset(topic_name, 0, processed_offset)

        consumer_mock.commit.side_effect = ValueError("Commit failure")
        # Checkpoint commit should fail if consumer failed to commit
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Producer should flush
        assert internal_producer_mock.flush.call_count
        # The transaction should remain prepared, but not completed
        assert tx.prepared
        assert not tx.completed

    def test_get_store_transaction_store_not_registered_fails(self, checkpoint_factory):
        checkpoint = checkpoint_factory()
        with pytest.raises(StoreNotRegisteredError):
            with checkpoint.get_store_transaction("topic", 0, "default"):
                ...

    def test_get_store_transaction_success(self, checkpoint_factory, state_manager):
        state_manager.register_store("topic", "default")
        store = state_manager.get_store("topic", "default")
        topic_name = "topic"
        store.assign_partition(0)

        checkpoint = checkpoint_factory(state_manager_=state_manager)
        tx = checkpoint.get_store_transaction(topic_name, 0, "default")
        assert tx
        tx2 = checkpoint.get_store_transaction(topic_name, 0, "default")
        assert tx2 is tx

    @pytest.mark.parametrize("internal_producer_mock", [1], indirect=True)
    def test_incomplete_flush(
        self,
        checkpoint_factory,
        internal_consumer,
        state_manager_factory,
        internal_producer_mock,
    ):
        state_manager = state_manager_factory(producer=internal_producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
        )
        checkpoint.store_offset("topic", 0, 0)

        with pytest.raises(CheckpointProducerTimeout) as err:
            checkpoint.commit()

        assert (
            str(err.value)
            == "'1' messages failed to be produced before the producer flush timeout"
        )

    def test_failed_commit(
        self, checkpoint_factory, state_manager_factory, internal_producer_mock
    ):
        consumer_mock = MagicMock(spec_set=Consumer)
        consumer_mock.commit.side_effect = KafkaException(KafkaError(1, "test error"))

        state_manager = state_manager_factory(producer=internal_producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
        )
        checkpoint.store_offset("topic", 0, 0)

        with pytest.raises(CheckpointConsumerCommitError) as err:
            checkpoint.commit()

        assert (
            str(err.value)
            == '<CheckpointConsumerCommitError code="1" description="test error">'
        )

    def test_failed_commit_partition(
        self, checkpoint_factory, state_manager_factory, internal_producer_mock
    ):
        consumer_mock = MagicMock(spec_set=Consumer)

        topic_partition = MagicMock(spec=TopicPartition)
        topic_partition.error = KafkaError(1, "test error")

        consumer_mock.commit.return_value = [topic_partition]
        state_manager = state_manager_factory(producer=internal_producer_mock)
        checkpoint = checkpoint_factory(
            consumer_=consumer_mock,
            state_manager_=state_manager,
            producer_=internal_producer_mock,
        )
        checkpoint.store_offset("topic", 0, 0)

        with pytest.raises(CheckpointConsumerCommitError) as err:
            checkpoint.commit()

        assert (
            str(err.value)
            == '<CheckpointConsumerCommitError code="1" description="test error">'
        )

    def test_commit_with_sink_success(
        self,
        topic_factory,
        internal_consumer,
        state_manager,
        checkpoint_factory,
        state_manager_factory,
        internal_producer_mock,
    ):
        topic_name, _ = topic_factory()
        sink_manager = SinkManager()
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            sink_manager_=sink_manager,
        )

        processed_offset = 999
        value, key, timestamp, headers = "value", "key", 1, []
        # Create two dummy sinks
        sink1 = DummySink()
        sink2 = DummySink()
        # Register sinks and add messages to them
        for sink in (sink1, sink2):
            sink_manager.register(sink)
            sink.add(
                value=value,
                key=key,
                timestamp=timestamp,
                topic=topic_name,
                partition=0,
                headers=headers,
                offset=processed_offset,
            )

        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)
        checkpoint.commit()

        # Ensure that both sinks has been flushed
        for sink in (sink1, sink2):
            assert len(sink.results) == 1
            sink_result = sink.results[0]
            assert sink_result.value == value
            assert sink_result.key == key
            assert sink_result.timestamp == timestamp
            assert sink_result.headers == headers

    def test_commit_with_sink_fails(
        self,
        topic_factory,
        internal_consumer,
        state_manager,
        checkpoint_factory,
        state_manager_factory,
        internal_producer_mock,
    ):
        topic_name, _ = topic_factory()
        sink_manager = SinkManager()
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            sink_manager_=sink_manager,
        )

        # Create a failing sink and register it
        sink = FailingSink()
        sink_manager.register(sink)

        processed_offset = 999
        value, key, timestamp, headers = "value", "key", 1, []
        sink.add(
            value=value,
            key=key,
            timestamp=timestamp,
            topic=topic_name,
            partition=0,
            headers=headers,
            offset=processed_offset,
        )

        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic_name, 0, processed_offset)

        # Ensure that the error in Sink is propagated and fails the checkpoint
        with pytest.raises(ValueError):
            checkpoint.commit()

        # Ensure that the offset has not been committed
        committed, *_ = internal_consumer.committed(
            [TopicPartition(topic=topic_name, partition=0)]
        )
        assert committed.offset == -1001

    def test_commit_with_sink_backpressured(
        self,
        internal_consumer,
        state_manager,
        checkpoint_factory,
        state_manager_factory,
        internal_producer_mock,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory()
        sink_manager = SinkManager()
        checkpoint = checkpoint_factory(
            consumer_=internal_consumer,
            state_manager_=state_manager,
            sink_manager_=sink_manager,
        )
        # First get some topic partitions assigned because the InternalConsumer will be
        # seeking to the committed offsets
        internal_consumer.subscribe([topic])
        while not internal_consumer.assignment():
            internal_consumer.poll(0.1)

        # Create sinks and register them
        backpressured_sink = BackpressuredSink()
        dummy_sink = DummySink()

        # It's important to register the backpressured sink first for this test
        sink_manager.register(backpressured_sink)
        sink_manager.register(dummy_sink)

        processed_offset = 999
        value, key, timestamp, headers = "value", "key", 1, []
        for sink in (backpressured_sink, dummy_sink):
            sink.add(
                value=value,
                key=key,
                timestamp=timestamp,
                topic=topic.name,
                partition=0,
                headers=headers,
                offset=processed_offset,
            )

        assert dummy_sink.total_batched == 1

        # Store the processed offset to simulate processing
        checkpoint.store_offset(topic.name, 0, processed_offset)

        checkpoint.commit()

        # Ensure that the offset has not been committed because of a backpressure
        committed, *_ = internal_consumer.committed(
            [TopicPartition(topic=topic.name, partition=0)]
        )
        assert committed.offset == -1001

        # Ensure that DummySink has not been flushed because
        # the FailingSink is backpressured
        assert not dummy_sink.results
        # Ensure that DummySink dropped the accumulated batch
        assert not dummy_sink.total_batched


class TestCheckpointFastRevoke:
    """
    Kafka-free unit tests for the "fast revoke" flush-skip logic in
    Checkpoint.commit(revoking=True). See docs/rocksdb-lock-contention-analysis.md.
    """

    def _make_checkpoint(self, exactly_once=False, revoke_flush_timeout=0.1):
        producer = MagicMock(spec_set=InternalProducer)
        producer.flush.return_value = 0
        producer.offsets = {}
        consumer = MagicMock(spec_set=InternalConsumer)
        consumer.commit.return_value = []
        registry = MagicMock(spec_set=DataFrameRegistry)
        registry.get_topics_for_stream_id.return_value = []
        return Checkpoint(
            commit_interval=1,
            producer=producer,
            consumer=consumer,
            state_manager=MagicMock(spec_set=StateStoreManager),
            sink_manager=SinkManager(),
            dataframe_registry=registry,
            exactly_once=exactly_once,
            revoke_flush_timeout=revoke_flush_timeout,
        )

    def _transaction(self, changelog_tp):
        tx = MagicMock(spec_set=PartitionTransaction)
        tx.failed = False
        tx.changelog_topic_partition = changelog_tp
        return tx

    def test_revoke_skips_flush_only_for_changelog_backed_transactions(self):
        checkpoint = self._make_checkpoint()
        tx_changelog = self._transaction(changelog_tp=("changelog", 0))
        tx_no_changelog = self._transaction(changelog_tp=None)
        checkpoint._store_transactions = {
            ("s1", 0, "default"): tx_changelog,
            ("s2", 0, "default"): tx_no_changelog,
        }

        checkpoint.commit(revoking=True)

        # Changelog is always produced (prepared) so the new owner can replay
        tx_changelog.prepare.assert_called_once()
        # ...but the slow local flush is skipped while holding the lock
        tx_changelog.flush.assert_not_called()
        # Without a changelog, skipping would be state loss -> must still flush
        tx_no_changelog.flush.assert_called_once()

    def test_normal_commit_flushes_changelog_backed_transactions(self):
        checkpoint = self._make_checkpoint()
        tx_changelog = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx_changelog}

        checkpoint.commit(revoking=False)

        # Outside of a revoke, everything flushes as before
        tx_changelog.flush.assert_called_once()
        # Normal-path invariant: the producer is flushed with no timeout arg,
        # byte-identical to the pre-existing behavior.
        checkpoint._producer.flush.assert_called_once_with()

    def test_revoke_producer_flush_timeout_aborts_without_offset_commit(self):
        """
        Finding 1 (revised): when the bounded producer flush times out on revoke
        (undelivered changelog messages remain), changelog delivery is
        unconfirmed, so the checkpoint aborts without committing offsets. It must
        NOT raise CheckpointProducerTimeout (that is the normal-path behavior
        only). The queued messages are purged so they cannot deliver after the
        handover, and no local state flush happens (step 5 is not reached).
        """
        checkpoint = self._make_checkpoint(revoke_flush_timeout=0.1)
        # One changelog message could not be delivered within the budget
        checkpoint._producer.flush.return_value = 1
        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}

        # Must not raise on the revoke path
        checkpoint.commit(revoking=True)

        # Producer flushed with the floored revoke budget: revoke_flush_timeout
        # (0.1s) is below _REVOKE_FLUSH_FLOOR, so the EOS-critical changelog flush
        # is guaranteed at least the floor (a slow sink must not starve it). It
        # still reports undelivered here (mocked return 1) -> the checkpoint aborts.
        checkpoint._producer.flush.assert_called_once()
        assert (
            checkpoint._producer.flush.call_args.kwargs["timeout"]
            >= _REVOKE_FLUSH_FLOOR
        )
        # The changelog was prepared (step 2), but the checkpoint then aborted:
        tx.prepare.assert_called_once()
        # No offset commit (step 4 skipped)
        checkpoint._consumer.commit.assert_not_called()
        # No local state flush (step 5 skipped)
        tx.flush.assert_not_called()
        # Queued messages purged so they cannot deliver after the handover
        checkpoint._producer.purge.assert_called_once()

    def test_revoke_confirmed_delivery_still_skips_local_flush(self):
        """
        Finding 1 companion: when the bounded producer flush confirms delivery
        (0 undelivered), the fast-revoke skip still fires for changelog-backed
        stores, and the flush used the revoke timeout.
        """
        checkpoint = self._make_checkpoint(revoke_flush_timeout=0.1)
        checkpoint._producer.flush.return_value = 0
        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}

        checkpoint.commit(revoking=True)

        checkpoint._producer.flush.assert_called_once()
        # revoke_flush_timeout (0.1s) is below _REVOKE_FLUSH_FLOOR, so the
        # changelog flush is floored to the reserved minimum.
        assert (
            checkpoint._producer.flush.call_args.kwargs["timeout"]
            >= _REVOKE_FLUSH_FLOOR
        )
        tx.prepare.assert_called_once()
        tx.flush.assert_not_called()

    def test_normal_commit_raises_on_producer_flush_timeout(self):
        """
        Finding 1 regression: the normal (non-revoke) path still raises
        CheckpointProducerTimeout when messages remain undelivered, and flushes
        with no timeout argument.
        """
        checkpoint = self._make_checkpoint()
        checkpoint._producer.flush.return_value = 1
        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}

        with pytest.raises(CheckpointProducerTimeout):
            checkpoint.commit(revoking=False)

        checkpoint._producer.flush.assert_called_once_with()

    def test_revoke_sink_failure_aborts_without_offset_commit(self):
        """
        Finding 2: a non-backpressure sink failure on the revoke path aborts the
        checkpoint via an early return - no producer flush, no offset commit -
        and does not raise.
        """
        checkpoint = self._make_checkpoint(revoke_flush_timeout=0.1)
        sink = FailingSink()
        sink.add(
            value="v",
            key="k",
            timestamp=1,
            headers=[],
            topic="t",
            partition=0,
            offset=5,
        )
        checkpoint._sink_manager.register(sink)
        checkpoint.store_offset("t", 0, 5)

        # Must not raise; aborts early
        checkpoint.commit(revoking=True)

        # Step 3+ never reached: no producer flush, no offset commit
        checkpoint._producer.flush.assert_not_called()
        checkpoint._consumer.commit.assert_not_called()
        checkpoint._producer.commit_transaction.assert_not_called()

    def test_revoke_sink_timeout_aborts_within_budget(self):
        """
        Finding 2: a slow/unreachable sink on the revoke path is bounded by
        revoke_flush_timeout; commit returns promptly (well under the sink's own
        blocking time) with no offset commit.
        """
        checkpoint = self._make_checkpoint(revoke_flush_timeout=0.1)
        release = Event()
        sink = BlockingSink(release)
        sink.add(
            value="v",
            key="k",
            timestamp=1,
            headers=[],
            topic="t",
            partition=0,
            offset=5,
        )
        checkpoint._sink_manager.register(sink)
        checkpoint.store_offset("t", 0, 5)

        try:
            start = time.monotonic()
            checkpoint.commit(revoking=True)
            elapsed = time.monotonic() - start

            # Returned within ~revoke_flush_timeout (+ generous margin), not the
            # 30s the sink would otherwise block for.
            assert elapsed < 5.0
            checkpoint._producer.flush.assert_not_called()
            checkpoint._consumer.commit.assert_not_called()
        finally:
            # Release the orphaned daemon flush thread.
            release.set()

    def test_revoke_sink_timeout_under_eos_must_abort_transaction(self):
        """
        Finding 2 under exactly-once: when a slow sink times out on the revoke
        path, the early return aborts the open Kafka transaction (so the next
        Checkpoint can begin one) and never commits it.
        """
        checkpoint = self._make_checkpoint(exactly_once=True, revoke_flush_timeout=0.1)
        release = Event()
        sink = BlockingSink(release)
        sink.add(
            value="v",
            key="k",
            timestamp=1,
            headers=[],
            topic="t",
            partition=0,
            offset=5,
        )
        checkpoint._sink_manager.register(sink)
        checkpoint.store_offset("t", 0, 5)

        try:
            checkpoint.commit(revoking=True)

            # The open EOS transaction is aborted, never committed.
            checkpoint._producer.abort_transaction.assert_called()
            checkpoint._producer.commit_transaction.assert_not_called()
        finally:
            # Release the orphaned daemon flush thread.
            release.set()

    def test_revoke_sink_backpressure_still_triggers_backpressure(self):
        """
        Finding 2: backpressure on the revoke path is preserved - the bounded
        helper re-raises SinkBackpressureError to the caller so the existing
        pause/seek handling runs (no offset commit).
        """
        checkpoint = self._make_checkpoint(revoke_flush_timeout=0.5)
        sink = BackpressuredSink()
        sink.add(
            value="v",
            key="k",
            timestamp=1,
            headers=[],
            topic="t",
            partition=0,
            offset=5,
        )
        checkpoint._sink_manager.register(sink)
        checkpoint.store_offset("t", 0, 5)

        checkpoint.commit(revoking=True)

        # Backpressure path: consumer paused/seek-ed, nothing committed
        checkpoint._consumer.trigger_backpressure.assert_called_once()
        checkpoint._producer.flush.assert_not_called()
        checkpoint._consumer.commit.assert_not_called()

    def test_exception_handler_suppresses_rocksdb_open_aborted(self):
        """
        Finding 8: Application._exception_handler treats RocksDBOpenAborted as a
        graceful stop - suppresses it (returns True) and keeps _failed False so
        the consumer close still commits healthy partitions in _on_revoke.
        A generic exception fails and is not suppressed; SourceException does not
        fail.
        """
        from quixstreams.sources import SourceException
        from quixstreams.state.rocksdb import RocksDBOpenAborted

        app = MagicMock()
        # Bind the real method so we exercise the production logic.
        handler = Application._exception_handler

        app._failed = False
        assert handler(app, RocksDBOpenAborted, RocksDBOpenAborted("x"), None) is True
        app.stop.assert_called_once_with(fail=False)

        app.reset_mock()
        assert handler(app, ValueError, ValueError("boom"), None) is False
        app.stop.assert_called_once_with(fail=True)

        app.reset_mock()
        assert (
            handler(app, SourceException, SourceException(MagicMock()), None) is False
        )
        app.stop.assert_called_once_with(fail=False)

    def test_exactly_once_revoke_skips_flush_and_commits_transaction(self):
        """
        Fast revoke composes with exactly-once: offsets are committed via the
        producer transaction as usual, while the local state flush is still
        skipped for changelog-backed stores.
        """
        checkpoint = self._make_checkpoint(exactly_once=True)
        tx_changelog = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx_changelog}
        checkpoint.store_offset("topic", 0, 10)

        checkpoint.commit(revoking=True)

        tx_changelog.prepare.assert_called_once()
        tx_changelog.flush.assert_not_called()
        # Offsets still go through the producer transaction, not the consumer
        checkpoint._producer.commit_transaction.assert_called_once()
        checkpoint._consumer.commit.assert_not_called()

    def test_commit_tolerates_none_commit_result(self):
        """
        Consumer.commit(asynchronous=False) may return None; iterating the
        result for per-partition errors must not raise on it.
        """
        checkpoint = self._make_checkpoint()
        checkpoint._consumer.commit.return_value = None
        checkpoint.store_offset("topic", 0, 10)

        checkpoint.commit()  # must not raise

        checkpoint._consumer.commit.assert_called_once()

    def test_revoke_flush_delivery_error_still_purges_and_does_not_raise(self):
        """
        Contract: a KafkaProducerDeliveryError raised by flush() during a
        revoking commit (step 3) must be caught and routed into the
        abort-or-purge path. Under ALOS the queued messages are purged, no
        offset is committed, and no exception propagates out of commit().
        Validates review finding B (delivery error bypasses abort/purge branch).
        """
        checkpoint = self._make_checkpoint(exactly_once=False, revoke_flush_timeout=0.1)
        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}
        checkpoint.store_offset("topic", 0, 10)

        # Simulate a delivery error raised by _raise_for_error inside flush()
        checkpoint._producer.flush.side_effect = KafkaProducerDeliveryError(
            KafkaError(KafkaError._MSG_TIMED_OUT)
        )

        # Must NOT raise — the error must be caught and the checkpoint aborted
        checkpoint.commit(revoking=True)

        # ALOS: queued messages should be purged (zombie-write prevention)
        checkpoint._producer.purge.assert_called_once()
        # No offset commit (at-least-once: new owner reprocesses)
        checkpoint._consumer.commit.assert_not_called()

    def test_revoke_eos_abort_uses_bounded_timeout(self):
        """
        Contract: _abort_transaction_if_eos on the revoke path must pass a
        bounded positive timeout to abort_transaction so it cannot block
        indefinitely inside the rebalance callback. ProcessingContext.__exit__
        uses abort_transaction(5) as precedent; any finite positive value
        derived from the revoke budget is acceptable.
        Validates review finding C (unbudgeted abort).
        """
        checkpoint = self._make_checkpoint(exactly_once=True, revoke_flush_timeout=30)
        # Trigger the undelivered-message abort branch (EOS path)
        checkpoint._producer.flush.return_value = 1
        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}
        checkpoint.store_offset("topic", 0, 10)

        checkpoint.commit(revoking=True)

        # abort_transaction must have been called with a bounded positive timeout
        checkpoint._producer.abort_transaction.assert_called_once()
        args, kwargs = checkpoint._producer.abort_transaction.call_args
        timeout = kwargs.get("timeout", args[0] if args else None)
        assert timeout is not None, (
            "abort_transaction called with no timeout argument "
            "(would block indefinitely inside rebalance callback)"
        )
        assert (
            0 < timeout <= 30
        ), f"abort_transaction timeout must be bounded and positive, got {timeout}"

    def test_revoke_eos_abort_retries_retriable_error_and_does_not_escape(self):
        """
        On the revoke path a transient retriable abort error (e.g. _TIMED_OUT
        during a coordinator blip) must be retried per confluent-kafka's abort
        contract instead of propagating on the first timeout. If it escaped
        commit(revoking=True), _on_revoke would skip _revoke_state_partitions and
        leak the RocksDB lock (the livelock this branch fixes). Uses a real
        InternalProducer (inner Producer mocked) so the retry loop is exercised
        end-to-end through the revoke path.

        A large revoke_flush_timeout (with sleep patched) keeps the shared budget
        from being exhausted by real wall-clock time between attempts, so the
        retry is deterministic (no monotonic-clock CI flake).
        """
        with (
            patch("quixstreams.internal_producer.Producer") as producer_cls,
            patch("quixstreams.internal_producer.sleep"),
        ):
            inner = producer_cls.return_value
            # Undelivered changelog on the bounded flush -> revoke abort branch.
            inner.flush.return_value = 1
            # Abort fails once with a retriable error, then succeeds on retry.
            inner.abort_transaction.side_effect = [
                make_kafka_exception(retriable=True),
                None,
            ]
            producer = InternalProducer(broker_address="xyz", transactional=True)

            registry = MagicMock(spec_set=DataFrameRegistry)
            registry.get_topics_for_stream_id.return_value = ["topic"]
            checkpoint = Checkpoint(
                commit_interval=1,
                producer=producer,
                consumer=MagicMock(spec_set=InternalConsumer),
                state_manager=MagicMock(spec_set=StateStoreManager),
                sink_manager=SinkManager(),
                dataframe_registry=registry,
                exactly_once=True,
                revoke_flush_timeout=30,
            )
            tx = self._transaction(changelog_tp=("changelog", 0))
            checkpoint._store_transactions = {("topic", 0, "default"): tx}
            checkpoint.store_offset("topic", 0, 10)

            # Must NOT raise: the transient retriable abort is retried away, so
            # the exception cannot escape before _revoke_state_partitions runs.
            checkpoint.commit(revoking=True)

        # The abort was retried (called more than once) rather than propagating.
        assert inner.abort_transaction.call_count == 2

    def test_idle_close_eos_abort_is_bounded(self):
        """
        R4-1 (red): closing an EMPTY checkpoint OFF the revoke path
        (revoking=False -- the idle main-loop close / graceful shutdown) under
        exactly-once must abort with a FINITE budget, never None (librdkafka -1 =
        block up to transaction.timeout.ms). Round 4 passed None here, so a
        coordinator outage could block the consumer thread past
        max.poll.interval.ms. Any finite positive budget is acceptable.
        """
        checkpoint = self._make_checkpoint(exactly_once=True)

        checkpoint.close(revoking=False)

        checkpoint._producer.abort_transaction.assert_called_once()
        args, kwargs = checkpoint._producer.abort_transaction.call_args
        timeout = kwargs.get("timeout", args[0] if args else None)
        assert timeout is not None, "idle EOS abort must be bounded, not None"
        assert (
            0 < timeout <= 60
        ), f"idle EOS abort timeout must be bounded and positive, got {timeout}"

    def test_revoke_eos_commit_transaction_is_bounded(self):
        """
        SWEEP (red): on the EOS revoke happy path the transaction commit itself
        must be bounded by the shared revoke budget. Round 4 bounded sink-flush +
        producer-flush + abort on revoke but called commit_transaction with no
        timeout, so it could block ~3x transaction.timeout.ms inside the
        rebalance callback (and on failure escape _on_revoke, leaking the lock).
        """
        checkpoint = self._make_checkpoint(exactly_once=True, revoke_flush_timeout=30)
        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}
        checkpoint.store_offset("topic", 0, 10)

        checkpoint.commit(revoking=True)

        checkpoint._producer.commit_transaction.assert_called_once()
        args, kwargs = checkpoint._producer.commit_transaction.call_args
        timeout = kwargs.get("timeout", args[2] if len(args) > 2 else None)
        assert timeout is not None, (
            "revoke EOS commit_transaction must be bounded (an unbounded commit "
            "can block the rebalance callback up to ~3x transaction.timeout.ms)"
        )
        assert (
            0 < timeout <= 30
        ), f"revoke commit_transaction timeout must be bounded, got {timeout}"

    def test_slow_sink_does_not_starve_changelog_flush_on_revoke(self):
        """
        Round-5 review finding #1 (red): the single shared revoke budget let a
        slow-but-SUCCESSFUL sink flush (step 1) drain deadline.remaining() toward
        0, leaving the EOS-critical changelog producer flush (step 3) and commit
        (step 4) a near-zero timeout. The starved flush returns while the
        just-produced changelog is still in flight (unproduced > 0), so the
        checkpoint ABORTS without committing offsets and the new owner
        reprocesses -> duplicate sink writes on every rebalance with a slow sink.

        _REVOKE_FLUSH_FLOOR guarantees the changelog flush + commit a minimum
        budget a slow sink cannot starve. Contract: even after the sink consumes
        most of an (> floor) revoke budget, the changelog flush is given at least
        the floor, confirms delivery, and the checkpoint COMMITS the transaction
        instead of aborting.

        Red on Round-5 code (flush gets deadline.remaining() ~= budget - sink,
        which is < floor -> _flush() reports undelivered -> abort); green once the
        flush + commit are floored at _REVOKE_FLUSH_FLOOR.
        """
        # Full revoke budget deliberately ABOVE the floor, so a starved flush is
        # caused by the slow sink draining the shared budget -- not by a budget
        # that is itself below the floor.
        revoke_flush_timeout = _REVOKE_FLUSH_FLOOR + 0.5
        checkpoint = self._make_checkpoint(
            exactly_once=True, revoke_flush_timeout=revoke_flush_timeout
        )

        # Model the wall-clock starvation deterministically: the just-produced
        # changelog is reported delivered (0 undelivered) only when the flush is
        # given at least the reserved floor; a starved (< floor) timeout returns
        # with the changelog still in flight.
        def _flush(timeout=None):
            if timeout is not None and timeout >= _REVOKE_FLUSH_FLOOR:
                return 0
            return 1

        checkpoint._producer.flush.side_effect = _flush

        # A slow-but-SUCCESSFUL sink: it sleeps past (budget - floor) so that
        # deadline.remaining() at the changelog flush is guaranteed below the
        # floor, then returns cleanly (no timeout, no error).
        sink_sleep = (revoke_flush_timeout - _REVOKE_FLUSH_FLOOR) + 0.25

        class _SlowOkSink(BatchingSink):
            def write(self, batch):
                pass

            def flush(self):
                time.sleep(sink_sleep)

        checkpoint._sink_manager.register(_SlowOkSink())

        tx = self._transaction(changelog_tp=("changelog", 0))
        checkpoint._store_transactions = {("s1", 0, "default"): tx}
        checkpoint.store_offset("topic", 0, 10)

        checkpoint.commit(revoking=True)

        # Contract: the changelog flush was floored despite the slow sink
        # draining the shared budget below the floor.
        checkpoint._producer.flush.assert_called_once()
        assert (
            checkpoint._producer.flush.call_args.kwargs["timeout"]
            >= _REVOKE_FLUSH_FLOOR
        ), (
            "changelog producer flush was starved by the slow sink; it must be "
            "guaranteed at least _REVOKE_FLUSH_FLOOR on the revoke path"
        )
        # Behavior: the EOS transaction was COMMITTED (offsets committed), not
        # aborted -> the new owner does NOT reprocess and re-write the sinks.
        checkpoint._producer.commit_transaction.assert_called_once()
        checkpoint._producer.abort_transaction.assert_not_called()
        # The commit itself is likewise floored.
        assert (
            checkpoint._producer.commit_transaction.call_args.kwargs["timeout"]
            >= _REVOKE_FLUSH_FLOOR
        )

    def test_flush_sink_bounded_sentinel_means_infinite_not_zero(self):
        """
        R4-5 (red): the -1.0 "infinite" sentinel must map to an unbounded join
        (block until the flush completes), NOT CPython's join(-1)->join(0), which
        returns immediately and reports a false timeout. A sink that finishes
        shortly must flush cleanly (return True), not be aborted as timed out.
        """
        checkpoint = self._make_checkpoint(revoke_flush_timeout=-1.0)

        class _SlowOkSink(BatchingSink):
            def write(self, batch):
                pass

            def flush(self):
                # Finishes well after a bogus join(0) would return, but quickly
                # enough to keep the test fast once the sentinel means "infinite".
                time.sleep(0.3)

        result = checkpoint._flush_sink_bounded(
            _SlowOkSink(), checkpoint._revoke_flush_timeout
        )

        assert result is True

    def test_on_revoke_releases_state_partitions_even_if_commit_raises(self):
        """
        R4-3 (red): if the bounded revoke commit exhausts its budget and raises,
        _on_revoke must STILL run _revoke_state_partitions (release the RocksDB
        store lock) and reset_backpressure -- otherwise the lock leaks and the
        incoming owner livelocks. Round 4 had no try/finally, so the exception
        escaped before the release ran. Binds the real Application._on_revoke to
        a mock app.
        """
        app = MagicMock()
        app._failed = False
        app._processing_context.commit_checkpoint.side_effect = RuntimeError(
            "revoke budget exhausted"
        )
        tps = [TopicPartition("topic", 0)]

        with pytest.raises(RuntimeError):
            Application._on_revoke(app, None, tps)

        # Lock released and backpressure cleared despite the commit failure.
        app._revoke_state_partitions.assert_called_once_with(topic_partitions=tps)
        app._consumer.reset_backpressure.assert_called_once()


class TestSourceCheckpointBounding:
    """
    C4 (red): KafkaReplicatorSource's checkpoint runs inside the source's
    rebalance callback. Its EOS abort (close) and commit (_commit) must be
    bounded by the source flush_timeout so a coordinator outage cannot block the
    callback past max.poll.interval.ms. Round 4 turned the shared
    InternalProducer.abort_transaction into a 3-attempt retry loop, so an
    unbounded source abort became 3x unbounded.
    """

    def _make(self, flush_timeout=7):
        producer = MagicMock(spec_set=InternalProducer)
        producer.flush.return_value = 0
        topic = MagicMock()
        topic.name = "src-topic"
        return SourceCheckpoint(
            producer=producer,
            producer_topic=topic,
            consumer=MagicMock(spec_set=BaseConsumer),
            commit_interval=1,
            flush_timeout=flush_timeout,
            exactly_once=True,
        )

    def test_source_close_abort_is_bounded(self):
        cp = self._make(flush_timeout=7)

        cp.close()

        cp._producer.abort_transaction.assert_called_once()
        args, kwargs = cp._producer.abort_transaction.call_args
        timeout = kwargs.get("timeout", args[0] if args else None)
        assert timeout is not None, "source EOS abort must be bounded, not None"
        assert 0 < timeout <= 7

    def test_source_commit_transaction_is_bounded(self):
        cp = self._make(flush_timeout=7)
        cp.store_offset("t", 0, 5)

        cp.commit()

        cp._producer.commit_transaction.assert_called_once()
        args, kwargs = cp._producer.commit_transaction.call_args
        timeout = kwargs.get("timeout", args[2] if len(args) > 2 else None)
        assert timeout is not None, "source EOS commit_transaction must be bounded"
        assert 0 < timeout <= 7
