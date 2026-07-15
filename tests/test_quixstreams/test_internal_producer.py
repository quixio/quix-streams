import uuid
from concurrent.futures import Future
from time import sleep
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from confluent_kafka import (
    KafkaError as ConfluentKafkaError,
)
from confluent_kafka import (
    KafkaException as ConfluentKafkaException,
)
from confluent_kafka import TopicPartition

from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka import Producer
from quixstreams.kafka.exceptions import KafkaProducerDeliveryError
from quixstreams.models import (
    JSONSerializer,
    SerializationError,
)
from tests.utils import make_kafka_exception


class TestInternalProducer:
    def test_produce_row_success(
        self,
        internal_consumer_factory,
        internal_producer_factory,
        topic_json_serdes_factory,
        row_factory,
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = {"field": "value"}
        headers = [("header1", b"1")]

        with internal_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=value,
                key=key,
                headers=headers,
            )
            producer.produce_row(topic=topic, row=row)

        with internal_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            row = consumer.poll_row(timeout=5.0)

        assert producer.offsets
        assert producer.offsets.get((topic.name, 0)) is not None

        assert row
        assert row.key == key
        assert row.value == value
        assert row.headers == headers

    @pytest.mark.parametrize(
        "init_key, new_key, expected_key",
        [
            (b"key", b"new_key", b"new_key"),
            (b"key", b"", b""),
            (b"key", None, None),
        ],
    )
    def test_produce_row_custom_key(
        self,
        init_key,
        new_key,
        expected_key,
        internal_consumer_factory,
        internal_producer_factory,
        topic_json_serdes_factory,
        row_factory,
    ):
        topic = topic_json_serdes_factory()

        value = {"field": "value"}
        headers = [("header1", b"1")]

        with (
            internal_consumer_factory(auto_offset_reset="earliest") as consumer,
            internal_producer_factory() as producer,
        ):
            row = row_factory(
                topic=topic.name,
                value=value,
                key=init_key,
                headers=headers,
            )
            producer.produce_row(topic=topic, row=row, key=new_key)
            consumer.subscribe([topic])
            row = consumer.poll_row(timeout=5.0)

        assert row
        assert row.key == expected_key
        assert row.value == value
        assert row.headers == headers

    def test_produce_row_serialization_error_raise(
        self, internal_producer_factory, row_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        with internal_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            with pytest.raises(SerializationError):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_produce_error_raise(
        self, internal_producer_factory, row_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        with internal_producer_factory(
            extra_config={"message.max.bytes": 1000}
        ) as producer:
            row = row_factory(
                topic=topic.name,
                value={"field": 1001 * "a"},
            )
            with pytest.raises(ConfluentKafkaException):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_serialization_error_suppress(
        self,
        internal_consumer_factory,
        internal_producer_factory,
        row_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        suppressed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, SerializationError)
            suppressed.set_result(True)
            return True

        with internal_producer_factory(on_error=on_error) as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            producer.produce_row(topic=topic, row=row)

    def test_produce_delivery_error_raised_on_produce(
        self, internal_producer_factory, topic_json_serdes_factory
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = b"value"

        producer = internal_producer_factory()

        # Send message to a non-existing partition to simulate error
        # in the delivery callback
        producer.produce(topic=topic.name, key=key, value=value, partition=3)
        # Poll for delivery callbacks
        producer.poll(5)
        # The next produce should fail after
        with pytest.raises(KafkaProducerDeliveryError):
            producer.produce(topic=topic.name, key=key, value=value)

    def test_produce_delivery_error_raised_on_flush(
        self, internal_producer_factory, topic_json_serdes_factory
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = b"value"

        producer = internal_producer_factory()

        # Send message to a non-existing partition to simulate error
        # in the delivery callback
        producer.produce(topic=topic.name, key=key, value=value, partition=3)
        # The flush should fail after that
        with pytest.raises(KafkaProducerDeliveryError):
            producer.flush()


class TestTransactionalInternalProducer:
    def test_produce_and_commit(
        self,
        internal_producer,
        transactional_internal_producer,
        topic_manager_topic_factory,
        internal_consumer_factory,
    ):
        """
        Simplest transactional consume + produce pattern
        """
        topic_args = dict(
            value_serializer="json",
            value_deserializer="json",
            partitions=1,  # this test assumes 1 partition topics
        )
        topic_in = topic_manager_topic_factory(**topic_args)
        topic_out = topic_manager_topic_factory(**topic_args)
        key = b"my_key"
        value = {"my": "value"}
        message_count = 3

        # produce our initial messages to consume (no EOS here)
        msg_in = topic_in.serialize(key=key, value=value)
        for _ in range(message_count):
            internal_producer.produce(
                topic=topic_in.name, key=msg_in.key, value=msg_in.value
            )
            internal_producer.flush(2)

        # consume + produce those same messages to new downstream topic, commit them
        producer = transactional_internal_producer
        with internal_consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer:
            consumer.subscribe([topic_in])
            producer.begin_transaction()
            while (msg := consumer.poll_row(2)) is not None:
                consumer_end_offset = msg.offset + 1
                msg_out = topic_in.serialize(key=msg.key, value=msg.value)
                producer.produce(
                    topic=topic_out.name, key=msg_out.key, value=msg_out.value
                )
            producer.commit_transaction(
                [TopicPartition(topic_in.name, 0, consumer_end_offset)],
                consumer.consumer_group_metadata(),
                3,
            )
            assert (
                consumer.committed([TopicPartition(topic_in.name, 0)])[0].offset
                == consumer_end_offset
            )

        # downstream consumer gets the committed messages
        rows = []
        with internal_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic_out])
            while (row := consumer.poll_row(2)) is not None:
                rows.append(row)
        assert len(rows) == message_count
        for row in rows:
            assert row.key == key
            assert row.value == value

    def test_produce_after_aborted_transaction(
        self,
        internal_producer,
        transactional_internal_producer,
        topic_manager_topic_factory,
        internal_consumer_factory,
    ):
        """
        transactional consume + produce pattern, but we mimic a failed transaction by
        aborting it directly (after producing + flushing to the downstream topic).

        Then, redo the consume + produce (and successfully commit the transaction).

        We confirm offset behavior from both failed and successful transactions.
        """
        topic_args = dict(
            value_serializer="json",
            value_deserializer="json",
            partitions=1,  # this test assumes 1 partition topics
        )
        topic_in = topic_manager_topic_factory(**topic_args)
        topic_out = topic_manager_topic_factory(**topic_args)
        key = b"my_key"
        value = {"my": "value"}
        message_count = 3
        consumer_group = str(uuid.uuid4())

        # produce our initial messages to consume (no EOS here)
        msg_in = topic_in.serialize(key=key, value=value)
        for _ in range(message_count):
            internal_producer.produce(
                topic=topic_in.name, key=msg_in.key, value=msg_in.value
            )
            internal_producer.flush(2)

        def consume_and_produce(consumer, producer):
            consumer_end_offset = None
            consumer.subscribe([topic_in])
            producer.begin_transaction()
            while (msg := consumer.poll_row(2)) is not None:
                consumer_end_offset = msg.offset + 1
                msg_out = topic_in.serialize(key=msg.key, value=msg.value)
                producer.produce(
                    topic=topic_out.name, key=msg_out.key, value=msg_out.value
                )
            producer.flush()
            return consumer_end_offset

        # consume + produce those same messages to new downstream topic
        # will abort the transaction instead of committing
        producer = transactional_internal_producer
        with internal_consumer_factory(
            auto_offset_reset="earliest",
            auto_commit_enable=False,
            consumer_group=consumer_group,
        ) as consumer:
            _ = consume_and_produce(consumer, producer)
            producer.abort_transaction(2)
        assert not producer.offsets

        # repeat, only this time we commit the transaction
        with internal_consumer_factory(
            auto_offset_reset="earliest",
            auto_commit_enable=False,
            consumer_group=consumer_group,
        ) as consumer:
            consumer_end_offset = consume_and_produce(consumer, producer)
            producer.commit_transaction(
                [TopicPartition(topic_in.name, 0, consumer_end_offset)],
                consumer.consumer_group_metadata(),
                3,
            )
            assert (
                consumer.committed([TopicPartition(topic_in.name, 0)])[0].offset
                == consumer_end_offset
            )

        # downstream consumer should only get the committed messages
        rows = []
        with internal_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic_out])
            while (row := consumer.poll_row(2)) is not None:
                rows.append(row)
            lowwater, highwater = consumer.get_watermark_offsets(
                TopicPartition(topic_out.name, 0), 3
            )
        assert len(rows) == message_count
        for row in rows:
            assert row.key == key
            assert row.value == value

        # as further proof the initial messages actually made it to the topic
        # (and thus were ignored) we can inspect our message offsets.

        # Produced offsets 0-2 were aborted; all direct aborts are followed
        # by an abort marker (offset 3).
        # The next valid offset (which is our first successful message) should be 4.
        # Note that the lowwater is still 0, meaning the messages were successfully added
        # to the topic (and could actually be read if isolation.level was "uncommitted"),
        # but the transaction abort means our consumer ignores them.
        assert lowwater == 0
        assert rows[0].offset == 4

        # Our successfully produced offsets were 4-6, with a commit marker at 7.
        # This means our highwater should be last successful offset (6) + 2;
        # +1 for normal offset behavior, and +1 to account for the transaction marker
        assert highwater == rows[-1].offset + 2 == 8

    def test_produce_transaction_timeout_no_abort(
        self,
        internal_producer,
        internal_producer_factory,
        topic_manager_topic_factory,
        internal_consumer_factory,
    ):
        """
        Validate the behavior around a transaction that times out via the producer
        config transaction.timeout.ms

        A timeout should invalidate that producer from further transactions
        (which also raises an exception to cause the Application to terminate).
        """
        topic_args = dict(
            value_serializer="json",
            value_deserializer="json",
            partitions=1,  # this test assumes 1 partition topics
        )
        topic_in = topic_manager_topic_factory(**topic_args)
        topic_out = topic_manager_topic_factory(**topic_args)
        key = b"my_key"
        value = {"my": "value"}
        message_count = 3

        # produce our initial messages to consume (no EOS here)
        msg_in = topic_in.serialize(key=key, value=value)
        for _ in range(message_count):
            internal_producer.produce(
                topic=topic_in.name, key=msg_in.key, value=msg_in.value
            )
            internal_producer.flush(2)

        # consume + produce those same messages to new downstream topic
        # however, we will wait to commit the transaction
        timeout_seconds = 1
        producer = internal_producer_factory(
            transactional=True,
            extra_config={"transaction.timeout.ms": timeout_seconds * 1000},
        )
        with internal_consumer_factory(
            auto_offset_reset="earliest",
            auto_commit_enable=False,
        ) as consumer:
            consumer.subscribe([topic_in])
            producer.begin_transaction()
            while (msg := consumer.poll_row(2)) is not None:
                consumer_end_offset = msg.offset + 1
                msg_out = topic_in.serialize(key=msg.key, value=msg.value)
                producer.produce(
                    topic=topic_out.name, key=msg_out.key, value=msg_out.value
                )
            producer.flush(2)

            # try to commit after sleeping past the transaction timeout window
            sleep(15)
            # there seems to be a significant grace period on transaction timeouts
            # despite changing related settings (socket.timeout, message.timeout)
            # which is why sleep is long; might be worth investigating later
            with pytest.raises(ConfluentKafkaException) as e:
                producer.commit_transaction(
                    [TopicPartition(topic_in.name, 0, consumer_end_offset)],
                    consumer.consumer_group_metadata(),
                    2,
                )
            kafka_error = e.value.args[0]
            assert kafka_error.code() == ConfluentKafkaError._FENCED
            assert kafka_error.fatal() is True
            assert (
                consumer.committed([TopicPartition(topic_in.name, 0)])[0].offset
                == -1001
            )

        # downstream consumer should ignore failed transaction messages
        with internal_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic_out])
            assert consumer.poll_row(2) is None
            lowwater, highwater = consumer.get_watermark_offsets(
                TopicPartition(topic_out.name, 0), 3
            )
        assert lowwater == 0
        # timing out does not seem to generate a transaction marker, so just +1
        assert highwater == message_count + 1

        # attempting a new transaction with the same producer raises same error
        with pytest.raises(ConfluentKafkaException) as e:
            producer.begin_transaction()
        kafka_error = e.value.args[0]
        assert kafka_error.code() == ConfluentKafkaError._FENCED

    def test_retriable_op_error(self):
        """
        Some specific failure cases from sending offsets or committing a transaction
        are retriable.
        """
        call_args = [["my", "offsets"], "consumer_metadata", 1]

        mock_producer = create_autospec(Producer)
        mock_producer.send_offsets_to_transaction.side_effect = [
            make_kafka_exception(retriable=True),
            None,
        ]
        with (
            patch(
                "quixstreams.internal_producer.Producer",
                return_value=mock_producer,
            ),
            patch("quixstreams.internal_producer.sleep"),
        ):
            producer = InternalProducer(broker_address="xyz", transactional=True)
            producer.commit_transaction(*call_args)

        # Retried send-offsets twice (1 retriable failure + 1 success). The
        # per-attempt timeout is now derived from the shared commit budget, so
        # only the positions/metadata are asserted, not the exact timeout value.
        assert mock_producer.send_offsets_to_transaction.call_count == 2
        for c in mock_producer.send_offsets_to_transaction.call_args_list:
            assert c.args[0] == ["my", "offsets"]
            assert c.args[1] == "consumer_metadata"
        mock_producer.commit_transaction.assert_called_once()


class TestAbortTransactionRetry:
    """
    InternalProducer.abort_transaction must retry a *retriable* error
    (e.g. _TIMED_OUT during a transient coordinator outage) per confluent-kafka's
    abort-transaction contract ("the application may call abort_transaction()
    again to continue the abort operation"), instead of propagating on the first
    failure. Non-retriable / fenced / fatal errors still propagate unchanged.
    """

    def test_abort_transaction_retries_retriable_error_and_succeeds(self):
        """
        A retriable KafkaError from the inner abort_transaction is retried and
        succeeds on a later attempt instead of propagating.
        """
        mock_producer = create_autospec(Producer)
        # Fail once with a retriable error, then succeed on the retry.
        mock_producer.abort_transaction.side_effect = [
            make_kafka_exception(retriable=True),
            None,
        ]
        with (
            patch("quixstreams.internal_producer.Producer", return_value=mock_producer),
            patch("quixstreams.internal_producer.sleep"),
        ):
            producer = InternalProducer(broker_address="xyz", transactional=True)
            producer._active_transaction = True
            # Must retry and succeed instead of raising.
            producer.abort_transaction(timeout=5.0)

        assert mock_producer.abort_transaction.call_count == 2
        assert producer._active_transaction is False

    def test_abort_transaction_propagates_non_retriable_error(self):
        """
        A non-retriable / fatal error is not retried: it propagates unchanged and
        leaves the transaction open (its truthful librdkafka code must surface).
        """
        mock_producer = create_autospec(Producer)
        mock_producer.abort_transaction.side_effect = make_kafka_exception(
            retriable=False
        )
        with (
            patch("quixstreams.internal_producer.Producer", return_value=mock_producer),
            patch("quixstreams.internal_producer.sleep"),
        ):
            producer = InternalProducer(broker_address="xyz", transactional=True)
            producer._active_transaction = True
            with pytest.raises(ConfluentKafkaException):
                producer.abort_transaction(timeout=5.0)

        assert mock_producer.abort_transaction.call_count == 1
        assert producer._active_transaction is True

    def test_abort_transaction_none_timeout_retries_indefinitely(self):
        """
        R4-2 (red): with no timeout (None = "wait out a transient coordinator
        outage"), a retriable abort error must be retried indefinitely (no
        attempt cap) until it succeeds or a non-retriable error occurs. Round 4
        still decremented the attempt counter on the None path, so it re-raised
        after 3 retriable failures -- contradicting its own docstring.
        """
        mock_producer = create_autospec(Producer)
        # Fail 5 times (well past the 3-attempt cap), then succeed.
        mock_producer.abort_transaction.side_effect = [
            make_kafka_exception(retriable=True) for _ in range(5)
        ] + [None]
        with (
            patch("quixstreams.internal_producer.Producer", return_value=mock_producer),
            patch("quixstreams.internal_producer.sleep"),
        ):
            producer = InternalProducer(broker_address="xyz", transactional=True)
            producer._active_transaction = True
            # No timeout => unbounded: must keep retrying and finally succeed.
            producer.abort_transaction(timeout=None)

        assert mock_producer.abort_transaction.call_count == 6
        assert producer._active_transaction is False

    def test_producer_abort_transaction_maps_none_timeout_to_infinite(self):
        """
        R4 latent guard: the Producer wrapper must map a None abort timeout to
        librdkafka's -1 (infinite), NOT pass None through. Raw confluent
        abort_transaction(None) raises TypeError, which would bypass
        InternalProducer's retriable `except KafkaException` and silently break
        the unbounded (idle/close) abort contract if this mapping regressed.
        Every other abort test mocks the Producer wrapper, so this exercises the
        real None->-1 mapping directly.
        """
        producer = Producer(broker_address="xyz", transactional=True)
        inner = MagicMock()
        producer._inner_producer = inner

        producer.abort_transaction(None)

        inner.abort_transaction.assert_called_once_with(-1)


class TestInternalProducerAbortPurge:
    """
    Contract: InternalProducer.abort_transaction must not leave purge-induced
    delivery errors in _error. librdkafka's abort purges undelivered messages
    and fires their delivery callbacks with _PURGE_QUEUE/_PURGE_INFLIGHT
    errors; the next produce()/flush() must not see those as real failures.
    Validates review finding A (EOS _error poisoning on abort).
    """

    def test_abort_transaction_swallows_purge_induced_delivery_errors(self):
        """
        Contract: after abort_transaction, _error must be None even when
        librdkafka fires _PURGE_QUEUE delivery callbacks during the abort.
        A subsequent _raise_for_error() must be a no-op.
        """
        # Build a real InternalProducer with the underlying Producer mocked out
        with patch("quixstreams.internal_producer.Producer"):
            producer = InternalProducer(broker_address="localhost:9092")

        # Replace the inner wrapper with a mock we control
        mock_inner = MagicMock()
        producer._producer = mock_inner
        producer._active_transaction = True

        # Simulate: librdkafka abort purges queued messages and fires delivery
        # callbacks with _PURGE_QUEUE errors
        purge_error = ConfluentKafkaError(ConfluentKafkaError._PURGE_QUEUE)
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test-topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42

        def abort_side_effect(timeout=None):
            # Simulate librdkafka firing delivery callbacks during abort
            producer._on_delivery(purge_error, mock_msg)

        mock_inner.abort_transaction.side_effect = abort_side_effect

        # Call abort_transaction
        producer.abort_transaction()

        # Contract: purge-induced errors must NOT remain in _error
        assert (
            producer._error is None
        ), f"abort_transaction left purge-induced error in _error: {producer._error}"
        # Equivalently: _raise_for_error must be a no-op
        producer._raise_for_error()  # must not raise

    def test_genuine_delivery_error_during_abort_still_surfaces(self):
        """
        R4-4 (red): a genuine (non-_PURGE) delivery error firing during the
        abort/poll window must NOT be discarded -- it must surface on the next
        flush()/produce(). The old blanket snapshot/restore of _error around the
        abort wiped it; filtering only the _PURGE codes in _on_delivery keeps it.
        """
        with patch("quixstreams.internal_producer.Producer"):
            producer = InternalProducer(broker_address="localhost:9092")

        mock_inner = MagicMock()
        producer._producer = mock_inner
        producer._active_transaction = True

        # A genuine, non-purge delivery failure fires while librdkafka aborts.
        genuine_error = ConfluentKafkaError(ConfluentKafkaError._MSG_TIMED_OUT)
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test-topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42

        def abort_side_effect(timeout=None):
            producer._on_delivery(genuine_error, mock_msg)

        mock_inner.abort_transaction.side_effect = abort_side_effect

        producer.abort_transaction()

        # The genuine error survived the abort and surfaces on the next check.
        with pytest.raises(KafkaProducerDeliveryError):
            producer._raise_for_error()
