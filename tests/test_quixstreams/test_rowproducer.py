import uuid
from concurrent.futures import Future
from time import sleep
from unittest.mock import call, create_autospec, patch

import pytest
from confluent_kafka import (
    KafkaError as ConfluentKafkaError,
)
from confluent_kafka import (
    KafkaException as ConfluentKafkaException,
)
from confluent_kafka import TopicPartition

from quixstreams.kafka.exceptions import KafkaProducerDeliveryError
from quixstreams.kafka.producer import TransactionalProducer
from quixstreams.models import (
    JSONSerializer,
    SerializationError,
)
from quixstreams.rowproducer import RowProducer


class TestRowProducer:
    def test_produce_row_success(
        self,
        row_consumer_factory,
        row_producer_factory,
        topic_json_serdes_factory,
        row_factory,
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = {"field": "value"}
        headers = [("header1", b"1")]

        with row_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=value,
                key=key,
                headers=headers,
            )
            producer.produce_row(topic=topic, row=row)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
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
        row_consumer_factory,
        row_producer_factory,
        topic_json_serdes_factory,
        row_factory,
    ):
        topic = topic_json_serdes_factory()

        value = {"field": "value"}
        headers = [("header1", b"1")]

        with (
            row_consumer_factory(auto_offset_reset="earliest") as consumer,
            row_producer_factory() as producer,
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
        self, row_producer_factory, row_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        with row_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            with pytest.raises(SerializationError):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_produce_error_raise(
        self, row_producer_factory, row_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        with row_producer_factory(extra_config={"message.max.bytes": 1000}) as producer:
            row = row_factory(
                topic=topic.name,
                value={"field": 1001 * "a"},
            )
            with pytest.raises(ConfluentKafkaException):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_serialization_error_suppress(
        self,
        row_consumer_factory,
        row_producer_factory,
        row_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        suppressed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, SerializationError)
            suppressed.set_result(True)
            return True

        with row_producer_factory(on_error=on_error) as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            producer.produce_row(topic=topic, row=row)

    def test_produce_delivery_error_raised_on_produce(
        self, row_producer_factory, topic_json_serdes_factory
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = b"value"

        producer = row_producer_factory()

        # Send message to a non-existing partition to simulate error
        # in the delivery callback
        producer.produce(topic=topic.name, key=key, value=value, partition=3)
        # Poll for delivery callbacks
        producer.poll(5)
        # The next produce should fail after
        with pytest.raises(KafkaProducerDeliveryError):
            producer.produce(topic=topic.name, key=key, value=value)

    def test_produce_delivery_error_raised_on_flush(
        self, row_producer_factory, topic_json_serdes_factory
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = b"value"

        producer = row_producer_factory()

        # Send message to a non-existing partition to simulate error
        # in the delivery callback
        producer.produce(topic=topic.name, key=key, value=value, partition=3)
        # The flush should fail after that
        with pytest.raises(KafkaProducerDeliveryError):
            producer.flush()


class TestTransactionalRowProducer:
    def test_produce_and_commit(
        self,
        row_producer,
        transactional_row_producer,
        topic_manager_topic_factory,
        row_consumer_factory,
    ):
        """
        Simplest transactional consume + produce pattern
        """
        topic_args = dict(
            create_topic=True,
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
            row_producer.produce(
                topic=topic_in.name, key=msg_in.key, value=msg_in.value
            )
            row_producer.flush(2)

        # consume + produce those same messages to new downstream topic, commit them
        producer = transactional_row_producer
        with row_consumer_factory(
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
        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic_out])
            while (row := consumer.poll_row(2)) is not None:
                rows.append(row)
        assert len(rows) == message_count
        for row in rows:
            assert row.key == key
            assert row.value == value

    def test_produce_after_aborted_transaction(
        self,
        row_producer,
        transactional_row_producer,
        topic_manager_topic_factory,
        row_consumer_factory,
    ):
        """
        transactional consume + produce pattern, but we mimic a failed transaction by
        aborting it directly (after producing + flushing to the downstream topic).

        Then, redo the consume + produce (and successfully commit the transaction).

        We confirm offset behavior from both failed and successful transactions.
        """
        topic_args = dict(
            create_topic=True,
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
            row_producer.produce(
                topic=topic_in.name, key=msg_in.key, value=msg_in.value
            )
            row_producer.flush(2)

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
        producer = transactional_row_producer
        with row_consumer_factory(
            auto_offset_reset="earliest",
            auto_commit_enable=False,
            consumer_group=consumer_group,
        ) as consumer:
            _ = consume_and_produce(consumer, producer)
            producer.abort_transaction(2)
        assert not producer.offsets

        # repeat, only this time we commit the transaction
        with row_consumer_factory(
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
        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
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
        row_producer,
        row_producer_factory,
        topic_manager_topic_factory,
        row_consumer_factory,
    ):
        """
        Validate the behavior around a transaction that times out via the producer
        config transaction.timeout.ms

        A timeout should invalidate that producer from further transactions
        (which also raises an exception to cause the Application to terminate).
        """
        topic_args = dict(
            create_topic=True,
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
            row_producer.produce(
                topic=topic_in.name, key=msg_in.key, value=msg_in.value
            )
            row_producer.flush(2)

        # consume + produce those same messages to new downstream topic
        # however, we will wait to commit the transaction
        timeout_seconds = 1
        producer = row_producer_factory(
            transactional=True,
            extra_config={"transaction.timeout.ms": timeout_seconds * 1000},
        )
        with row_consumer_factory(
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
        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
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

        class MockKafkaError(Exception):
            def retriable(self):
                return True

        call_args = [["my", "offsets"], "consumer_metadata", 1]
        error = ConfluentKafkaException(MockKafkaError())

        mock_producer = create_autospec(TransactionalProducer)
        mock_producer.send_offsets_to_transaction.__name__ = "send_offsets"
        mock_producer.commit_transaction.__name__ = "commit"
        mock_producer.send_offsets_to_transaction.side_effect = [error, None]
        with patch(
            "quixstreams.rowproducer.TransactionalProducer", return_value=mock_producer
        ):
            row_producer = RowProducer(broker_address="lol", transactional=True)
            row_producer.commit_transaction(*call_args)

        mock_producer.send_offsets_to_transaction.assert_has_calls(
            [call(*call_args)] * 2
        )
        mock_producer.commit_transaction.assert_called_once()
