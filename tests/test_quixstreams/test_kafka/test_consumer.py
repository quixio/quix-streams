import contextlib
import time
import uuid
from typing import List

import confluent_kafka
import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.error import KafkaError
from tests.utils import Timeout, DEFAULT_TIMEOUT


def test_consumer_start_close(
    consumer,
):
    with consumer:
        metadata = consumer.list_topics()
        assert metadata
        broker_meta = metadata.brokers[0]
        broker_address = f"{broker_meta.host}:{broker_meta.port}"
        assert broker_address


def test_consumer_close_inner_consumer(
    consumer,
):
    with consumer:
        metadata = consumer.list_topics()
        assert metadata
        broker_meta = metadata.brokers[0]
        broker_address = f"{broker_meta.host}:{broker_meta.port}"
        assert broker_address
        consumer.close()
        with pytest.raises(RuntimeError) as raised:
            consumer.poll(timeout=0)
    assert str(raised.value) == "Consumer closed"


class TestConsumerSubscribe:
    def test_consumer_subscribe_topic_exists(
        self,
        consumer,
        topic_factory,
    ):
        topic_name, num_partitions = topic_factory()

        with consumer:
            consumer.subscribe(topics=[topic_name])
            msg = consumer.poll(timeout=1)
            assert msg is None
            metadata = consumer.list_topics()
            assert topic_name in metadata.topics
            assert len(metadata.topics[topic_name].partitions) == num_partitions

    def test_consumer_subscribe_topic_doesnt_exist(self, consumer):
        topic_name = str(uuid.uuid4())

        with consumer:
            consumer.subscribe(topics=[topic_name])
            msg = consumer.poll(timeout=1)
            assert msg
            err = msg.error()
            assert err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART

    def test_consumer_subscribe_multiple_topics(
        self,
        consumer,
        topic_factory,
    ):
        topic1, _ = topic_factory()
        topic2, _ = topic_factory()

        assigned_partitions = []

        def on_assign(_, partitions: List[TopicPartition]):
            nonlocal assigned_partitions
            assigned_partitions = partitions

        with consumer:
            consumer.subscribe(topics=[topic1, topic2], on_assign=on_assign)
            consumer.poll(timeout=5)

        assert len(assigned_partitions) == 2
        for partition in assigned_partitions:
            assert partition.topic in (topic1, topic2)

    def test_consumer_subscribe_multiple_times(
        self,
        consumer,
        topic_factory,
    ):
        topic1, _ = topic_factory()
        topic2, _ = topic_factory()

        with consumer:
            consumer.subscribe(topics=[topic1])
            consumer.poll(timeout=5)
            assert len(consumer.assignment()) == 1

            consumer.subscribe(topics=[topic1, topic2])
            consumer.poll(timeout=5)
            assert len(consumer.assignment()) == 2


class TestConsumerOnAssign:
    def test_consumer_on_assign_callback_single_consumer(self, consumer, topic_factory):
        topic_name, num_partitions = topic_factory()

        num_partitions_assigned = 0

        def on_assign(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            nonlocal num_partitions_assigned
            num_partitions_assigned = len(partitions)

        with consumer:
            consumer.subscribe(topics=[topic_name], on_assign=on_assign)

            while Timeout():
                msg = consumer.poll(timeout=0.1)
                assert msg is None
                if num_partitions == num_partitions_assigned:
                    return

    def test_consumer_on_assign_callback_new_partition_added(
        self, consumer, topic_factory, set_topic_partitions
    ):
        topic_name, num_partitions = topic_factory()
        partitions_increased = False

        with consumer:
            consumer.subscribe(topics=[topic_name])

            while Timeout():
                msg = consumer.poll(timeout=0.1)
                assert msg is None
                # Wait for the initial partition assignment
                if not partitions_increased and len(consumer.assignment()) == 1:
                    # Increase topic partitions count to 10
                    set_topic_partitions(topic=topic_name, num_partitions=10)
                    partitions_increased = True
                # Validate that new partitions are assigned to a consumer
                if len(consumer.assignment()) == 10:
                    return

    def test_cooperative_pause_during_on_assign_callback_remains_paused(
        self, consumer_factory, topic_factory, producer
    ):
        """
        Confirm that consumer partitions that are manually assigned, and then paused,
        during the on_assign callback remain paused (they must be assigned first!).

        Also, confirm incremental assigns of other partitions outside the normal
        rebalance scope do not affect this pause status.
        """

        def on_assign(consumer, tp_list):
            consumer.incremental_assign(tp_list)
            if len(tp_list) == 1:
                consumer.pause(tp_list)

        topic, _ = topic_factory(num_partitions=2)
        topic_empty, _ = topic_factory(num_partitions=2)
        stack = contextlib.ExitStack()

        consumer_args = dict(
            auto_offset_reset="earliest",
            extra_config={
                "partition.assignment.strategy": "cooperative-sticky",
                "max.poll.interval.ms": 60000,
            },
        )
        consumer_0 = consumer_factory(**consumer_args)
        stack.enter_context(consumer_0)
        consumer_0.subscribe(topics=[topic])
        while len(consumer_0.assignment()) != 2:
            consumer_0.poll(0.1)

        consumer_1 = consumer_factory(**consumer_args)
        stack.enter_context(consumer_1)
        consumer_1.subscribe(
            topics=[topic],
            on_assign=on_assign,
        )

        while len(consumer_0.assignment()) != len(consumer_1.assignment()):
            consumer_1.poll(0.1)
            consumer_0.poll(0.1)

        # assigning a partition outside normal scope should not unpause others
        consumer_1.incremental_assign([TopicPartition(topic_empty, 0)])
        while len(consumer_1.assignment()) < 2:
            consumer_1.poll(0.1)

        stack.enter_context(producer)
        producer.produce(key="key0", value="value", topic=topic, partition=0)
        producer.produce(key="key1", value="value", topic=topic, partition=1)
        producer.flush()

        try:
            assert consumer_0.poll(3).offset() == 0
            assert consumer_1.poll(3) is None
        finally:
            stack.close()


class TestConsumerOnRevoke:
    def test_consumer_on_revoke_callback_new_consumer_joined(
        self, consumer_factory, topic_factory
    ):
        """
        Validate that Consumer handles on_revoke callback on new consumer
        joining the consumer group
        """
        topic_name, num_partitions = topic_factory(num_partitions=2)

        num_partitions_assigned = 0
        num_partitions_revoked = 0

        def on_assign(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            nonlocal num_partitions_assigned
            num_partitions_assigned = len(partitions)

        def on_revoke(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            nonlocal num_partitions_revoked
            num_partitions_revoked = len(partitions)

        exit_stack = contextlib.ExitStack()

        # Start first consumer
        consumer1 = consumer_factory()
        exit_stack.enter_context(consumer1)
        with exit_stack:
            consumer1.subscribe(
                topics=[topic_name], on_assign=on_assign, on_revoke=on_revoke
            )

            while Timeout():
                msg = consumer1.poll(timeout=0.1)
                assert msg is None

                # Wait until consumer has partitions assigned
                if num_partitions_assigned > 0:
                    # Start second consumer
                    consumer2 = consumer_factory()
                    exit_stack.enter_context(consumer2)
                    consumer2.subscribe(topics=[topic_name])
                    num_partitions_assigned = 0

                # Make sure some partitions are revoked
                if num_partitions_revoked > 0:
                    return


class TestConsumerPoll:
    def test_poll_single_message_success(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = topic_factory()

        messages = [
            {
                "key": "test",
                "value": b"test0",
                "headers": {"header": b"header"},
            },
            {
                "key": "test",
                "value": b"test2",
                "headers": {"header": b"header"},
            },
        ]

        with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            consumer.subscribe(topics=[topic])
            for msg in messages:
                producer.produce(
                    topic=topic,
                    key=msg["key"],
                    value=msg["value"],
                    headers=msg["headers"],
                )

            for msg_expected in messages:
                msg = consumer.poll(timeout=DEFAULT_TIMEOUT)

                assert not msg.error()
                assert msg.key() == msg_expected["key"].encode()
                assert msg.value() == msg_expected["value"]
                assert msg.headers() == list(msg_expected["headers"].items())

    def test_poll_multiple_partitions_success(
        self, consumer_factory, producer, topic_factory
    ):
        topic, num_partitions = topic_factory(num_partitions=5)

        messages = {
            "key1": {
                "value": b"test1",
                "headers": {"header": b"header"},
            },
            "key2": {
                "key": "test",
                "value": b"test2",
                "headers": {"header": b"header"},
            },
        }

        with consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe(topics=[topic])
            with producer:
                for key, msg in messages.items():
                    producer.produce(
                        topic=topic,
                        key=key,
                        value=msg["value"],
                        headers=msg["headers"],
                    )

            for i in range(len(messages)):
                msg = consumer.poll(timeout=DEFAULT_TIMEOUT)

                assert not msg.error()
                key = msg.key().decode()
                assert key in messages
                assert msg.value() == messages[key]["value"]
                assert msg.headers() == list(messages[key]["headers"].items())


class TestConsumerStoreOffsets:
    def test_store_offsets_committed(
        self, consumer_factory, producer_factory, topic_factory
    ):
        """
        Test that store_offsets() submits messages for commit,
        and Consumer commits the offsets on closing
        """
        topic, _ = topic_factory()
        committed_msg = {
            "key": "key1",
            "value": b"test1",
        }
        new_msg = {
            "key": "key2",
            "value": b"test1",
        }

        with consumer_factory(
            auto_offset_reset="earliest"
        ) as consumer, producer_factory() as producer:
            consumer.subscribe(topics=[topic])
            # Produce a message
            producer.produce(
                topic=topic,
                key=committed_msg["key"],
                value=committed_msg["value"],
            )
            producer.flush()

            # Receive a message
            msg = consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Store offsets to be committed later by autocommit and close the consumer
            consumer.store_offsets(message=msg)

        # Start new consumer and producer
        with consumer_factory(
            auto_offset_reset="latest"
        ) as consumer, producer_factory() as producer:
            consumer.subscribe(topics=[topic])
            # Produce a new message
            producer.produce(
                topic=topic,
                key=new_msg["key"],
                value=new_msg["value"],
            )
            producer.flush()
            # Consume a message
            msg = consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Ensure that only new uncommitted message is received
            assert msg.key() == new_msg["key"].encode()

    def test_store_offsets_none_passed(self, consumer):
        with consumer:
            with pytest.raises(ValueError) as raised:
                consumer.store_offsets()
            assert str(raised.value) == 'One of "message" or "offsets" must be passed'

    def test_store_offsets_offsets_and_message_passed(self, consumer):
        with consumer:
            with pytest.raises(ValueError) as raised:
                consumer.store_offsets(offsets=[], message=123)
            assert (
                str(raised.value)
                == 'Parameters "message" and "offsets" are mutually exclusive'
            )

    def test_store_offsets_autocommit_disabled(
        self, consumer_factory, producer_factory, topic_factory
    ):
        topic, _ = topic_factory()
        msg = {
            "key": "key1",
            "value": b"test1",
        }

        with consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer, producer_factory() as producer:
            consumer.subscribe(topics=[topic])
            # Produce a message
            producer.produce(
                topic=topic,
                key=msg["key"],
                value=msg["value"],
            )
            producer.flush()

            # Receive a message
            msg = consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Store offsets to be committed later by autocommit and close the consumer
            consumer.store_offsets(message=msg)

        # Start new consumer with offset reset "latest" and producer
        with consumer_factory(auto_offset_reset="latest") as consumer:
            consumer.subscribe(topics=[topic])
            # Try to consume a message
            msg = consumer.poll(timeout=DEFAULT_TIMEOUT)
            # Ensure that message is empty.
            # Previous consumer hasn't committed anything,
            # and offset reset "latest" doesn't see any messages.
            assert msg is None


class TestConsumerCommit:
    def test_commit_offsets_and_message_passed(self, consumer):
        with consumer:
            with pytest.raises(ValueError) as raised:
                consumer.commit(offsets=[], message=123)
            assert (
                str(raised.value)
                == 'Parameters "message" and "offsets" are mutually exclusive'
            )

    def test_commit_committed_sync(
        self, consumer_factory, producer_factory, topic_factory
    ):
        topic, _ = topic_factory()
        committed_msg = {
            "key": "key1",
            "value": b"test1",
        }

        with consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer, producer_factory() as producer:
            consumer.subscribe(topics=[topic])
            # Produce a message
            producer.produce(
                topic=topic,
                key=committed_msg["key"],
                value=committed_msg["value"],
            )
            producer.flush()

            # Receive a message
            msg = consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Commit a message synchronously
            committed = consumer.commit(message=msg, asynchronous=False)
            assert committed
            assert committed[0].offset == 1

        with consumer_factory(auto_offset_reset="latest") as consumer:
            committed_partitions = consumer.committed(
                partitions=[confluent_kafka.TopicPartition(topic, partition=0)],
            )
            assert committed_partitions[0].offset == 1

    def test_commit_committed_async(
        self, consumer_factory, producer_factory, topic_factory
    ):
        topic, _ = topic_factory()
        committed_msg = {
            "key": "key1",
            "value": b"test1",
        }

        with consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer, producer_factory() as producer:
            consumer.subscribe(topics=[topic])
            # Produce a message
            producer.produce(
                topic=topic,
                key=committed_msg["key"],
                value=committed_msg["value"],
            )
            producer.flush()

            # Receive a message
            msg = consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Commit a message asynchronously
            committed = consumer.commit(message=msg, asynchronous=True)
            assert committed is None

        # Start new consumer
        with consumer_factory(auto_offset_reset="latest") as consumer:
            committed_partitions = consumer.committed(
                partitions=[confluent_kafka.TopicPartition(topic, partition=0)],
            )
            assert committed_partitions[0].offset == 1


class TestConsumerAssignment:
    def test_assignment_empty(self, consumer):
        with consumer:
            topics = consumer.assignment()
            assert not topics

    def test_assignment_assigned(self, consumer, topic_factory):
        topic, _ = topic_factory()

        assigned = False

        def on_assign(*args):
            nonlocal assigned
            assigned = True

        with consumer:
            consumer.subscribe([topic], on_assign=on_assign)

            while Timeout():
                msg = consumer.poll(timeout=0.1)
                assert msg is None

                if assigned:
                    topics = consumer.assignment()
                    assert topics
                    assert topics[0].topic == topic
                    return


class TestConsumerUnsubscribe:
    def test_subscribe_assign_unsubscribe(self, topic_factory, consumer):
        topic, _ = topic_factory()

        assigned = False
        revoked = False

        def on_assign(*args):
            nonlocal assigned
            assigned = True

        def on_revoke(*args):
            nonlocal revoked
            revoked = True

        with consumer:
            consumer.subscribe(
                [topic],
                on_assign=on_assign,
                on_revoke=on_revoke,
            )

            while Timeout():
                msg = consumer.poll(timeout=0.1)
                assert msg is None

                if assigned:
                    consumer.unsubscribe()
                    assigned = False
                if revoked:
                    return

    def test_unsubscribe_not_subscribed(self, consumer):
        with consumer:
            consumer.unsubscribe()


class TestConsumerListTopics:
    def test_list_topics_all_topics(self, consumer, topic_factory):
        topic1, _ = topic_factory()
        topic2, _ = topic_factory()

        with consumer:
            metadata = consumer.list_topics()
        for topic in (topic1, topic2):
            assert topic in metadata.topics
            assert not metadata.topics[topic].error

    def test_list_topics_one_topic(self, consumer, topic_factory):
        topic1, _ = topic_factory()
        topic2, _ = topic_factory()
        with consumer:
            metadata = consumer.list_topics(topic=topic2)
        assert topic2 in metadata.topics
        assert len(metadata.topics) == 1
        assert not metadata.topics[topic2].error

    def test_list_topics_one_topic_doesnt_exist(self, consumer):
        with consumer:
            metadata = consumer.list_topics(topic="another_topic")
        assert "another_topic" in metadata.topics
        assert (
            metadata.topics["another_topic"].error == KafkaError.UNKNOWN_TOPIC_OR_PART
        )


class TestConsumerPauseResume:
    def test_pause_resume(self, consumer_factory, topic_factory, producer):
        """
        Test that consumer doesn't receive messages for paused partitions and resumes
        consuming after `resume()`
        """
        topic, _ = topic_factory(num_partitions=2)

        with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            consumer.subscribe(topics=[topic])
            # Send a message
            producer.produce(key="key", value="value", topic=topic, partition=1)
            producer.flush()

            # Consume a message
            msg = consumer.poll(5.0)
            assert msg
            assert msg.key() == b"key"
            # Pause consuming for partition 1
            consumer.pause(partitions=[TopicPartition(topic=topic, partition=1)])
            # Produce messages for partitions 0 and 1
            producer.produce(key="key", value="value", topic=topic, partition=0)
            producer.produce(key="key", value="value", topic=topic, partition=1)
            producer.flush()
            # Poll and ensure that we receive messages only from partition 0
            msg = consumer.poll(timeout=5.0)
            assert msg.partition() == 0
            # Resume consuming from partition 1
            consumer.resume(partitions=[TopicPartition(topic=topic, partition=1)])
            # Receive a message from partition 1
            msg = consumer.poll(5.0)
            assert msg
            assert msg.key() == b"key"


class TestConsumerGetWatermarkOffsets:
    def test_get_watermark_offsets_empty_topic(self, topic_factory, consumer):
        topic, _ = topic_factory()
        with consumer:
            consumer.subscribe(topics=[topic])

            msg = consumer.poll(timeout=5.0)
            assert msg is None

            low, high = consumer.get_watermark_offsets(
                partition=TopicPartition(topic=topic, partition=0)
            )
            assert low == 0
            assert high == 0

    def test_get_watermark_offsets_with_committed(
        self, topic_factory, consumer, producer
    ):
        topic, _ = topic_factory()
        with consumer, producer:
            consumer.subscribe(topics=[topic])

            consumer.poll(timeout=5.0)
            producer.produce(topic=topic, key="key", value="value")
            msg = consumer.poll(timeout=5.0)
            consumer.commit(message=msg)

            low, high = consumer.get_watermark_offsets(
                partition=TopicPartition(topic=topic, partition=0)
            )
            assert low == 0
            assert high == 1


class TestConsumerPositionSeek:
    def test_position_empty_topic(self, consumer, topic_factory):
        topic, _ = topic_factory(num_partitions=2)

        with consumer:
            consumer.subscribe(topics=[topic])
            consumer.poll(timeout=5.0)

            partitions = consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                    TopicPartition(topic=topic, partition=1),
                ]
            )
        for partition in partitions:
            assert partition.offset == -1001

    def test_position_with_committed(self, consumer_factory, producer, topic_factory):
        topic, _ = topic_factory()

        with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            consumer.subscribe(topics=[topic])
            producer.produce(topic=topic, key="key", value="value")
            producer.flush()
            msg = consumer.poll(timeout=5.0)
            assert msg

            consumer.commit(message=msg)
            partitions = consumer.position(
                partitions=[TopicPartition(topic=topic, partition=0)]
            )
        assert partitions[0].offset == 1

    def test_position_with_committed_seek_to_beginning(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = topic_factory()

        with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            consumer.subscribe(topics=[topic])
            # Produce 3 messages
            producer.produce(topic=topic, key="key", value="value")
            producer.produce(topic=topic, key="key", value="value")
            producer.produce(topic=topic, key="key", value="value")
            producer.flush()

            # Consume and commit 3 messages
            messages_count = 0
            while Timeout():
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    continue
                consumer.commit(message=msg, asynchronous=False)
                messages_count += 1
                if messages_count == 3:
                    break

            # Check that current position is 3
            partitions = consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                ]
            )
            assert partitions[0].offset == 3
            # Seek to beginning of the partition
            consumer.seek(
                partition=TopicPartition(
                    topic=topic, partition=0, offset=confluent_kafka.OFFSET_BEGINNING
                )
            )
            # Consume one message
            msg = consumer.poll(5.0)
            # Check that message offset is 0
            assert msg.offset() == 0
            # Check that the position is 1
            partitions = consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                ]
            )
            assert partitions[0].offset == 1


class TestConsumerOffsetsForTimes:
    def test_offsets_for_times_topic_doesnt_exist(self, consumer):
        with consumer:
            with pytest.raises(confluent_kafka.KafkaException) as raised:
                consumer.offsets_for_times(
                    partitions=[TopicPartition(topic="some_topic", partition=0)]
                )
                assert (
                    raised.value.args[0]
                    == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART
                )

    def test_offsets_for_times_topic_exists_empty(self, consumer, topic_factory):
        topic, _ = topic_factory()
        with consumer:
            partitions = consumer.offsets_for_times(
                partitions=[
                    TopicPartition(
                        topic=topic, partition=0, offset=round(time.time() * 1000)
                    )
                ]
            )
        assert len(partitions) == 1
        assert partitions[0].offset == confluent_kafka.OFFSET_END

    def test_offsets_for_times_topic_with_messages(
        self, consumer, topic_factory, producer
    ):
        topic, _ = topic_factory()
        with consumer, producer:
            current_time_ms = round(time.time() * 1000)
            producer.produce(topic=topic, value="test", timestamp=current_time_ms)
            producer.flush()
            partitions = consumer.offsets_for_times(
                partitions=[
                    TopicPartition(topic=topic, partition=0, offset=current_time_ms + 1)
                ]
            )
            assert partitions[0].offset == confluent_kafka.OFFSET_END

            partitions = consumer.offsets_for_times(
                partitions=[
                    TopicPartition(topic=topic, partition=0, offset=current_time_ms)
                ]
            )
            assert partitions[0].offset == 0

            partitions = consumer.offsets_for_times(
                partitions=[
                    TopicPartition(topic=topic, partition=0, offset=current_time_ms - 1)
                ]
            )
            assert partitions[0].offset == 0
