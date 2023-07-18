import asyncio
import time
import uuid
from concurrent.futures import Future
from typing import List

import confluent_kafka
import pytest
from confluent_kafka.error import KafkaError
from confluent_kafka import TopicPartition


DEFAULT_TIMEOUT = 15.0


async def wait_for_future(fut: Future, timeout: float = DEFAULT_TIMEOUT):
    wrapped_fut = asyncio.wrap_future(fut)
    return await asyncio.wait_for(wrapped_fut, timeout=timeout)


async def test_consumer_start_close(
    consumer,
):
    async with consumer:
        metadata = await consumer.list_topics()
        assert metadata
        broker_meta = metadata.brokers[0]
        broker_address = f"{broker_meta.host}:{broker_meta.port}"
        assert broker_address

    assert not consumer.running


async def test_consumer_close_inner_consumer(
    consumer,
):
    async with consumer:
        metadata = await consumer.list_topics()
        assert metadata
        broker_meta = metadata.brokers[0]
        broker_address = f"{broker_meta.host}:{broker_meta.port}"
        assert broker_address
        await consumer.close()
        with pytest.raises(RuntimeError) as raised:
            await consumer.poll(timeout=0)
    assert str(raised.value) == "Consumer closed"


class TestAsyncConsumerSubscribe:
    async def test_consumer_subscribe_topic_exists(
        self,
        consumer,
        topic_factory,
    ):
        topic_name, num_partitions = await topic_factory()

        async with consumer:
            await consumer.subscribe(topics=[topic_name])
            msg = await consumer.poll(timeout=1)
            assert msg is None
            metadata = await consumer.list_topics()
            assert topic_name in metadata.topics
            assert len(metadata.topics[topic_name].partitions) == num_partitions

    async def test_consumer_subscribe_topic_doesnt_exist(
        self,
        consumer,
        topic_factory,
    ):
        topic_name = str(uuid.uuid4())

        async with consumer:
            await consumer.subscribe(topics=[topic_name])
            msg = await consumer.poll(timeout=1)
            assert msg
            err = msg.error()
            assert err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART

    async def test_consumer_subscribe_multiple_topics(
        self,
        consumer,
        topic_factory,
    ):
        topic1, _ = await topic_factory()
        topic2, _ = await topic_factory()

        assigned_partitions = Future()

        def on_assign(_, partitions: List[TopicPartition]):
            assigned_partitions.set_result(partitions)

        async with consumer:
            await consumer.subscribe(topics=[topic1, topic2], on_assign=on_assign)
            await consumer.poll(timeout=5)
            assigned_partitions = await wait_for_future(assigned_partitions)
        assert len(assigned_partitions) == 2
        for partition in assigned_partitions:
            assert partition.topic in (topic1, topic2)

    async def test_consumer_subscribe_multiple_times(
        self,
        consumer,
        topic_factory,
    ):
        topic1, _ = await topic_factory()
        topic2, _ = await topic_factory()

        assigned_partitions = Future()

        def on_assign(_, partitions: List[TopicPartition]):
            assigned_partitions.set_result(partitions)

        async with consumer:
            await consumer.subscribe(topics=[topic1], on_assign=on_assign)
            await consumer.poll(timeout=5)
            partitions = await wait_for_future(assigned_partitions)
            assert len(partitions) == 1

            assigned_partitions = Future()
            await consumer.subscribe(topics=[topic1, topic2], on_assign=on_assign)
            await consumer.poll(timeout=5)
            partitions = await wait_for_future(assigned_partitions)
            assert len(partitions) == 2


class TestAsyncConsumerOnAssign:
    async def test_consumer_on_assign_callback_single_consumer(
        self, consumer, topic_factory, event_loop
    ):
        topic_name, num_partitions = await topic_factory()

        num_partitions_assigned = Future()

        def on_assign(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            num_partitions_assigned.set_result(len(partitions))

        async with consumer:
            await consumer.subscribe(topics=[topic_name], on_assign=on_assign)

            async def poll():
                while consumer.running:
                    msg = await consumer.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            await wait_for_future(num_partitions_assigned)
            assert num_partitions == num_partitions_assigned.result()
        await task

    async def test_consumer_on_assign_callback_new_partition_added(
        self, consumer, topic_factory, event_loop, set_topic_partitions
    ):
        topic_name, num_partitions = await topic_factory()

        num_partitions_assigned = Future()

        def on_assign(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            num_partitions_assigned.set_result(len(partitions))

        async with consumer:
            await consumer.subscribe(topics=[topic_name], on_assign=on_assign)

            async def poll():
                while consumer.running:
                    msg = await consumer.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            # Wait for the initial partition assignment
            assert await wait_for_future(num_partitions_assigned) == 1
            num_partitions_assigned = Future()
            # Increase topic partitions count to 10
            await set_topic_partitions(topic=topic_name, num_partitions=10)
            # Validate that new partitions are assigned to a consumer
            assert await wait_for_future(num_partitions_assigned) == 10
        await task


class TestAsyncConsumerOnRevoke:
    async def test_consumer_on_revoke_callback_consumer_closed(
        self, consumer, topic_factory, event_loop
    ):
        """
        Validate that AsyncConsumer handles on_revoke callback
        """
        topic_name, num_partitions = await topic_factory()

        num_partitions_assigned = Future()
        num_partitions_revoked = Future()

        def on_assign(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            num_partitions_assigned.set_result(len(partitions))

        def on_revoke(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            num_partitions_revoked.set_result(len(partitions))

        async with consumer:
            await consumer.subscribe(
                topics=[topic_name], on_assign=on_assign, on_revoke=on_revoke
            )

            async def poll():
                while consumer.running:
                    msg = await consumer.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            await wait_for_future(num_partitions_assigned)
        num_partitions_revoked = await wait_for_future(num_partitions_revoked)
        await task
        assert num_partitions_revoked == num_partitions

    async def test_consumer_on_revoke_callback_new_consumer_joined(
        self, consumer_factory, topic_factory, event_loop
    ):
        """
        Validate that AsyncConsumer handles on_revoke callback on new consumer
        joining the consumer group
        """
        topic_name, num_partitions = await topic_factory(num_partitions=2)

        num_partitions_assigned = Future()
        num_partitions_revoked = Future()

        def on_assign(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            num_partitions_assigned.set_result(len(partitions))

        def on_revoke(
            _: confluent_kafka.Consumer,
            partitions: List[confluent_kafka.TopicPartition],
        ):
            num_partitions_revoked.set_result(len(partitions))

        # Start first consumer
        async with consumer_factory() as consumer1:
            await consumer1.subscribe(
                topics=[topic_name], on_assign=on_assign, on_revoke=on_revoke
            )

            async def poll():
                while consumer1.running:
                    msg = await consumer1.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            # Wait until consumer has partitions assigned
            await wait_for_future(num_partitions_assigned)

            # Start second consumer
            async with consumer_factory() as consumer2:
                await consumer2.subscribe(topics=[topic_name])
                # Wait until partitions are revoked from the first consumer
                num_partitions_revoked = await wait_for_future(num_partitions_revoked)
        await task
        # Make sure some partitions are revoked
        assert num_partitions_revoked


class TestAsyncConsumerPoll:
    async def test_poll_single_messsage_success(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = await topic_factory()

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

        async with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            await consumer.subscribe(topics=[topic])
            for msg in messages:
                await producer.produce(
                    topic=topic,
                    key=msg["key"],
                    value=msg["value"],
                    headers=msg["headers"],
                    blocking=True,
                )

            for msg_expected in messages:
                msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)

                assert not msg.error()
                assert msg.key() == msg_expected["key"].encode()
                assert msg.value() == msg_expected["value"]
                assert msg.headers() == list(msg_expected["headers"].items())

    async def test_poll_multiple_partitions_success(
        self, consumer_factory, producer, topic_factory
    ):
        topic, num_partitions = await topic_factory(num_partitions=5)

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

        async with consumer_factory(auto_offset_reset="earliest") as consumer:
            await consumer.subscribe(topics=[topic])
            async with producer:
                for key, msg in messages.items():
                    await producer.produce(
                        topic=topic,
                        key=key,
                        value=msg["value"],
                        headers=msg["headers"],
                        blocking=True,
                    )

            for i in range(len(messages)):
                msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)

                assert not msg.error()
                key = msg.key().decode()
                assert key in messages
                assert msg.value() == messages[key]["value"]
                assert msg.headers() == list(messages[key]["headers"].items())

    async def test_poll_many_success(self, consumer_factory, producer, topic_factory):
        topic, num_partitions = await topic_factory(num_partitions=5)
        num_messages = 35
        batch_size = 10

        msg_sent = {
            "key": "key1",
            "value": b"test1",
            "headers": {"header": b"header"},
        }
        async with producer:
            for _ in range(num_messages):
                await producer.produce(
                    topic=topic,
                    key=msg_sent["key"],
                    value=msg_sent["value"],
                    headers=msg_sent["headers"],
                )

        num_consumed = 0

        async with consumer_factory(auto_offset_reset="earliest") as consumer:
            await consumer.subscribe(topics=[topic])

            total_timeout_sec = 30
            started_at = time.monotonic()
            while num_consumed < num_messages:
                if (time.monotonic() - started_at) > total_timeout_sec:
                    raise TimeoutError(
                        f"Test timeout of {total_timeout_sec}s is exceeded"
                    )
                batch = await consumer.poll_many(batch_size=batch_size, timeout=5.0)
                assert len(batch) <= batch_size
                for msg in batch:
                    assert not msg.error()
                    key = msg.key().decode()
                    assert key == msg_sent["key"]
                    assert msg.value() == msg_sent["value"]
                    assert msg.headers() == list(msg_sent["headers"].items())
                num_consumed += len(batch)

    async def test_poll_many_empty(self, consumer_factory, topic_factory):
        topic, _ = await topic_factory()
        async with consumer_factory(auto_offset_reset="earliest") as consumer:
            await consumer.subscribe(topics=[topic])
            batch = await consumer.poll_many(batch_size=5, timeout=1)
            assert isinstance(batch, list)
            assert len(batch) == 0

    async def test_poll_many_size_accumulated(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = await topic_factory()
        num_messages = 5
        msg_sent = {
            "key": "key1",
            "value": b"test1",
            "headers": {"header": b"header"},
        }
        async with producer:
            for _ in range(num_messages):
                await producer.produce(
                    topic=topic,
                    key=msg_sent["key"],
                    value=msg_sent["value"],
                    headers=msg_sent["headers"],
                )

        async with consumer_factory(auto_offset_reset="earliest") as consumer:
            await consumer.subscribe(topics=[topic])
            batch = await consumer.poll_many(
                batch_size=num_messages, timeout=DEFAULT_TIMEOUT
            )
            assert len(batch) == num_messages

    async def test_poll_many_size_timeout_exceeded(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = await topic_factory()
        num_messages = 5
        msg_sent = {
            "key": "key1",
            "value": b"test1",
            "headers": {"header": b"header"},
        }
        async with producer:
            # Send 5 messages
            for _ in range(num_messages):
                await producer.produce(
                    topic=topic,
                    key=msg_sent["key"],
                    value=msg_sent["value"],
                    headers=msg_sent["headers"],
                )

        async with consumer_factory(auto_offset_reset="earliest") as consumer:
            await consumer.subscribe(topics=[topic])
            # Try to consume 10 messages with a short timeout.
            # Expect only 5 messages to arrive in a batch
            batch = await consumer.poll_many(batch_size=num_messages + 5, timeout=5)
            assert len(batch) == num_messages


class TestAsyncConsumerStoreOffsets:
    async def test_store_offsets_committed(
        self, consumer_factory, producer_factory, topic_factory
    ):
        """
        Test that store_offsets() submits messages for commit,
        and AsyncConsumer commits the offsets on closing
        """
        topic, _ = await topic_factory()
        committed_msg = {
            "key": "key1",
            "value": b"test1",
        }
        new_msg = {
            "key": "key2",
            "value": b"test1",
        }

        async with consumer_factory(
            auto_offset_reset="earliest"
        ) as consumer, producer_factory() as producer:
            await consumer.subscribe(topics=[topic])
            # Produce a message
            await producer.produce(
                topic=topic,
                key=committed_msg["key"],
                value=committed_msg["value"],
                blocking=True,
            )

            # Receive a message
            msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Store offsets to be committed later by autocommit and close the consumer
            consumer.store_offsets(message=msg)

        # Start new consumer and producer
        async with consumer_factory(
            auto_offset_reset="latest"
        ) as consumer, producer_factory() as producer:
            await consumer.subscribe(topics=[topic])
            # Produce a new message
            await producer.produce(
                topic=topic,
                key=new_msg["key"],
                value=new_msg["value"],
                blocking=True,
            )
            # Consume a message
            msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Ensure that only new uncommitted message is received
            assert msg.key() == new_msg["key"].encode()

    async def test_store_offsets_none_passed(self, consumer):
        async with consumer:
            with pytest.raises(ValueError) as raised:
                consumer.store_offsets()
            assert str(raised.value) == 'One of "message" or "offsets" must be passed'

    async def test_store_offsets_offsets_and_message_passed(self, consumer):
        async with consumer:
            with pytest.raises(ValueError) as raised:
                consumer.store_offsets(offsets=[], message=123)
            assert (
                str(raised.value)
                == 'Parameters "message" and "offsets" are mutually exclusive'
            )

    async def test_store_offsets_autocommit_disabled(
        self, consumer_factory, producer_factory, topic_factory
    ):
        topic, _ = await topic_factory()
        msg = {
            "key": "key1",
            "value": b"test1",
        }

        async with consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer, producer_factory() as producer:
            await consumer.subscribe(topics=[topic])
            # Produce a message
            await producer.produce(
                topic=topic,
                key=msg["key"],
                value=msg["value"],
                blocking=True,
            )

            # Receive a message
            msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Store offsets to be committed later by autocommit and close the consumer
            consumer.store_offsets(message=msg)

        # Start new consumer with offset reset "latest" and producer
        async with consumer_factory(
            auto_offset_reset="latest"
        ) as consumer, producer_factory() as producer:
            await consumer.subscribe(topics=[topic])
            # Try to consume a message
            msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)
            # Ensure that message is empty.
            # Previous consumer hasn't committed anything,
            # and offset reset "latest" doesn't see any messages.
            assert msg is None


class TestAsyncConsumerCommit:
    async def test_commit_none_passed(self, consumer_factory):
        async with consumer_factory() as consumer:
            with pytest.raises(ValueError) as raised:
                await consumer.commit()
        assert str(raised.value) == 'One of "message" or "offsets" must be passed'

    async def test_commit_offsets_and_message_passed(self, consumer):
        async with consumer:
            with pytest.raises(ValueError) as raised:
                await consumer.commit(offsets=[], message=123)
            assert (
                str(raised.value)
                == 'Parameters "message" and "offsets" are mutually exclusive'
            )

    async def test_commit_committed_sync(
        self, consumer_factory, producer_factory, topic_factory
    ):
        topic, _ = await topic_factory()
        committed_msg = {
            "key": "key1",
            "value": b"test1",
        }

        async with consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer, producer_factory() as producer:
            await consumer.subscribe(topics=[topic])
            # Produce a message
            await producer.produce(
                topic=topic,
                key=committed_msg["key"],
                value=committed_msg["value"],
                blocking=True,
            )

            # Receive a message
            msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Commit a message synchronously
            committed = await consumer.commit(message=msg, asynchronous=False)
            assert committed
            assert committed[0].offset == 1

        async with consumer_factory(auto_offset_reset="latest") as consumer:
            committed_partitions = await consumer.committed(
                partitions=[confluent_kafka.TopicPartition(topic, partition=0)],
            )
            assert committed_partitions[0].offset == 1

    async def test_commit_committed_async(
        self, consumer_factory, producer_factory, topic_factory
    ):
        topic, _ = await topic_factory()
        committed_msg = {
            "key": "key1",
            "value": b"test1",
        }

        async with consumer_factory(
            auto_offset_reset="earliest", auto_commit_enable=False
        ) as consumer, producer_factory() as producer:
            await consumer.subscribe(topics=[topic])
            # Produce a message
            await producer.produce(
                topic=topic,
                key=committed_msg["key"],
                value=committed_msg["value"],
                blocking=True,
            )

            # Receive a message
            msg = await consumer.poll(timeout=DEFAULT_TIMEOUT)
            assert not msg.error()
            # Commit a message asynchronously
            committed = await consumer.commit(message=msg, asynchronous=True)
            assert committed is None

        # Start new consumer
        async with consumer_factory(auto_offset_reset="latest") as consumer:
            committed_partitions = await consumer.committed(
                partitions=[confluent_kafka.TopicPartition(topic, partition=0)],
            )
            assert committed_partitions[0].offset == 1


class TestAsyncConsumerAssignment:
    async def test_assignment_empty(self, consumer):
        async with consumer:
            topics = await consumer.assignment()
            assert not topics

    async def test_assignment_assigned(self, consumer, topic_factory):
        topic, _ = await topic_factory()

        assigned = Future()
        async with consumer:
            await consumer.subscribe(
                [topic], on_assign=lambda *args: assigned.set_result(True)
            )

            async def poll():
                while consumer.running:
                    msg = await consumer.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            await wait_for_future(assigned)
            topics = await consumer.assignment()
            assert topics
            assert topics[0].topic == topic


class TestAsyncConsumerUnsubscribe:
    async def test_subscribe_assign_unsubscribe(self, topic_factory, consumer):
        topic, _ = await topic_factory()

        assigned = Future()
        revoked = Future()

        async with consumer:
            await consumer.subscribe(
                [topic],
                on_assign=lambda *args: assigned.set_result(True),
                on_revoke=lambda *args: revoked.set_result(True),
            )

            async def poll():
                while consumer.running:
                    msg = await consumer.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            await wait_for_future(assigned)

            await consumer.unsubscribe()
            await wait_for_future(revoked)

    async def test_unsubscribe_not_subscribed(self, consumer):
        async with consumer:
            await consumer.unsubscribe()


class TestAsyncConsumerListTopics:
    async def test_list_topics_all_topics(self, consumer, topic_factory):
        topic1, _ = await topic_factory()
        topic2, _ = await topic_factory()

        async with consumer:
            metadata = await consumer.list_topics()
        for topic in (topic1, topic2):
            assert topic in metadata.topics
            assert not metadata.topics[topic].error

    async def test_list_topics_one_topic(self, consumer, topic_factory):
        topic1, _ = await topic_factory()
        topic2, _ = await topic_factory()
        async with consumer:
            metadata = await consumer.list_topics(topic=topic2)
        assert topic2 in metadata.topics
        assert len(metadata.topics) == 1
        assert not metadata.topics[topic2].error

    async def test_list_topics_one_topic_doesnt_exist(self, consumer):
        async with consumer:
            metadata = await consumer.list_topics(topic="another_topic")
        assert "another_topic" in metadata.topics
        assert (
            metadata.topics["another_topic"].error == KafkaError.UNKNOWN_TOPIC_OR_PART
        )


class TestAsyncConsumerPauseResume:
    async def test_pause_resume(self, consumer_factory, topic_factory, producer):
        """
        Test that consumer doesn't receive messages for paused partitions and resumes
        consuming after `resume()`
        """
        topic, _ = await topic_factory(num_partitions=2)

        async with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            await consumer.subscribe(topics=[topic])
            # Send a message
            await producer.produce(
                key="key", value="value", topic=topic, partition=1, blocking=True
            )
            # Consume a message
            msg = await consumer.poll(5.0)
            assert msg
            assert msg.key() == b"key"
            # Pause consuming for partition 1
            await consumer.pause(partitions=[TopicPartition(topic=topic, partition=1)])
            # Produce messages for partitions 0 and 1
            await producer.produce(
                key="key", value="value", topic=topic, partition=0, blocking=True
            )
            await producer.produce(
                key="key", value="value", topic=topic, partition=1, blocking=True
            )
            # Poll a batch and ensure that we receive messages only from partition 0
            batch = await consumer.poll_many(timeout=5.0, batch_size=2)
            assert len(batch) == 1
            assert batch[0].partition() == 0
            # Resume consuming from partition 1
            await consumer.resume(partitions=[TopicPartition(topic=topic, partition=1)])
            # Receive a message from partition 1
            msg = await consumer.poll(5.0)
            assert msg
            assert msg.key() == b"key"


class TestAsyncConsumerGetWatermarkOffsets:
    async def test_get_watermark_offsets_empty_topic(self, topic_factory, consumer):
        topic, _ = await topic_factory()
        assigned = Future()
        async with consumer:
            await consumer.subscribe(
                topics=[topic], on_assign=lambda *args: assigned.set_result(True)
            )

            async def poll():
                while consumer.running:
                    msg = await consumer.poll(timeout=0.1)
                    assert msg is None

            task = asyncio.create_task(poll())
            low, high = await consumer.get_watermark_offsets(
                partition=TopicPartition(topic=topic, partition=0)
            )
            assert low == 0
            assert high == 0

    async def test_get_watermark_offsets_with_committed(
        self, topic_factory, consumer, producer
    ):
        topic, _ = await topic_factory()
        assigned = Future()
        async with consumer, producer:
            await consumer.subscribe(
                topics=[topic], on_assign=lambda *args: assigned.set_result(True)
            )

            await consumer.poll(timeout=5.0)
            await producer.produce(topic=topic, key="key", value="value")
            msg = await consumer.poll(timeout=5.0)
            await consumer.commit(message=msg)

            low, high = await consumer.get_watermark_offsets(
                partition=TopicPartition(topic=topic, partition=0)
            )
            assert low == 0
            assert high == 1


class TestAsyncConsumerPositionSeek:
    async def test_position_empty_topic(self, consumer, topic_factory):
        topic, _ = await topic_factory(num_partitions=2)

        assigned = Future()
        async with consumer:
            await consumer.subscribe(
                topics=[topic], on_assign=lambda *args: assigned.set_result(True)
            )
            await consumer.poll(timeout=5.0)
            await wait_for_future(assigned)

            partitions = await consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                    TopicPartition(topic=topic, partition=1),
                ]
            )
        for partition in partitions:
            assert partition.offset == -1001

    async def test_position_with_committed(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = await topic_factory()

        async with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            await consumer.subscribe(topics=[topic])
            await producer.produce(topic=topic, key="key", value="value", blocking=True)
            msg = await consumer.poll(timeout=5.0)
            assert msg

            await consumer.commit(message=msg)
            partitions = await consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                ]
            )
        assert partitions[0].offset == 1

    async def test_position_with_committed_seek_to_beginning(
        self, consumer_factory, producer, topic_factory
    ):
        topic, _ = await topic_factory()

        async with consumer_factory(auto_offset_reset="earliest") as consumer, producer:
            await consumer.subscribe(topics=[topic])
            # Produce 3 messages
            await producer.produce(topic=topic, key="key", value="value", blocking=True)
            await producer.produce(topic=topic, key="key", value="value", blocking=True)
            await producer.produce(topic=topic, key="key", value="value", blocking=True)
            # Consume and commit 3 messages
            batch = await consumer.poll_many(timeout=5.0, batch_size=3)
            assert len(batch) == 3

            # Check that current position is 3
            partitions = await consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                ]
            )
            assert partitions[0].offset == 3
            # Seek to beginning of the partition
            await consumer.seek(
                partition=TopicPartition(
                    topic=topic, partition=0, offset=confluent_kafka.OFFSET_BEGINNING
                )
            )
            # Consume one message
            msg = await consumer.poll(5.0)
            # Check that message offset is 0
            assert msg.offset() == 0
            # Check that the position is 1
            partitions = await consumer.position(
                partitions=[
                    TopicPartition(topic=topic, partition=0),
                ]
            )
            assert partitions[0].offset == 1


class TestAsyncConsumerOffsetsForTimes:
    async def test_offsets_for_times_topic_doesnt_exist(self, consumer):
        async with consumer:
            with pytest.raises(confluent_kafka.KafkaException) as raised:
                await consumer.offsets_for_times(
                    partitions=[TopicPartition(topic="some_topic", partition=0)]
                )
                assert (
                    raised.value.args[0]
                    == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART
                )

    async def test_offsets_for_times_topic_exists_empty(self, consumer, topic_factory):
        topic, _ = await topic_factory()
        async with consumer:
            partitions = await consumer.offsets_for_times(
                partitions=[
                    TopicPartition(
                        topic=topic, partition=0, offset=round(time.time() * 1000)
                    )
                ]
            )
        assert len(partitions) == 1
        assert partitions[0].offset == confluent_kafka.OFFSET_END

    async def test_offsets_for_times_topic_with_messages(
        self, consumer, topic_factory, producer
    ):
        topic, _ = await topic_factory()
        async with consumer, producer:
            current_time_ms = round(time.time() * 1000)
            await producer.produce(
                topic=topic, value="test", timestamp=current_time_ms, blocking=True
            )
            partitions = await consumer.offsets_for_times(
                partitions=[
                    TopicPartition(topic=topic, partition=0, offset=current_time_ms + 1)
                ]
            )
            assert partitions[0].offset == confluent_kafka.OFFSET_END

            partitions = await consumer.offsets_for_times(
                partitions=[
                    TopicPartition(topic=topic, partition=0, offset=current_time_ms)
                ]
            )
            assert partitions[0].offset == 0

            partitions = await consumer.offsets_for_times(
                partitions=[
                    TopicPartition(topic=topic, partition=0, offset=current_time_ms - 1)
                ]
            )
            assert partitions[0].offset == 0


class TestAsyncConsumerIncrementalAssign:
    async def test_incremental_assign(self, topic_factory, consumer):
        pass
