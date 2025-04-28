import pytest
from confluent_kafka import TopicPartition

from quixstreams.rowconsumer.buffering import InternalConsumerBuffer
from tests.utils import ConfluentKafkaMessageStub


class TestInternalConsumerBuffer:
    def test_pop_item_empty(self):
        buffer = InternalConsumerBuffer()

        assert buffer.pop() is None

        buffer.on_assign([TopicPartition(topic="test", partition=0)])
        assert buffer.pop() is None

    def test_pop_item_single_topic_multiple_partitions(self):
        """
        Test that we can pop items from the InternalConsumerBuffer when
        there's a single partition in the group (i.e. only one partition with the same number),
        """
        buffer = InternalConsumerBuffer()
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test", partition=1),
            ]
        )

        messages = [
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=0
            ),
            ConfluentKafkaMessageStub(
                topic="test", partition=1, value=b"1", timestamp=(1, 11), offset=1
            ),
        ]
        buffer.feed(messages=messages, high_watermarks={("test", 0): 0, ("test", 1): 1})

        assert buffer.pop() is messages[0]
        assert buffer.pop() is messages[1]

    def test_pop_item_multiple_topics_single_partition_in_order(self):
        """
        Test that InternalConsumerBuffer returns messages from different topics
        but the same partitions in timestamp order.
        """

        buffer = InternalConsumerBuffer()
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test1", partition=0),
            ]
        )

        # Simulate a batch of messages returned by Consumer.consume().
        # Note that messages are ordered per partition in this list.
        messages = [
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=0
            ),
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 13), offset=1
            ),
            ConfluentKafkaMessageStub(
                topic="test1", partition=0, value=b"1", timestamp=(1, 11), offset=0
            ),
            ConfluentKafkaMessageStub(
                topic="test1", partition=0, value=b"1", timestamp=(1, 12), offset=1
            ),
        ]
        buffer.feed(
            messages=messages, high_watermarks={("test", 0): 999, ("test1", 0): 999}
        )

        # Ensure that messages are returned in the correct order
        assert buffer.pop() is messages[0]
        assert buffer.pop() is messages[2]
        assert buffer.pop() is messages[3]
        # The last message will remain in the buffer because the partition
        # is not idle
        assert buffer.pop() is None

    def test_pop_item_multiple_topics_single_partition_empty(self):
        """
        Test that InternalConsumerBuffer does not return a message if the group
        has more than one partition, one of them is empty AND not idle
        (i.e. still has messages to be consumed from the topic)
        """

        buffer = InternalConsumerBuffer()
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test1", partition=0),
            ]
        )

        # Simulate a batch of messages returned by Consumer.consume().
        # Note that messages are ordered per partition in this list.
        messages = [
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=0
            ),
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 11), offset=1
            ),
        ]
        high_watermarks = {
            ("test", 0): 999,
            ("test1", 0): 999,  # This partition has messages to be consumed
        }

        buffer.feed(messages=messages, high_watermarks=high_watermarks)

        # The buffer must not return any messages because one of the two partitions
        # is empty and not idle (highwater > offset)
        assert buffer.pop() is None

    def test_pop_item_multiple_topics_single_partition_idle(self):
        """
        Test that InternalConsumerBuffer returns a message if the group
        has more than one partition, one of them is empty AND is idle.
        (i.e. up-to-date with the Kafka topic)
        """

        buffer = InternalConsumerBuffer()
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test1", partition=0),
            ]
        )

        # Simulate a batch of messages returned by Consumer.consume().
        # Note that messages are ordered per partition in this list.
        messages = [
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=0
            ),
            ConfluentKafkaMessageStub(
                topic="test1", partition=0, value=b"1", timestamp=(1, 11), offset=1
            ),
        ]
        high_watermarks = {
            ("test", 0): 0,  # This partition has no new messages to be consumed
            ("test1", 0): 999,
        }

        buffer.feed(messages=messages, high_watermarks=high_watermarks)

        # The first message must be returned
        assert buffer.pop() is messages[0]

        # The next message remains buffered until we update the watermark
        # for the empty partition buffer
        assert buffer.pop() is None

        # Update the high watermarks. The partition "test[0]" is now idle
        buffer.feed(messages=[], high_watermarks=high_watermarks)

        # The message from "test[1]" must be returned now
        assert buffer.pop() is messages[1]

    def test_pop_item_multiple_topics_single_partition_empty_idleness_unknown(self):
        """
        Test that InternalConsumerBuffer does not return a message if the group
        has more than one partition, one of them is empty AND its idleness is not known yet.
        """

        buffer = InternalConsumerBuffer()
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test1", partition=0),
            ]
        )

        # Simulate a batch of messages returned by Consumer.consume().
        # Note that messages are ordered per partition in this list.
        messages = [
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=0
            ),
            ConfluentKafkaMessageStub(
                topic="test", partition=0, value=b"1", timestamp=(1, 11), offset=1
            ),
        ]
        high_watermarks = {
            ("test", 0): 999,
            ("test1", 0): -1001,  # This partition has no new messages to be consumed
        }

        buffer.feed(messages=messages, high_watermarks=high_watermarks)

        # The buffer must not return any messages because one of the two partitions
        # is empty but its watermark is -1001 (unknown)
        assert buffer.pop() is None

    def test_feed_invalid_offset(self):
        buffer = InternalConsumerBuffer()
        buffer.on_assign([TopicPartition(topic="test", partition=0)])

        buffer.feed(
            [
                ConfluentKafkaMessageStub(
                    topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=10
                )
            ],
            high_watermarks={("test", 0): 999},
        )
        with pytest.raises(ValueError, match="Invalid offset"):
            buffer.feed(
                [
                    ConfluentKafkaMessageStub(
                        topic="test",
                        partition=0,
                        value=b"1",
                        timestamp=(1, 10),
                        offset=9,
                    )
                ],
                high_watermarks={("test", 0): 999},
            )

    def test_pause_full(self):
        buffer = InternalConsumerBuffer(max_partition_buffer_size=2)
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test1", partition=0),
            ]
        )

        high_watermarks = {("test", 0): 999, ("test1", 0): 999}

        buffer.feed(
            [
                ConfluentKafkaMessageStub(
                    topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=10
                ),
            ],
            high_watermarks=high_watermarks,
        )

        assert not buffer.pause_full()

        buffer.feed(
            [
                ConfluentKafkaMessageStub(
                    topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=11
                ),
            ],
            high_watermarks=high_watermarks,
        )

        # The full partition must be paused
        assert buffer.pause_full() == [("test", 0)]

        # The partition can be paused only once
        assert not buffer.pause_full()

    def test_resume_empty(self):
        buffer = InternalConsumerBuffer(max_partition_buffer_size=1)
        buffer.on_assign(
            [
                TopicPartition(topic="test", partition=0),
                TopicPartition(topic="test1", partition=0),
            ]
        )

        high_watermarks = {("test", 0): 999, ("test1", 0): 999}

        buffer.feed(
            [
                ConfluentKafkaMessageStub(
                    topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=10
                ),
                ConfluentKafkaMessageStub(
                    topic="test1", partition=0, value=b"1", timestamp=(1, 11), offset=10
                ),
            ],
            high_watermarks=high_watermarks,
        )
        # No buffers should be resumed since none of them are empty yet
        assert not buffer.resume_empty()

        # Pause full buffers
        assert buffer.pause_full()

        # Pop an item from one of them
        assert buffer.pop()

        # Resume the buffer that is now empty
        assert buffer.resume_empty() == [("test", 0)]

        # The partition can be resumed only once
        assert not buffer.resume_empty()

    def test_clear(self):
        buffer = InternalConsumerBuffer()
        buffer.on_assign([TopicPartition(topic="test", partition=0)])

        buffer.feed(
            [
                ConfluentKafkaMessageStub(
                    topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=10
                ),
            ],
            high_watermarks={("test", 0): 999},
        )

        # Clear the contents of the topic partition buffer
        buffer.clear(topic="test", partition=0)

        # Ensure that the previously added message is not available anymore
        assert buffer.pop() is None

    def test_close(self):
        buffer = InternalConsumerBuffer()
        buffer.on_assign([TopicPartition(topic="test", partition=0)])

        buffer.feed(
            [
                ConfluentKafkaMessageStub(
                    topic="test", partition=0, value=b"1", timestamp=(1, 10), offset=10
                ),
            ],
            high_watermarks={("test", 0): 999},
        )

        # Close the buffer and clear it fully
        buffer.close()

        # Ensure that the previously added message is not available anymore
        assert buffer.pop() is None
