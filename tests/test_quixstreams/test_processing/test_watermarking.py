from unittest.mock import Mock

from quixstreams.models import Topic, TopicConfig
from quixstreams.processing.watermarking import WatermarkManager


class TestWatermarkBootstrap:
    """
    Basic tests for watermark bootstrapping.

    Note: Full testing of the progressive search algorithm requires integration
    tests with a real Kafka broker, as mocking the complex message polling and
    deserialization flow is error-prone and doesn't provide meaningful coverage.
    """

    def test_bootstrap_watermarks_empty_topic(self, topic_manager_factory):
        """
        Test that bootstrap handles an empty watermarks topic gracefully.
        """
        topic_manager = topic_manager_factory()
        producer = Mock()
        wm_manager = WatermarkManager(
            producer=producer, topic_manager=topic_manager, interval=1.0
        )

        test_topic = Topic(
            name="topic1",
            value_deserializer="json",
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        test_topic.broker_config = test_topic.create_config
        wm_manager.set_topics([test_topic])

        consumer = Mock()
        consumer.get_watermark_offsets.return_value = (0, 0)  # Empty topic

        wm_manager.bootstrap_watermarks(consumer)

        # Watermark should remain at -1
        assert wm_manager._watermarks[("topic1", 0)] == -1

        # No seek should be called for empty topic
        consumer.seek.assert_not_called()

    def test_bootstrap_watermarks_no_expected_tps(self, topic_manager_factory):
        """
        Test that bootstrap handles the case where no topic-partitions are expected.
        """
        topic_manager = topic_manager_factory()
        producer = Mock()
        wm_manager = WatermarkManager(
            producer=producer, topic_manager=topic_manager, interval=1.0
        )

        # Don't set any topics - no expected TPs
        consumer = Mock()

        wm_manager.bootstrap_watermarks(consumer)

        # Should exit early without calling get_watermark_offsets
        consumer.get_watermark_offsets.assert_not_called()

    def test_bootstrap_watermarks_exponential_backoff(self, topic_manager_factory):
        """
        Test that bootstrap uses exponential backoff when not all TPs are found.

        This test verifies true exponential backoff WITHOUT re-reading:
        1. Initial seek: offset 900 (1000 - 100), read 100 messages to offset 1000
        2. If not all TPs found, seek back: offset 700 (900 - 200), read 200 messages to offset 900
        3. If still not found, seek back: offset 300 (700 - 400), read 400 messages to offset 700
        4. Continues until all TPs found or offset 0 is reached

        Key: Each iteration seeks BACK from the previous position, not from high_offset.
        This avoids re-reading the same messages multiple times.
        """
        topic_manager = topic_manager_factory()
        producer = Mock()
        wm_manager = WatermarkManager(
            producer=producer, topic_manager=topic_manager, interval=1.0
        )

        # Set up 3 topic-partitions
        test_topic = Topic(
            name="topic1",
            value_deserializer="json",
            create_config=TopicConfig(num_partitions=3, replication_factor=1),
        )
        test_topic.broker_config = test_topic.create_config
        wm_manager.set_topics([test_topic])

        consumer = Mock()
        consumer.get_watermark_offsets.return_value = (0, 1000)

        # Track which iteration we're in
        poll_count = [0]
        found_partitions = set()

        def mock_poll(timeout):
            poll_count[0] += 1

            # First iteration: seeking to 900, reading toward 1000
            # Return messages for partitions 0 and 1 only
            if consumer.seek.call_count == 1:
                if poll_count[0] <= 100:
                    # Return watermarks for partitions 0 and 1
                    partition = 0 if poll_count[0] % 2 == 0 else 1
                    if partition not in found_partitions:
                        found_partitions.add(partition)
                    return create_mock_watermark_message(
                        wm_manager.watermarks_topic,
                        "topic1",
                        partition,
                        1000 + poll_count[0],
                    )
                else:
                    return None  # Timeout to trigger next iteration

            # Second iteration: seeking to 700 (900 - 200), reading toward 900
            # Now include partition 2
            elif consumer.seek.call_count == 2:
                # Return watermark for partition 2
                if poll_count[0] % 3 == 0:
                    found_partitions.add(2)
                    return create_mock_watermark_message(
                        wm_manager.watermarks_topic, "topic1", 2, 3000
                    )
                return None  # Return None to speed up the loop

            return None

        consumer.poll.side_effect = mock_poll

        # Run bootstrap
        wm_manager.bootstrap_watermarks(consumer)

        # Verify exponential backoff happened
        assert consumer.seek.call_count >= 2, "Should have seeked at least twice"

        # Verify seek offsets show TRUE exponential backoff (seeking backwards, not from high_offset)
        seek_calls = consumer.seek.call_args_list
        first_seek = seek_calls[0][0][0]
        second_seek = seek_calls[1][0][0]

        # First seek: offset = 1000 - 100 = 900
        assert (
            first_seek.offset == 900
        ), f"First seek should be at 900, got {first_seek.offset}"

        # Second seek: offset = 900 - 200 = 700 (seeking back from previous position)
        assert (
            second_seek.offset == 700
        ), f"Second seek should be at 700 (900 - 200), got {second_seek.offset}"

        # All watermarks should be set
        assert wm_manager._watermarks[("topic1", 0)] > -1
        assert wm_manager._watermarks[("topic1", 1)] > -1
        assert wm_manager._watermarks[("topic1", 2)] > -1


def create_mock_watermark_message(watermarks_topic, topic, partition, timestamp):
    """
    Helper to create a properly mocked Kafka message.
    """
    msg = Mock()
    msg.error.return_value = None
    msg.topic.return_value = watermarks_topic.name
    msg.partition.return_value = 0
    msg.offset.return_value = 1

    # Create a mock Row that will be returned by deserialize
    mock_row = Mock()
    mock_row.value = {
        "topic": topic,
        "partition": partition,
        "timestamp": timestamp,
    }

    # Mock the deserialize method to return our row
    watermarks_topic.deserialize = Mock(return_value=mock_row)

    return msg
