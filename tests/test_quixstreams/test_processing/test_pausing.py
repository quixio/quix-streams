from unittest.mock import MagicMock

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer
from quixstreams.processing import PausingManager
from tests.utilities.utils import TopicPartitionStub


class TestPausingManager:
    def test_revoke(self):
        topic, partition = "topic", 0
        consumer = MagicMock(spec_set=Consumer)
        consumer.position.return_value = [
            TopicPartitionStub(topic=topic, partition=partition)
        ]
        pausing_manager = PausingManager(consumer=consumer)
        pausing_manager.pause(
            topic=topic, partition=partition, resume_after=100, offset_to_seek=0
        )
        assert pausing_manager.is_paused(topic=topic, partition=partition)
        pausing_manager.revoke(topic=topic, partition=partition)
        assert not pausing_manager.is_paused(topic=topic, partition=partition)

    def test_pause(self):
        topic, partition = "topic1", 0
        offset_to_seek = 999

        consumer = MagicMock(spec_set=Consumer)

        consumer.position.return_value = [
            TopicPartitionStub(topic=topic, partition=partition)
        ]
        pausing_manager = PausingManager(consumer=consumer)
        pausing_manager.pause(
            topic=topic,
            partition=partition,
            resume_after=1,
            offset_to_seek=offset_to_seek,
        )
        assert pausing_manager.is_paused(topic=topic, partition=partition)
        assert consumer.pause.call_count == 1
        assert consumer.seek.call_count == 1
        seek_call = consumer.seek.call_args_list[0]
        assert seek_call.kwargs["partition"] == TopicPartition(
            topic=topic, partition=partition, offset=offset_to_seek
        )

    def test_pause_already_paused(self):
        topic, partition = "topic1", 0
        offset_to_seek = 999
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        consumer.position.return_value = [
            TopicPartitionStub(topic=topic, partition=partition)
        ]

        pausing_manager.pause(
            topic="topic1", partition=0, resume_after=1, offset_to_seek=offset_to_seek
        )
        pausing_manager.pause(
            topic="topic1", partition=0, resume_after=1, offset_to_seek=offset_to_seek
        )
        assert consumer.pause.call_count == 1

    def test_is_paused_not_paused(self):
        topic, partition = "topic1", 0
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        assert not pausing_manager.is_paused(topic=topic, partition=partition)

    def test_resume_if_ready_nothing_paused(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        pausing_manager.resume_if_ready()
        assert not consumer.resume.called

    def test_resume_if_ready(self):
        topic = "topic"
        partition1, partition2 = 0, 1
        offset_to_seek = 999

        consumer = MagicMock(spec_set=Consumer)
        consumer.position.return_value = [
            TopicPartitionStub(topic=topic, partition=partition1)
        ]
        pausing_manager = PausingManager(consumer=consumer)

        # Pause one partition that is ready to be resumed right now
        pausing_manager.pause(
            topic=topic,
            partition=partition1,
            resume_after=0,
            offset_to_seek=offset_to_seek,
        )
        # Pause another partition that not be resumed yet
        pausing_manager.pause(
            topic=topic,
            partition=partition2,
            resume_after=9999,
            offset_to_seek=offset_to_seek,
        )
        pausing_manager.resume_if_ready()

        # Ensure that partition1 is resumed
        resume_calls = consumer.resume.call_args_list
        assert len(resume_calls) == 1
        resume_call = resume_calls[0]
        assert resume_call.kwargs["partitions"] == [
            TopicPartition(topic=topic, partition=partition1)
        ]

        # Ensure that partition2 is still paused
        assert pausing_manager.is_paused(topic=topic, partition=partition2)
