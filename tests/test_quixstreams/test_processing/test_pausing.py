from unittest.mock import MagicMock

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer
from quixstreams.processing import PausingManager


class TestPausingManager:
    def test_revoke(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        topic, partition = "topic", 0
        pausing_manager.pause(topic=topic, partition=partition, resume_after=100)
        assert pausing_manager.is_paused(topic=topic, partition=partition)
        pausing_manager.revoke(topic=topic, partition=partition)
        assert not pausing_manager.is_paused(topic=topic, partition=partition)

    def test_pause(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)

        pausing_manager.pause(topic="topic1", partition=0, resume_after=1)
        assert pausing_manager.is_paused(topic="topic1", partition=0)
        assert consumer.pause.call_count == 1

    def test_pause_already_paused(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)

        pausing_manager.pause(topic="topic1", partition=0, resume_after=1)
        pausing_manager.pause(topic="topic1", partition=0, resume_after=1)
        assert consumer.pause.call_count == 1

    def test_is_paused_not_paused(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        assert not pausing_manager.is_paused(topic="topic1", partition=0)

    def test_resume_if_ready_nothing_paused(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        pausing_manager.resume_if_ready()
        assert not consumer.resume.called

    def test_resume_if_ready(self):
        consumer = MagicMock(spec_set=Consumer)
        pausing_manager = PausingManager(consumer=consumer)
        topic = "topic"
        partition1, partition2 = 0, 1

        # Pause one partition that is ready to be resumed right now
        pausing_manager.pause(topic=topic, partition=partition1, resume_after=0)
        # Pause another partition that not be resumed yet
        pausing_manager.pause(topic=topic, partition=partition2, resume_after=9999)
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
