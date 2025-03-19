import uuid
from unittest.mock import MagicMock

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer
from quixstreams.models import TopicConfig
from quixstreams.processing import PausingManager


class TestPausingManager:
    def test_pause(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        offset_to_seek = 999

        consumer = MagicMock(spec_set=Consumer)
        assignment = [TopicPartition(topic=topic.name, partition=0)]
        consumer.assignment.return_value = assignment
        consumer.position.return_value = assignment

        pausing_manager = PausingManager(consumer=consumer, topic_manager=topic_manager)
        pausing_manager.pause(
            resume_after=1,
            offsets_to_seek={(topic.name, 0): offset_to_seek},
        )
        assert consumer.pause.call_count == 1
        assert consumer.seek.call_count == 1
        seek_call = consumer.seek.call_args_list[0]
        assert seek_call.kwargs["partition"] == TopicPartition(
            topic=topic.name, partition=0, offset=offset_to_seek
        )

    def test_resume_if_ready_nothing_paused(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        consumer = MagicMock(spec_set=Consumer)
        assignment = [TopicPartition(topic=topic.name, partition=0)]
        consumer.assignment.return_value = assignment

        pausing_manager = PausingManager(consumer=consumer, topic_manager=topic_manager)
        pausing_manager.resume_if_ready()
        assert not consumer.resume.called

    def test_resume_if_ready(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=2, replication_factor=1),
        )
        # Create a changelog topic
        topic_manager.changelog_topic(
            stream_id=topic.name, store_name="default", config=topic.broker_config
        )

        offset_to_seek = 999

        consumer = MagicMock(spec_set=Consumer)
        assignment = [
            TopicPartition(topic=topic.name, partition=0),
            TopicPartition(topic=topic.name, partition=1),
        ]
        consumer.assignment.return_value = assignment
        consumer.position.return_value = assignment

        pausing_manager = PausingManager(consumer=consumer, topic_manager=topic_manager)

        # Pause one partition that is ready to be resumed right now
        pausing_manager.pause(
            resume_after=0,
            offsets_to_seek={
                (topic.name, 0): offset_to_seek,
                (topic.name, 1): offset_to_seek,
            },
        )
        # Resume partitions
        pausing_manager.resume_if_ready()

        # Ensure that only data partitions are resumed
        resume_calls = consumer.resume.call_args_list
        assert len(resume_calls) == 2
        assert resume_calls[0].kwargs["partitions"] == [
            TopicPartition(topic=topic.name, partition=0)
        ]
        assert resume_calls[1].kwargs["partitions"] == [
            TopicPartition(topic=topic.name, partition=1)
        ]
