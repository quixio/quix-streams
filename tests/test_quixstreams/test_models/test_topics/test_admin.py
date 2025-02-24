import logging
from unittest.mock import patch
from uuid import uuid4

import pytest
from confluent_kafka.admin import TopicMetadata
from confluent_kafka.error import KafkaException

from quixstreams.models import Topic
from quixstreams.models.topics import TopicAdmin, TopicConfig
from quixstreams.models.topics.exceptions import CreateTopicFailure, CreateTopicTimeout

logger = logging.getLogger(__name__)


class TestTopicAdmin:
    def test_list_topics(self, topic_admin, topic_factory):
        topic_name, _ = topic_factory()
        result = topic_admin.list_topics()

        assert isinstance(result, dict)
        assert isinstance(result[topic_name], TopicMetadata)

    def test_inspect_topics(self, topic_admin, topic_factory):
        topic_name, _ = topic_factory()
        not_a_topic = "non-existent-topic-name"
        result = topic_admin.inspect_topics([topic_name, not_a_topic])

        assert isinstance(result, dict)
        assert isinstance(result[topic_name], TopicConfig)
        assert result[not_a_topic] is None

    def test_inspect_topics_timeout(self):
        with pytest.raises(KafkaException):
            TopicAdmin("bad_address").inspect_topics(["my_topic"], timeout=1)

    def test_create_topics(self, topic_admin, topic_manager_factory):
        topic_manager = topic_manager_factory()
        # Generate topics manually to avoid creating them via TopicManager
        topic1 = Topic(name=str(uuid4()), create_config=topic_manager.topic_config())
        topic2 = Topic(name=str(uuid4()), create_config=topic_manager.topic_config())

        topic_admin.create_topics([topic1, topic2])

        topics = topic_admin.list_topics()
        assert topic1.name in topics
        assert topic2.name in topics

    def test_create_topics_timeout(self, topic_manager_factory):
        admin = TopicAdmin("bad_address")
        topic_manager = topic_manager_factory()

        # Generate topics manually to avoid creating them via TopicManager
        topic = Topic(name=str(uuid4()), create_config=topic_manager.topic_config())

        with pytest.raises(KafkaException):
            admin.create_topics([topic], timeout=1)

    def test_create_topics_finalize_timeout(self, topic_admin, topic_manager_factory):
        """
        Finalize timeout raises as expected (not a request-based timeout).
        """
        topic_manager = topic_manager_factory()
        # Generate topics manually to avoid creating them via TopicManager
        topic = Topic(name=str(uuid4()), create_config=topic_manager.topic_config())
        with pytest.raises(CreateTopicTimeout) as e:
            topic_admin.create_topics([topic], finalize_timeout=0)

        error_str = str(e.value.args[0])
        assert topic.name in error_str

    def test_create_topics_already_exist(
        self, topic_admin, topic_manager_factory, topic_factory, caplog
    ):
        topic_name, _ = topic_factory()

        topic_manager = topic_manager_factory()
        # Generate topics manually to avoid creating them via TopicManager
        existing_topic = Topic(
            name=topic_name, create_config=topic_manager.topic_config()
        )

        with (
            caplog.at_level(level=logging.INFO),
            patch.object(topic_admin, "list_topics") as list_topics_mock,
        ):
            # Mock "list_topics" call to simulate a topic being created
            # simultaneously by multiple instances
            list_topics_mock.return_value = {}
            topic_admin.create_topics([existing_topic])

        assert f'Topic "{existing_topic.name}" already exists' in caplog.text

    def test_create_topics_invalid_config(self, topic_admin, topic_manager_factory):
        topic_manager = topic_manager_factory()
        invalid_topic = Topic(
            name=str(uuid4()),
            create_config=topic_manager.topic_config(
                extra_config={"bad_option": "not_real"}
            ),
        )

        with pytest.raises(CreateTopicFailure, match="Unknown topic config name"):
            topic_admin.create_topics([invalid_topic])
