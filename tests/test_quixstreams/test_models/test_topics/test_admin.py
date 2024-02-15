import pytest
import logging
from confluent_kafka.admin import TopicMetadata
from unittest.mock import patch
from uuid import uuid4

from quixstreams.models.topics import TopicConfig
from quixstreams.models.topics.exceptions import CreateTopicTimeout, CreateTopicFailure


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

    def test_create_topics(self, topic_admin, topic_factory, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(name=str(uuid4()))
        topic2 = topic_manager.topic(name=str(uuid4()))

        topic_admin.create_topics([topic1, topic2])

        topics = topic_admin.list_topics()
        assert topic1.name in topics
        assert topic2.name in topics

    def test_create_topics_finalize_timeout(self, topic_admin, topic_manager_factory):
        topic_manager = topic_manager_factory()
        create = topic_manager.topic(name="create_me_timeout")
        with pytest.raises(CreateTopicTimeout) as e:
            topic_admin.create_topics([create], finalize_timeout=0)

        error_str = str(e.value.args[0])
        assert create.name in error_str

    def test_create_topics_already_exist(
        self, topic_admin, topic_manager_factory, topic_factory, caplog
    ):
        topic_name, _ = topic_factory()

        topic_manager = topic_manager_factory()
        existing_topic = topic_manager.topic(name=topic_name)

        with caplog.at_level(level=logging.INFO), patch.object(
            topic_admin, "list_topics"
        ) as list_topics_mock:
            # Mock "list_topics" call to simulate a topic being created
            # simultaneously by multiple instances
            list_topics_mock.return_value = {}
            topic_admin.create_topics([existing_topic])

        assert f'Topic "{existing_topic.name}" already exists' in caplog.text

    def test_create_topics_invalid_config(self, topic_admin, topic_manager_factory):
        topic_manager = topic_manager_factory()
        invalid_topic = topic_manager.topic(
            name=str(uuid4()),
            config=topic_manager.topic_config(extra_config={"bad_option": "not_real"}),
        )

        with pytest.raises(CreateTopicFailure, match="Unknown topic config name"):
            topic_admin.create_topics([invalid_topic])
