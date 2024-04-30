import logging
from contextlib import ExitStack
from unittest.mock import patch, PropertyMock
from uuid import uuid4

import pytest
from confluent_kafka.admin import TopicMetadata

from quixstreams.models.topics import TopicConfig, TopicAdmin
from quixstreams.models.topics.admin import confluent_topic_config, convert_topic_list
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

    def test_inspect_topics_timeout(self, topic_manager_factory, topic_admin):
        """
        Confirm timeout argument passthrough.
        """
        timeout = 8.5
        topic_manager = topic_manager_factory(topic_admin)
        topic = topic_manager.topic(name=str(uuid4()))
        topic_manager.create_topics([topic])

        list_topics_result = topic_admin.list_topics()
        describe_topics = [confluent_topic_config(topic.name)]
        describe_configs_result = topic_admin.admin_client.describe_configs(
            describe_topics
        )

        stack = ExitStack()
        # mocking a property like this allows mocking its respective methods.
        stack.enter_context(
            patch.object(
                TopicAdmin,
                "admin_client",
                new_callable=PropertyMock,
                return_value=topic_admin._inner_admin,
            )
        )
        describe_configs = stack.enter_context(
            patch.object(
                topic_admin.admin_client,
                "describe_configs",
                return_value=describe_configs_result,
            )
        )
        list_topics = stack.enter_context(
            patch.object(topic_admin, "list_topics", return_value=list_topics_result)
        )

        topic_admin.inspect_topics([topic.name], timeout=timeout)
        list_topics.assert_called_with(timeout=timeout)
        describe_configs.assert_called_with(describe_topics, request_timeout=timeout)
        stack.close()

    def test_create_topics(self, topic_admin, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(name=str(uuid4()))
        topic2 = topic_manager.topic(name=str(uuid4()))

        topic_admin.create_topics([topic1, topic2])

        topics = topic_admin.list_topics()
        assert topic1.name in topics
        assert topic2.name in topics

    def test_create_topics_timeout(self, topic_admin, topic_manager_factory):
        """
        Confirm timeout argument passthrough.
        """
        timeout = 8.5
        finalize_timeout = 9.5
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(name=str(uuid4()))
        create_topics_result = {topic.name: "create_topics_result"}

        stack = ExitStack()
        stack.enter_context(
            patch.object(
                TopicAdmin,
                "admin_client",
                new_callable=PropertyMock,
                return_value=topic_admin._inner_admin,
            )
        )
        list_topics = stack.enter_context(patch.object(topic_admin, "list_topics"))
        create_topics = stack.enter_context(
            patch.object(
                topic_admin.admin_client,
                "create_topics",
                return_value=create_topics_result,
            )
        )
        _finalize_create = stack.enter_context(
            patch.object(topic_admin, "_finalize_create")
        )
        topic_admin.create_topics(
            [topic], timeout=timeout, finalize_timeout=finalize_timeout
        )

        list_topics.assert_called_with(timeout=timeout)
        create_topics.assert_called_with(
            convert_topic_list([topic]), request_timeout=timeout
        )
        _finalize_create.assert_called_with(
            create_topics_result, finalize_timeout=finalize_timeout
        )
        stack.close()

    def test_create_topics_finalize_timeout(self, topic_admin, topic_manager_factory):
        """
        Finalize timeout raises as expected (not a request-based timeout).
        """
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
