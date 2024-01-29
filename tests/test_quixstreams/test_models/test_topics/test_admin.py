import logging
from unittest.mock import patch
from uuid import uuid4

import pytest
from confluent_kafka.admin import TopicMetadata

from quixstreams.models.topics import TopicConfig
from quixstreams.models.topics.exceptions import CreateTopicTimeout, CreateTopicFailure


logger = logging.getLogger(__name__)


def test_list_topics(topic_admin, topic_factory):
    topic_name, _ = topic_factory()
    result = topic_admin.list_topics()

    assert isinstance(result, dict)
    assert isinstance(result[topic_name], TopicMetadata)


def test_inspect_topics(topic_admin, topic_factory):
    topic_name, _ = topic_factory()
    not_a_topic = "non-existent-topic-name"
    result = topic_admin.inspect_topics([topic_name, not_a_topic])

    assert isinstance(result, dict)
    assert isinstance(result[topic_name], TopicConfig)
    assert result[not_a_topic] is None


def test_create_topics(topic_admin, topic_factory, topic_manager_factory):
    """
    Confirm topic create happy paths.

    2 topics will already exist ("skip", "ignore"), but we will pretend that "ignore"
    does not actually exist and attempt to create it.

    1 will have a normal config that should succeed ("create")
    """
    exist1, _ = topic_factory()
    exist2, _ = topic_factory()

    topic_manager = topic_manager_factory()
    skip = topic_manager.topic(name=exist1)
    ignore = topic_manager.topic(name=exist2)
    create = topic_manager.topic(name=str(uuid4()))

    with patch.object(topic_admin, "list_topics") as list_topics:
        # mock "ignore" topic being created simultaneously by another instance
        list_topics.return_value = {skip.name: "Metadata"}
        topic_admin.create_topics([create, ignore])
    assert create.name in topic_admin.list_topics()

    topic_admin.create_topics([])  # does nothing, basically


def test_create_topics_finalize_timeout(topic_admin, topic_manager_factory):
    topic_manager = topic_manager_factory()
    create = topic_manager.topic(name="create_me_timeout")
    with pytest.raises(CreateTopicTimeout) as e:
        topic_admin.create_topics([create], finalize_timeout=0)

    error_str = str(e.value.args[0])
    assert create.name in error_str


def test_create_topics_failure(
    topic_admin, topic_manager_factory, topic_factory, caplog
):
    """
    Cover all other cases of topic creation.

    We test with 4 total topics, 1 should successfully create via Admin.

    2 topics will already exist ("skip", "ignore"), but we will pretend that "ignore"
    does not actually exist and attempt to create it.

    1 will have a normal config that should succeed ("create")

    1 will have a bad config value and should not create ("has_errors"), which
    ultimately leads to the Exception being thrown after collecting the results of all
    creation attempts.

    We also use caplog to inspect the logs and confirm results are as expected.
    """
    exist1, _ = topic_factory()
    exist2, _ = topic_factory()

    topic_manager = topic_manager_factory()
    skip = topic_manager.topic(name=exist1)
    ignore = topic_manager.topic(name=exist2)
    create = topic_manager.topic(
        name=str(uuid4()),
        config=topic_manager.topic_config(extra_config={"retention.ms": "600000"}),
    )
    has_errors = topic_manager.topic(
        name=str(uuid4()),
        config=topic_manager.topic_config(extra_config={"bad_option": "not_real"}),
    )

    with pytest.raises(CreateTopicFailure) as e:
        with caplog.at_level(level=logging.INFO):
            with patch.object(topic_admin, "list_topics") as list_topics:
                # mock "ignore" topic being created simultaneously by another instance
                list_topics.return_value = {skip.name: "Metadata"}
                topic_admin.create_topics([create, ignore, has_errors])

    error_str = str(e.value.args[0])
    for expected in [has_errors.name, "bad_option", "not_real"]:
        assert expected in error_str

    assert f"created topics: ['{create.name}']" in caplog.text
    assert f"already exist: ['{ignore.name}']" in caplog.text

    cluster_topic_info = topic_admin.inspect_topics([create.name, has_errors.name])
    assert cluster_topic_info[has_errors.name] is None
    expected_topic_config = create.config
    actual_topic_config = cluster_topic_info[create.name]
    actual_topic_config.update_extra_config(
        allowed=list(expected_topic_config.extra_config.keys())
    )
    assert expected_topic_config == actual_topic_config
