import pytest
import uuid

from typing import Optional
from unittest.mock import patch

from quixstreams.kafka.admin import Admin
from quixstreams.state.changelog import ChangelogWriter


@pytest.fixture()
def changelog_writer_factory(changelog_manager_factory):
    """
    Makes a changelog manager.

    If admin is passed, will also create changelog for you
    """

    def factory(
        source_topic_name: str = str(uuid.uuid4()),
        suffix: str = "suffix",
        partition_num: int = 0,
        admin: Optional[Admin] = None,
    ):
        changelog_manager = changelog_manager_factory(admin=admin)
        topic_manager = changelog_manager._topic_manager

        kwargs = dict(source_topic_name=source_topic_name, suffix=suffix)
        topic_manager.topic(
            source_topic_name
        )  # changelogs depend on topic objects existing
        changelog_topic = topic_manager.changelog_topic(
            **kwargs, consumer_group="group"
        )
        if admin:
            topic_manager.create_topics([changelog_topic])
        return changelog_manager.get_writer(**kwargs, partition_num=partition_num)

    return factory


@pytest.fixture()
def changelog_writer_patched(changelog_writer_factory):
    writer = changelog_writer_factory()
    with patch.object(writer, "produce"):
        yield writer


@pytest.fixture()
def changelog_writer_with_changelog(changelog_writer_factory, admin):
    return changelog_writer_factory(admin=admin)
