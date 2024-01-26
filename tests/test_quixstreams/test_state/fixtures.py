import pytest
import uuid

from typing import Optional
from unittest.mock import patch, create_autospec

from quixstreams.kafka import Admin, Consumer
from quixstreams.state.recovery import RecoveryPartition, RecoveryManager
from quixstreams.state.types import StorePartition


@pytest.fixture()
def changelog_writer_factory(changelog_manager_factory):
    """
    Makes a changelog manager.

    If admin is passed, will also create changelog for you
    """

    def factory(
        topic_name: str = str(uuid.uuid4()),
        store_name: str = "store_name",
        partition_num: int = 0,
        admin: Optional[Admin] = None,
    ):
        changelog_manager = changelog_manager_factory(admin=admin)
        topic_manager = changelog_manager._topic_manager

        topic_manager.topic(topic_name)  # changelogs depend on topic objects existing
        changelog_topic = topic_manager.changelog_topic(
            topic_name=topic_name, suffix=store_name, consumer_group="group"
        )
        if admin:
            topic_manager.create_topics([changelog_topic])
        return changelog_manager.get_writer(
            topic_name=topic_name, store_name=store_name, partition_num=partition_num
        )

    return factory


@pytest.fixture()
def changelog_writer_patched(changelog_writer_factory):
    writer = changelog_writer_factory()
    with patch.object(writer, "produce"):
        yield writer


@pytest.fixture()
def changelog_writer_with_changelog(changelog_writer_factory, admin):
    return changelog_writer_factory(admin=admin)


@pytest.fixture()
def recovery_partition_store_mock(rocksdb_store_factory):
    topic_name = str(uuid.uuid4())
    store = create_autospec(StorePartition)()
    store.get_changelog_offset.return_value = 15
    recovery_partition = RecoveryPartition(
        topic_name=topic_name,
        changelog_name=f"changelog__{topic_name}",
        partition_num=0,
        store_partition=store,
    )
    recovery_partition._changelog_lowwater = 10
    recovery_partition._changelog_highwater = 20
    return recovery_partition


@pytest.fixture()
def recovery_manager_mock_consumer():
    return RecoveryManager(
        consumer=create_autospec(Consumer)("broker", "group", "latest")
    )
