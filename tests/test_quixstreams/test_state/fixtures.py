import pytest
import uuid

from typing import Optional
from unittest.mock import patch, create_autospec

from quixstreams.kafka import Consumer
from quixstreams.models.topics import TopicAdmin
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
        topic_admin: Optional[TopicAdmin] = None,
    ):
        changelog_manager = changelog_manager_factory(topic_admin=topic_admin)
        topic_manager = changelog_manager._topic_manager

        topic_manager.topic(topic_name)  # changelogs depend on topic objects existing
        changelog_topic = topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name, consumer_group="group"
        )
        if topic_admin:
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
def changelog_writer_with_changelog(changelog_writer_factory, topic_admin):
    return changelog_writer_factory(topic_admin=topic_admin)


@pytest.fixture()
def recovery_partition_store_mock():
    store = create_autospec(StorePartition)()
    store.get_changelog_offset.return_value = 15
    recovery_partition = RecoveryPartition(
        changelog_name=f"changelog__{str(uuid.uuid4())}",
        partition_num=0,
        store_partition=store,
    )
    recovery_partition._changelog_lowwater = 10
    recovery_partition._changelog_highwater = 20
    return recovery_partition


@pytest.fixture()
def recovery_partition_factory():
    """Mocks a StorePartition if none provided"""

    def factory(
        changelog_name: str = str(uuid.uuid4()),
        partition_num: int = 0,
        mocked_changelog_offset: Optional[int] = 15,
        lowwater: Optional[int] = None,
        highwater: Optional[int] = None,
        store_partition: Optional[StorePartition] = None,
    ):
        if not store_partition:
            store_partition = create_autospec(StorePartition)()
            store_partition.get_changelog_offset.return_value = mocked_changelog_offset
        recovery_partition = RecoveryPartition(
            changelog_name=changelog_name,
            partition_num=partition_num,
            store_partition=store_partition,
        )
        if lowwater:
            recovery_partition._changelog_lowwater = lowwater
        if highwater:
            recovery_partition._changelog_highwater = highwater
        return recovery_partition

    return factory


@pytest.fixture()
def recovery_manager_mock_consumer():
    return RecoveryManager(
        consumer=create_autospec(Consumer)("broker", "group", "latest")
    )
