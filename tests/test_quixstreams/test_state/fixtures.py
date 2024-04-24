import uuid
from typing import Optional
from unittest.mock import create_autospec

import pytest

from quixstreams.kafka import Consumer
from quixstreams.state.recovery import RecoveryPartition, RecoveryManager
from quixstreams.state.types import StorePartition


@pytest.fixture()
def recovery_manager_factory(topic_manager_factory):
    def factory(consumer: Consumer) -> RecoveryManager:
        return RecoveryManager(topic_manager=topic_manager_factory(), consumer=consumer)

    return factory


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
def recovery_manager_mock_consumer(topic_manager_factory):
    return RecoveryManager(
        consumer=create_autospec(Consumer)("broker", "group", "latest"),
        topic_manager=topic_manager_factory(),
    )
