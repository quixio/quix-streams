import uuid
from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.kafka import Consumer
from quixstreams.models import TopicManager
from quixstreams.state.recovery import RecoveryPartition, RecoveryManager
from quixstreams.state.types import StorePartition


@pytest.fixture()
def recovery_manager_factory(topic_manager_factory):
    def factory(
        topic_manager: Optional[TopicManager] = None,
        consumer: Optional[Consumer] = None,
    ) -> RecoveryManager:
        topic_manager = topic_manager or topic_manager_factory()
        consumer = consumer or MagicMock(Consumer)
        return RecoveryManager(topic_manager=topic_manager, consumer=consumer)

    return factory


@pytest.fixture()
def recovery_partition_factory():
    """Mocks a StorePartition if none provided"""

    def factory(
        changelog_name: str = "",
        partition_num: int = 0,
        store_partition: Optional[StorePartition] = None,
        committed_offset: int = -1001,
    ):
        changelog_name = changelog_name or f"changelog__{str(uuid.uuid4())}"
        if not store_partition:
            store_partition = MagicMock(spec_set=StorePartition)
        recovery_partition = RecoveryPartition(
            changelog_name=changelog_name,
            partition_num=partition_num,
            store_partition=store_partition,
            committed_offset=committed_offset,
        )
        return recovery_partition

    return factory
