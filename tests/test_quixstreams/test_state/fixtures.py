import uuid
from typing import Generator, Optional
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.kafka import Consumer
from quixstreams.models import TopicManager
from quixstreams.state.base import StorePartition
from quixstreams.state.recovery import (
    ChangelogProducer,
    ChangelogProducerFactory,
    RecoveryManager,
    RecoveryPartition,
)
from quixstreams.state.rocksdb import (
    RocksDBOptions,
    RocksDBStore,
    RocksDBStorePartition,
)


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


@pytest.fixture()
def store_type(request):
    if hasattr(request, "param"):
        return request.param
    else:
        return RocksDBStore


def rocksdb_store_factory(tmp_path):
    def factory(
        topic: Optional[str] = None,
        name: str = "default",
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
    ) -> RocksDBStore:
        topic = topic or str(uuid.uuid4())
        return RocksDBStore(
            topic=topic,
            name=name,
            base_dir=str(tmp_path),
            changelog_producer_factory=changelog_producer_factory,
        )

    return factory


@pytest.fixture()
def store_factory(store_type, tmp_path):
    if store_type == RocksDBStore:
        return rocksdb_store_factory(tmp_path)
    else:
        raise ValueError(f"invalid store type {store_type}")


@pytest.fixture()
def store(store_factory):
    store = store_factory()
    yield store
    store.close()


def rocksdb_partition_factory(tmp_path, changelog_producer_mock):
    def factory(
        name: str = "db",
        options: Optional[RocksDBOptions] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> RocksDBStorePartition:
        path = (tmp_path / name).as_posix()
        _options = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        return RocksDBStorePartition(
            path,
            changelog_producer=changelog_producer or changelog_producer_mock,
            options=_options,
        )

    return factory


@pytest.fixture()
def store_partition_factory(store_type, tmp_path, changelog_producer_mock):
    if store_type == RocksDBStore:
        return rocksdb_partition_factory(tmp_path, changelog_producer_mock)
    else:
        raise ValueError(f"invalid store type {store_type}")


@pytest.fixture()
def store_partition(store_partition_factory) -> Generator[StorePartition, None, None]:
    partition = store_partition_factory()
    yield partition
    partition.close()


@pytest.fixture()
def changelog_producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer
