import uuid
from typing import Optional
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state import ChangelogProducer, ChangelogProducerFactory
from quixstreams.state.rocksdb import RocksDBStore
from quixstreams.state.rocksdb.options import RocksDBOptions
from quixstreams.state.rocksdb.partition import RocksDBStorePartition


@pytest.fixture()
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
def rocksdb_partition(rocksdb_partition_factory) -> RocksDBStorePartition:
    partition = rocksdb_partition_factory()
    yield partition
    partition.close()


@pytest.fixture()
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
def rocksdb_store(rocksdb_store_factory) -> RocksDBStore:
    store = rocksdb_store_factory()
    yield store
    store.close()


@pytest.fixture()
def changelog_producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer
