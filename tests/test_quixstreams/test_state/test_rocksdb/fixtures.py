import uuid
from typing import Optional
from unittest.mock import create_autospec

import pytest

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.state import ChangelogProducer, ChangelogProducerFactory
from quixstreams.state.rocksdb import RocksDBStore
from quixstreams.state.rocksdb.options import RocksDBOptions
from quixstreams.state.rocksdb.partition import (
    RocksDBStorePartition,
    RocksDBPartitionRecoveryTransaction,
)


@pytest.fixture()
def rocksdb_partition_factory(tmp_path):
    def factory(
        name: str = "db",
        options: Optional[RocksDBOptions] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> RocksDBStorePartition:
        path = (tmp_path / name).as_posix()
        _options = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        if not changelog_producer:
            changelog_producer = create_autospec(ChangelogProducer)(
                "topic", "partition", "producer"
            )
        return RocksDBStorePartition(
            path,
            changelog_producer=changelog_producer,
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
def partition_recovery_transaction_factory(rocksdb_partition):
    def factory(
        changelog_message: ConfluentKafkaMessageProto,
        store_partition: Optional[RocksDBStorePartition] = rocksdb_partition,
    ):
        return RocksDBPartitionRecoveryTransaction(
            partition=store_partition, changelog_message=changelog_message
        )

    return factory
