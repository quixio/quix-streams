import uuid
from typing import Optional
from unittest.mock import create_autospec

import pytest

from quixstreams.rowproducer import RowProducer
from quixstreams.state.recovery import ChangelogProducerFactory, ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.windowed.partition import WindowedRocksDBStorePartition
from quixstreams.state.rocksdb.windowed.store import WindowedRocksDBStore


@pytest.fixture()
def windowed_rocksdb_store_factory(tmp_path):
    def factory(
        topic: Optional[str] = None,
        name: str = "default",
    ) -> WindowedRocksDBStore:
        topic = topic or str(uuid.uuid4())
        return WindowedRocksDBStore(
            topic=topic,
            name=name,
            base_dir=str(tmp_path),
        )

    return factory


@pytest.fixture()
def windowed_rocksdb_partition_factory(tmp_path):
    def factory(
        name: str = "db",
        options: Optional[RocksDBOptions] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> WindowedRocksDBStorePartition:
        path = (tmp_path / name).as_posix()
        _options = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        if not changelog_producer:
            changelog_producer = create_autospec(ChangelogProducer)(
                "topic", "partition", "producer"
            )
        return WindowedRocksDBStorePartition(
            path,
            changelog_producer=changelog_producer,
            options=_options,
        )

    return factory


@pytest.fixture()
def windowed_rocksdb_store_factory_changelog(tmp_path, changelog_producer_mock):
    def factory(
        topic: Optional[str] = None,
        changelog: Optional[str] = None,
        name: str = "default",
        producer: Optional[RowProducer] = None,
    ) -> WindowedRocksDBStore:
        topic = topic or str(uuid.uuid4())
        return WindowedRocksDBStore(
            topic=topic,
            name=name,
            base_dir=str(tmp_path),
            changelog_producer_factory=ChangelogProducerFactory(
                changelog_name=changelog or str(uuid.uuid4()),
                source_topic_name=topic,
                producer=producer or create_autospec(RowProducer)("address"),
            ),
        )

    return factory
