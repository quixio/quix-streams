import uuid
from typing import Optional

from quixstreams.state.recovery import (
    ChangelogProducer,
    ChangelogProducerFactory,
)
from quixstreams.state.rocksdb import (
    RocksDBOptions,
    RocksDBStore,
    RocksDBStorePartition,
)

__all__ = (
    "rocksdb_store_factory",
    "rocksdb_partition_factory",
)


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
