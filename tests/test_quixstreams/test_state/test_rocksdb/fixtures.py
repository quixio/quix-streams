import uuid
from typing import Optional

import pytest

from quixstreams.state.rocksdb import RocksDBStore
from quixstreams.state.rocksdb.options import RocksDBOptions
from quixstreams.state.rocksdb.partition import RocksDBStorePartition


@pytest.fixture()
def rocksdb_partition_factory(tmp_path):
    def factory(
        name: str = "db",
        options: Optional[RocksDBOptions] = None,
    ) -> RocksDBStorePartition:
        path = (tmp_path / name).as_posix()
        _options = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        return RocksDBStorePartition(
            path,
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
    def factory(topic: Optional[str] = None, name: str = "default") -> RocksDBStore:
        topic = topic or str(uuid.uuid4())
        return RocksDBStore(topic=topic, name=name, base_dir=str(tmp_path))

    return factory


@pytest.fixture()
def rocksdb_store(rocksdb_store_factory) -> RocksDBStore:
    store = rocksdb_store_factory()
    yield store
    store.close()
