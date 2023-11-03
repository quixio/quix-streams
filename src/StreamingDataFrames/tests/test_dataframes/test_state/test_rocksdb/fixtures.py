import uuid
from typing import Optional

import pytest

from streamingdataframes.state.rocksdb import RocksDBStore
from streamingdataframes.state.rocksdb.options import RocksDBOptions
from streamingdataframes.state.rocksdb.partition import RocksDBStorePartition


@pytest.fixture()
def rocksdb_partition_factory(tmp_path):
    def factory(
        name: str = "db",
        options: Optional[RocksDBOptions] = None,
        open_max_retries: int = 0,
        open_retry_backoff: float = 3.0,
    ) -> RocksDBStorePartition:
        path = (tmp_path / name).as_posix()
        return RocksDBStorePartition(
            path,
            options=options,
            open_max_retries=open_max_retries,
            open_retry_backoff=open_retry_backoff,
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
