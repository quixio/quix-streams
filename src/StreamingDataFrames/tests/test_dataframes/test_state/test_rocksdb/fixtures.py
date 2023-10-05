from typing import Optional

import pytest

from streamingdataframes.state.rocksdb import RocksDBStorage, RocksDBOptions
from streamingdataframes.state.rocksdb.serialization import DumpsFunc, LoadsFunc


@pytest.fixture()
def rocksdb_storage_factory(tmp_path):
    def factory(
        name: str = "db",
        options: Optional[RocksDBOptions] = None,
        open_max_retries: int = 0,
        open_retry_backoff: float = 3.0,
        dumps: Optional[DumpsFunc] = None,
        loads: Optional[LoadsFunc] = None,
    ) -> RocksDBStorage:
        path = (tmp_path / name).as_posix()
        return RocksDBStorage(
            path,
            options=options,
            open_max_retries=open_max_retries,
            open_retry_backoff=open_retry_backoff,
            dumps=dumps,
            loads=loads,
        )

    return factory


@pytest.fixture()
def rocksdb_storage(rocksdb_storage_factory) -> RocksDBStorage:
    storage = rocksdb_storage_factory()
    yield storage
    storage.close()
