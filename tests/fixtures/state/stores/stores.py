from typing import Generator

import pytest

from quixstreams.state.base import StorePartition
from quixstreams.state.memory import MemoryStore
from quixstreams.state.rocksdb import RocksDBStore

from .memory import memory_partition_factory, memory_store_factory
from .rocksdb import rocksdb_partition_factory, rocksdb_store_factory


@pytest.fixture()
def store_type(request):
    if hasattr(request, "param"):
        return request.param
    else:
        return RocksDBStore


@pytest.fixture()
def store_factory(store_type, tmp_path):
    if store_type == RocksDBStore:
        return rocksdb_store_factory(tmp_path)
    elif store_type == MemoryStore:
        return memory_store_factory()
    else:
        raise ValueError(f"invalid store type {store_type}")


@pytest.fixture()
def store(store_factory):
    store = store_factory()
    yield store
    store.close()


@pytest.fixture()
def store_partition_factory(store_type, tmp_path, changelog_producer_mock):
    if store_type == RocksDBStore:
        return rocksdb_partition_factory(tmp_path, changelog_producer_mock)
    elif store_type == MemoryStore:
        return memory_partition_factory(changelog_producer_mock)
    else:
        raise ValueError(f"invalid store type {store_type}")


@pytest.fixture()
def store_partition(store_partition_factory) -> Generator[StorePartition, None, None]:
    partition = store_partition_factory()
    yield partition
    partition.close()
