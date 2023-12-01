import uuid
from typing import Optional

import pytest

from quixstreams.state.rocksdb.windowed.store import WindowedRocksDBStore


@pytest.fixture()
def windowed_rocksdb_store_factory(tmp_path):
    def factory(
        topic: Optional[str] = None, name: str = "default", grace_period: float = 0.0
    ) -> WindowedRocksDBStore:
        topic = topic or str(uuid.uuid4())
        return WindowedRocksDBStore(
            topic=topic, name=name, base_dir=str(tmp_path), grace_period=grace_period
        )

    return factory


class TestWindowedRocksDBStore:
    def test_store(self, windowed_rocksdb_store_factory):
        store = windowed_rocksdb_store_factory()
        store.assign_partition(0)
        with store.start_partition_transaction(0, key=b"__key__") as tx:
            tx.update_window(start=0, end=10, value=1, timestamp=2)
            tx.update_window(start=2, end=12, value=1, timestamp=3)
            tx.update_window(start=4, end=14, value=1, timestamp=4)
            assert tx.get_window(start=0, end=10) == 1

        with store.start_partition_transaction(0, key=b"__key1__") as tx:
            tx.update_window(10, 20, value=1, timestamp=16)

        with store.start_partition_transaction(0, key=b"__key__") as tx:
            expired_windows = tx.get_expired_windows()
            assert len(expired_windows) == 3
            assert tx.get_window(0, 10) == 1

        with store.start_partition_transaction(0, key=b"__key__") as tx:
            assert tx.get_window(0, 10) is None

        store.revoke_partition(0)

        partition = store.assign_partition(0)
