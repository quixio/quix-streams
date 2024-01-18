import uuid
from typing import Optional

import pytest

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
