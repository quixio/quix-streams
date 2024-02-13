import uuid
from typing import Optional
from unittest.mock import create_autospec

import pytest

from quixstreams.rowproducer import RowProducer
from quixstreams.state.recovery import ChangelogProducerFactory
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
def windowed_rocksdb_store_factory_changelog(tmp_path):
    def factory(
        topic: Optional[str] = None,
        changelog: Optional[str] = None,
        name: str = "default",
        producer: Optional[RowProducer] = None,
    ) -> WindowedRocksDBStore:
        return WindowedRocksDBStore(
            topic=topic or str(uuid.uuid4()),
            name=name,
            base_dir=str(tmp_path),
            changelog_producer_factory=ChangelogProducerFactory(
                changelog_name=changelog or str(uuid.uuid4()),
                producer=producer or create_autospec(RowProducer)("address"),
            ),
        )

    return factory
