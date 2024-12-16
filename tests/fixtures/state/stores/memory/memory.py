import uuid
from typing import Optional

from quixstreams.state.memory import MemoryStore, MemoryStorePartition
from quixstreams.state.recovery import (
    ChangelogProducer,
    ChangelogProducerFactory,
)

__all__ = (
    "memory_store_factory",
    "memory_partition_factory",
)


def memory_store_factory():
    def factory(
        topic: Optional[str] = None,
        name: str = "default",
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
    ):
        return MemoryStore(
            topic=topic or str(uuid.uuid4()),
            name=name,
            changelog_producer_factory=changelog_producer_factory,
        )

    return factory


def memory_partition_factory(changelog_producer_mock):
    def factory(
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        return MemoryStorePartition(
            changelog_producer=changelog_producer or changelog_producer_mock,
        )

    return factory
