import logging
from typing import Optional

from quixstreams.state.base import Store
from quixstreams.state.recovery import ChangelogProducer, ChangelogProducerFactory

from .partition import MemoryStorePartition

logger = logging.getLogger(__name__)

__all__ = ("MemoryStore",)


class MemoryStore(Store):
    """
    In-memory state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.

    Requires a full state recovery for each partition on assignment.
    """

    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
    ) -> None:
        """
        :param name: a unique store name
        :param stream_id: a topic name for this store
        :param changelog_producer_factory: a ChangelogProducerFactory instance
            if using changelogs topics.
        """
        super().__init__(name, stream_id)

        self._changelog_producer_factory = changelog_producer_factory

    def create_new_partition(self, partition: int) -> MemoryStorePartition:
        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return MemoryStorePartition(changelog_producer)
