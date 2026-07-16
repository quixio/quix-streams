import logging
from datetime import timedelta
from typing import Optional

from quixstreams.state.base import Store
from quixstreams.state.recovery import ChangelogProducer, ChangelogProducerFactory

from .partition import _DEFAULT_MAX_EVICTIONS_PER_FLUSH, MemoryStorePartition

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
        legacy_records_ttl: Optional[timedelta] = None,
        ttl_changelog_tombstones: bool = True,
        max_evictions_per_flush: int = _DEFAULT_MAX_EVICTIONS_PER_FLUSH,
    ) -> None:
        """
        :param name: a unique store name
        :param stream_id: a topic name for this store
        :param changelog_producer_factory: a ChangelogProducerFactory instance
            if using changelogs topics.
        :param legacy_records_ttl: uniform expiry for leftover legacy records
            completed during a MIXED-changelog recovery (parity with
            ``RocksDBOptions.legacy_records_ttl``). Forwarded to each
            ``MemoryStorePartition``.
        :param ttl_changelog_tombstones: whether TTL evictions are produced to the
            changelog as tombstones (parity with
            ``RocksDBOptions.ttl_changelog_tombstones``).
        :param max_evictions_per_flush: cap on TTL-driven evictions per flush
            (parity with ``RocksDBOptions.max_evictions_per_flush``).
        """
        super().__init__(name, stream_id)

        self._changelog_producer_factory = changelog_producer_factory
        self._legacy_records_ttl = legacy_records_ttl
        self._ttl_changelog_tombstones = ttl_changelog_tombstones
        self._max_evictions_per_flush = max_evictions_per_flush

    def create_new_partition(self, partition: int) -> MemoryStorePartition:
        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return MemoryStorePartition(
            changelog_producer,
            max_evictions_per_flush=self._max_evictions_per_flush,
            legacy_records_ttl=self._legacy_records_ttl,
            ttl_changelog_tombstones=self._ttl_changelog_tombstones,
        )
