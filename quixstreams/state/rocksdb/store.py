import logging
from pathlib import Path
from typing import Optional

from quixstreams.state.base import Store
from quixstreams.state.recovery import ChangelogProducer, ChangelogProducerFactory

from .partition import (
    RocksDBStorePartition,
)
from .types import RocksDBOptionsType

logger = logging.getLogger(__name__)

__all__ = ("RocksDBStore",)


class RocksDBStore(Store):
    """
    RocksDB-based state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[RocksDBOptionsType] = None,
    ):
        """
        :param name: a unique store name
        :param stream_id: a topic name for this store
        :param base_dir: path to a directory with the state
        :param changelog_producer_factory: a ChangelogProducerFactory instance
            if using changelogs
        :param options: RocksDB options. If `None`, the default options will be used.
        """
        super().__init__(name, stream_id)

        partitions_dir = Path(base_dir).absolute() / self._name
        if self._stream_id:
            partitions_dir = partitions_dir / self._stream_id

        self._partitions_dir = partitions_dir
        self._changelog_producer_factory = changelog_producer_factory
        self._options = options

    def create_new_partition(
        self,
        partition: int,
    ) -> RocksDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())

        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return RocksDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
