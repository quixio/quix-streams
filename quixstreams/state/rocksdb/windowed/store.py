from typing import Optional

from ...recovery import ChangelogProducer, ChangelogProducerFactory
from ..store import RocksDBStore
from ..types import RocksDBOptionsType
from .partition import WindowedRocksDBStorePartition


class WindowedRocksDBStore(RocksDBStore):
    """
    RocksDB-based windowed state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(
        self,
        name: str,
        stream_id: str,
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
        super().__init__(
            name=name,
            stream_id=stream_id,
            base_dir=base_dir,
            changelog_producer_factory=changelog_producer_factory,
            options=options,
        )

    def create_new_partition(self, partition) -> WindowedRocksDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())

        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return WindowedRocksDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
