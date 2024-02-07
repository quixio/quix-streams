from typing import Optional, cast

from .partition import WindowedRocksDBStorePartition
from .transaction import WindowedRocksDBPartitionTransaction
from ..store import RocksDBStore
from ..types import RocksDBOptionsType
from ...recovery import ChangelogProducerFactory, ChangelogProducer


class WindowedRocksDBStore(RocksDBStore):
    """
    RocksDB-based windowed state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(
        self,
        name: str,
        topic: str,
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[RocksDBOptionsType] = None,
    ):
        """
        :param name: a unique store name
        :param topic: a topic name for this store
        :param base_dir: path to a directory with the state
        :param changelog_producer_factory: a ChangelogProducerFactory instance
            if using changelogs
        :param options: RocksDB options. If `None`, the default options will be used.
        """
        super().__init__(
            name=name,
            topic=topic,
            base_dir=base_dir,
            changelog_producer_factory=changelog_producer_factory,
            options=options,
        )

    def create_new_partition(
        self, path: str, changelog_producer: Optional[ChangelogProducer] = None
    ) -> WindowedRocksDBStorePartition:
        db_partition = WindowedRocksDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
        return db_partition

    def assign_partition(self, partition: int) -> WindowedRocksDBStorePartition:
        return cast(
            WindowedRocksDBStorePartition, super().assign_partition(partition=partition)
        )

    def start_partition_transaction(
        self, partition: int
    ) -> WindowedRocksDBPartitionTransaction:
        return cast(
            WindowedRocksDBPartitionTransaction,
            super().start_partition_transaction(partition=partition),
        )
