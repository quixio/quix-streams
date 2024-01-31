from typing import Optional, cast

from .partition import WindowedRocksDBStorePartition
from .transaction import WindowedRocksDBPartitionTransaction
from ..store import RocksDBStore
from ..types import RocksDBOptionsType
from ...recovery import ChangelogManager


class WindowedRocksDBStore(RocksDBStore):
    def __init__(
        self,
        name: str,
        topic: str,
        base_dir: str,
        changelog_manager: Optional[ChangelogManager] = None,
        options: Optional[RocksDBOptionsType] = None,
    ):
        super().__init__(
            name=name,
            topic=topic,
            base_dir=base_dir,
            changelog_manager=changelog_manager,
            options=options,
        )

    def create_new_partition(self, path: str) -> WindowedRocksDBStorePartition:
        db_partition = WindowedRocksDBStorePartition(
            path=path,
            options=self._options,
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
