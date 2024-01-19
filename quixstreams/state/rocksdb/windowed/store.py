from typing import Optional, cast

from .partition import WindowedRocksDBStorePartition
from .transaction import WindowedRocksDBPartitionTransaction
from ..store import RocksDBStore
from ..types import RocksDBOptionsType


class WindowedRocksDBStore(RocksDBStore):
    def __init__(
        self,
        name: str,
        topic: str,
        base_dir: str,
        options: Optional[RocksDBOptionsType] = None,
        open_max_retries: int = 10,
        open_retry_backoff: float = 3.0,
    ):
        super().__init__(
            name=name,
            topic=topic,
            base_dir=base_dir,
            options=options,
            open_max_retries=open_max_retries,
            open_retry_backoff=open_retry_backoff,
        )

    def create_new_partition(self, path: str) -> WindowedRocksDBStorePartition:
        db_partition = WindowedRocksDBStorePartition(
            path=path,
            options=self._options,
            open_max_retries=self._open_max_retries,
            open_retry_backoff=self._open_retry_backoff,
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
