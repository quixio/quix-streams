import logging
from typing import Optional

from rocksdict import RdictItems, ReadOptions, WriteBatch  # type: ignore

from quixstreams.state.base import PartitionTransactionCache
from quixstreams.state.recovery import ChangelogProducer

from ..exceptions import ColumnFamilyDoesNotExist
from ..partition import RocksDBStorePartition
from ..types import RocksDBOptionsType
from .metadata import (
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_TIMESTAMPS_CF_NAME,
)
from .transaction import WindowedRocksDBPartitionTransaction

logger = logging.getLogger(__name__)


class WindowedRocksDBStorePartition(RocksDBStorePartition):
    """
    A base class to access windowed state in RocksDB.
    It represents a single RocksDB database.

    Besides the data, it keeps track of the latest observed timestamp and
    stores the expiration index to delete expired windows.

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        super().__init__(
            path=path, options=options, changelog_producer=changelog_producer
        )
        self._ensure_column_family(LATEST_EXPIRED_WINDOW_CF_NAME)
        self._ensure_column_family(LATEST_TIMESTAMPS_CF_NAME)

    def iter_items(
        self, from_key: bytes, read_opt: ReadOptions, cf_name: str = "default"
    ) -> RdictItems:
        cf = self.get_column_family(cf_name=cf_name)
        return cf.items(from_key=from_key, read_opt=read_opt)

    def begin(self) -> "WindowedRocksDBPartitionTransaction":
        return WindowedRocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def _ensure_column_family(self, cf_name: str):
        try:
            self.get_column_family(cf_name)
        except ColumnFamilyDoesNotExist:
            self.create_column_family(cf_name)

    def write(
        self,
        cache: PartitionTransactionCache,
        processed_offset: Optional[int],
        changelog_offset: Optional[int],
        batch: Optional[WriteBatch] = None,
        latest_timestamp_ms: Optional[int] = None,
    ):
        batch = WriteBatch(raw_mode=True)

        super().write(
            cache=cache,
            processed_offset=processed_offset,
            changelog_offset=changelog_offset,
            batch=batch,
        )
