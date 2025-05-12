import logging
from typing import Iterator, Type, cast

from ..partition import RocksDBStorePartition
from .metadata import (
    GLOBAL_COUNTER_CF_NAME,
    LATEST_DELETED_VALUE_CF_NAME,
    LATEST_DELETED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_CF_NAME,
    LATEST_TIMESTAMPS_CF_NAME,
    VALUES_CF_NAME,
)
from .transaction import WindowedRocksDBPartitionTransaction

logger = logging.getLogger(__name__)


class WindowedRocksDBStorePartition(RocksDBStorePartition):
    """
    A base class to access windowed state in RocksDB.
    It represents a single RocksDB database.

    Besides the data, it keeps track of the latest observed timestamp and
    stores the expiration index to delete expired windows.
    """

    partition_transaction_class: Type[WindowedRocksDBPartitionTransaction] = (
        WindowedRocksDBPartitionTransaction
    )
    additional_column_families = (
        LATEST_DELETED_VALUE_CF_NAME,
        LATEST_EXPIRED_WINDOW_CF_NAME,
        LATEST_DELETED_WINDOW_CF_NAME,
        LATEST_TIMESTAMPS_CF_NAME,
        GLOBAL_COUNTER_CF_NAME,
        VALUES_CF_NAME,
    )

    def iter_keys(self, cf_name: str = "default") -> Iterator[bytes]:
        """
        Iterate over all keys in the DB.

        Addition and deletion of keys during iteration is not supported.

        :param cf_name: rocksdb column family name. Default - "default"
        :return: An iterable of keys
        """
        cf_dict = self.get_column_family(cf_name)
        return cast(Iterator[bytes], cf_dict.keys())

    def begin(self) -> WindowedRocksDBPartitionTransaction:
        """
        Start a new `WindowedRocksDBPartitionTransaction`

        Using `WindowedRocksDBPartitionTransaction` is a recommended way for accessing the data.
        """
        return self.partition_transaction_class(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )
