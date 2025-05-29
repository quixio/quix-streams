import logging
from typing import Iterator, cast

from ..partition import RocksDBStorePartition
from .transaction import WindowedRocksDBPartitionTransaction

logger = logging.getLogger(__name__)


class WindowedRocksDBStorePartition(RocksDBStorePartition):
    """
    A base class to access windowed state in RocksDB.
    It represents a single RocksDB database.

    Besides the data, it keeps track of the latest observed timestamp and
    stores the expiration index to delete expired windows.
    """

    def iter_keys(self, cf_name: str = "default") -> Iterator[bytes]:
        """
        Iterate over all keys in the DB.

        Addition and deletion of keys during iteration is not supported.

        :param cf_name: rocksdb column family name. Default - "default"
        :return: An iterable of keys
        """
        cf_dict = self.get_or_create_column_family(cf_name)
        return cast(Iterator[bytes], cf_dict.keys())

    def begin(self) -> WindowedRocksDBPartitionTransaction:
        """
        Start a new `WindowedRocksDBPartitionTransaction`

        Using `WindowedRocksDBPartitionTransaction` is a recommended way for accessing the data.
        """
        return WindowedRocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )
