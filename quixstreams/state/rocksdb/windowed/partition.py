import logging
from typing import Iterator, Optional, cast

from quixstreams.state.recovery import ChangelogProducer

from ..metadata import LATEST_TIMESTAMPS_CF_NAME
from ..partition import RocksDBStorePartition
from ..types import RocksDBOptionsType
from .metadata import (
    GLOBAL_COUNTER_CF_NAME,
    LATEST_DELETED_VALUE_CF_NAME,
    LATEST_DELETED_WINDOW_CF_NAME,
    LATEST_EXPIRED_WINDOW_CF_NAME,
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

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    """

    partition_transaction_class = WindowedRocksDBPartitionTransaction

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        super().__init__(
            path=path, options=options, changelog_producer=changelog_producer
        )
        self._ensure_column_family(LATEST_DELETED_VALUE_CF_NAME)
        self._ensure_column_family(LATEST_EXPIRED_WINDOW_CF_NAME)
        self._ensure_column_family(LATEST_DELETED_WINDOW_CF_NAME)
        self._ensure_column_family(LATEST_TIMESTAMPS_CF_NAME)
        self._ensure_column_family(GLOBAL_COUNTER_CF_NAME)
        self._ensure_column_family(VALUES_CF_NAME)

    def iter_keys(self, cf_name: str = "default") -> Iterator[bytes]:
        """
        Iterate over all keys in the DB.

        Addition and deletion of keys during iteration is not supported.

        :param cf_name: rocksdb column family name. Default - "default"
        :return: An iterable of keys
        """
        cf_dict = self.get_column_family(cf_name)
        return cast(Iterator[bytes], cf_dict.keys())
