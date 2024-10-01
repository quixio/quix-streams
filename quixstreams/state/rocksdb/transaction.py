import logging
from typing import Any, Optional

from quixstreams.state.metadata import UNDEFINED
from quixstreams.state.base import PartitionTransaction

__all__ = ("RocksDBPartitionTransaction",)

logger = logging.getLogger(__name__)


class RocksDBPartitionTransaction(PartitionTransaction):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single RocksDB partition.

    Serialization
    *************
    `RocksDBTransaction` automatically serializes keys and values to bytes.

    Prefixing
    *********
    Methods `get()`, `set()`, `delete()` and `exists()` methods require prefixes for
    the keys.
    Normally, the Kafka message keys are supposed to be used as prefixes.

    Transactional properties
    ************************
    `RocksDBTransaction` uses a combination of in-memory update cache
    and RocksDB's WriteBatch in order to accumulate all the state mutations
    in a single batch, flush them atomically, and allow the updates be visible
    within the transaction before it's flushed (aka "read-your-own-writes" problem).

    If any mutation fails during the transaction
    (e.g., failed to write the updates to the RocksDB), the whole transaction
    will be marked as failed and cannot be used anymore.
    In this case, a new `RocksDBTransaction` should be created.

    `RocksDBTransaction` can be used only once.
    """

    def _get(
        self, key_serialized: bytes, default: Any = None, cf_name: str = "default"
    ) -> Optional[Any]:
        """
        Get a key from the store.

        It returns `None` if the key is not found and `default` is not provided.

        :param key_serialized: a key to get from DB
        :param default: value to return if the key is not present in the state.
            It can be of any type.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: value or `default`
        """
        # The value is not found in cache, check the db
        stored = self._partition.get(key_serialized, UNDEFINED, cf_name=cf_name)
        if stored is UNDEFINED:
            return default
        return self._deserialize_value(stored)

    def _exists(self, key_serialized: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key exists in the store.

        :param key_serialized: a key to get from DB
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key exists, `False` otherwise.
        """
        return self._partition.exists(key_serialized, cf_name=cf_name)
