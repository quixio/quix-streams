import logging

from typing import Optional, Dict, Callable, Any, TYPE_CHECKING

from quixstreams.state.metadata import UNDEFINED
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.base import PartitionTransaction

if TYPE_CHECKING:
    from .partition import MemoryStorePartition

logger = logging.getLogger(__name__)

__all__ = ("MemoryPartitionTransaction",)


class MemoryPartitionTransaction(PartitionTransaction):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single in-memory partition.

    Serialization
    *************
    `MemoryPartitionTransaction` automatically serializes keys and values to bytes.

    Prefixing
    *********
    Methods `get()`, `set()`, `delete()` and `exists()` methods require prefixes for
    the keys.
    Normally, the Kafka message keys are supposed to be used as prefixes.

    Transactional properties
    ************************
    `MemoryPartitionTransaction` keeps all data in-memory. On restart all state is lost
    and needs to be recovered from the changelog.

    If any mutation fails during the transaction
    (e.g., failed to serialize the data), the whole transaction
    will be marked as failed and cannot be used anymore.
    In this case, a new `MemoryPartitionTransaction` should be created.

    `MemoryPartitionTransaction` can be used only once.
    """

    def __init__(
        self,
        partition: "MemoryStorePartition",
        state: Dict[str, Any],
        dumps: Callable[[Any], bytes],
        loads: Callable[[bytes], Any],
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> None:
        super().__init__(partition, dumps, loads, changelog_producer)

        self._state = state

    def _get(
        self, key_serialized: bytes, default: Any = None, cf_name: str = "default"
    ) -> Any:
        """
        Get a key from the store

        :param key_serialized: a key to get from DB
        :param default: value to return if the key is not present in the state.
            It can be of any type.
        :param cf_name: column family name. Default - "default"
        :return: value or `default`
        """
        stored = self._state.get(cf_name, {}).get(key_serialized, UNDEFINED)
        if stored is UNDEFINED:
            return default
        return self._deserialize_value(stored)

    def _exists(self, key_serialized: bytes, cf_name: str) -> bool:
        """
        Check if the key exists in state.
        :param key_serialized: a key to get from DB
        :return: True if key exists, False otherwise
        """
        return key_serialized in self._state.get(cf_name, {})
