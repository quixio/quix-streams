import functools
import logging
from typing import Any, Dict, Literal, Optional, Union

from quixstreams.state.base import PartitionTransactionCache, StorePartition
from quixstreams.state.exceptions import ColumnFamilyDoesNotExist
from quixstreams.state.metadata import METADATA_CF_NAME, Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.utils.json import dumps as json_dumps
from quixstreams.utils.json import loads as json_loads

logger = logging.getLogger(__name__)

__all__ = ("MemoryStorePartition",)


def _validate_partition_state():
    """
    Check that the store is not closed
    """

    def wrapper(func):
        @functools.wraps(func)
        def _wrapper(partition: "MemoryStorePartition", *args, **kwargs):
            if partition.closed:
                raise RuntimeError("partition is closed")

            return func(partition, *args, **kwargs)

        return _wrapper

    return wrapper


class MemoryStorePartition(StorePartition):
    """
    Class to access in-memory state.

    Responsibilities:
     1. Recovering from changelog messages
     2. Creating transaction to interact with data
     3. Track partition state in-memory
    """

    def __init__(self, changelog_producer: Optional[ChangelogProducer]) -> None:
        super().__init__(
            dumps=json_dumps,
            loads=json_loads,
            changelog_producer=changelog_producer,
        )
        self._changelog_offset: Optional[int] = None
        self._state: Dict[str, Dict[bytes, Any]] = {
            "default": {},
            METADATA_CF_NAME: {},
        }
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    @_validate_partition_state()
    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
    ) -> None:
        """
        Write data to the state

        :param cache: The partition update cache
        :param changelog_offset: The changelog message offset of the data.
        """
        if changelog_offset is not None:
            self._changelog_offset = changelog_offset

        for cf_name in cache.get_column_families():
            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    self._state.setdefault(cf_name, {})[key] = value

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                self._state[cf_name].pop(key, None)

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ) -> None:
        if cf_name not in self._state:
            raise ColumnFamilyDoesNotExist(f'Column family "{cf_name}" does not exist')

        if value:
            self._state.setdefault(cf_name, {})[key] = value
        else:
            self._state.setdefault(cf_name, {}).pop(key, None)

        self._changelog_offset = offset

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        return self._changelog_offset

    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """
        self._changelog_offset = offset

    @_validate_partition_state()
    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from the store

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the store. Otherwise, `default`
        """
        return self._state.get(cf_name, {}).get(key, Marker.UNDEFINED)

    @_validate_partition_state()
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the store.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        return key in self._state.get(cf_name, {})
