import logging

from typing import Optional, Dict, Any, Union

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.base import StorePartition, CACHE_TYPE
from quixstreams.state.metadata import DELETED
from quixstreams.utils.json import loads as json_loads, dumps as json_dumps

logger = logging.getLogger(__name__)

__all__ = ("MemoryStorePartition",)


class MemoryStorePartition(StorePartition):
    """
    Class to access in-memory state.

    Responsabilities:
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
        self._processed_offset: Optional[int] = None
        self._changelog_offset: Optional[int] = None
        self._state: Dict[str, Dict[bytes, Dict[bytes, Any]]] = {"default": {}}

    def close(self) -> None: ...

    def write(
        self,
        data: CACHE_TYPE,
        processed_offset: Optional[int],
        changelog_offset: Optional[int],
    ) -> None:
        """
        Write data to the state

        :param data: The modified data
        :param processed_offset: The offset processed to generate the data.
        :param changelog_offset: The changelog message offset of the data.
        """
        if processed_offset is not None:
            self._processed_offset = processed_offset
        if changelog_offset is not None:
            self._changelog_offset = changelog_offset

        for cf_name, prefixes in data.items():
            for values in prefixes.values():
                for key, value in values.items():
                    if value is DELETED:
                        self._state[cf_name].pop(key, None)
                    else:
                        self._state.setdefault(cf_name, {})[key] = value

    def _recover_from_changelog_message(
        self,
        changelog_message: ConfluentKafkaMessageProto,
        cf_name: str,
        processed_offset: Optional[int],
        committed_offset: int,
    ) -> None:
        """
        Updates state from a given changelog message.

        :param changelog_message: A raw Confluent message read from a changelog topic.
        :param committed_offset: latest committed offset for the partition
        """
        self._changelog_offset = changelog_message.offset()

        if self._should_apply_changelog(processed_offset, committed_offset):
            if value := changelog_message.value():
                self._state.setdefault(cf_name, {})[changelog_message.key()] = value
            else:
                self._state.setdefault(cf_name, {}).pop(changelog_message.key, None)

    def get_processed_offset(self) -> Optional[int]:
        """
        Get last processed offset for the given partition
        :return: offset or `None` if there's no processed offset yet
        """
        return self._processed_offset

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        return self._changelog_offset

    def get(
        self, key: bytes, default: Any = None, cf_name: str = "default"
    ) -> Union[None, bytes, Any]:
        """
        Get a key from the store

        :param key: a key encoded to `bytes`
        :param default: a default value to return if the key is not found.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the store. Otherwise, `default`
        """
        return self._state.get(cf_name, {}).get(key, default)

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the store.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        return key in self._state.get(cf_name, {})
