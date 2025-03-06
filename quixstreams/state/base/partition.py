import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, Optional, Union

from quixstreams.state.metadata import (
    Marker,
)
from quixstreams.state.serialization import DumpsFunc, LoadsFunc

from .transaction import PartitionTransaction, PartitionTransactionCache

if TYPE_CHECKING:
    from quixstreams.state.recovery import ChangelogProducer


__all__ = ("StorePartition",)

logger = logging.getLogger(__name__)


class StorePartition(ABC):
    """
    A base class to access state in the underlying storage.
    It represents a single instance of some storage (e.g. a single database for
    the persistent storage).
    """

    def __init__(
        self,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"],
    ) -> None:
        super().__init__()
        self._dumps = dumps
        self._loads = loads
        self._changelog_producer = changelog_producer

    @abstractmethod
    def close(self): ...

    @abstractmethod
    def get_changelog_offset(self) -> Optional[int]:
        """
        Get the changelog offset that the state is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        ...

    @abstractmethod
    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """

    @abstractmethod
    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
    ):
        """
        Update the state with data from the update cache

        :param cache: The modified data
        :param changelog_offset: The changelog message offset of the data.
        """

    @abstractmethod
    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from the store

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the store. Otherwise, `default`
        """

    @abstractmethod
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the store.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """

    @abstractmethod
    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        """
        Updates state from a given changelog message.

        :param key: changelog message key
        :param value: changelog message value
        :param cf_name: column family name
        :param offset: changelog message offset
        """

    def begin(self) -> PartitionTransaction:
        """
        Start a new `PartitionTransaction`

        Using `PartitionTransaction` is a recommended way for accessing the data.
        """
        return PartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
