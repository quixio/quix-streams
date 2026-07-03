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
        self,
        key: bytes,
        value: Optional[bytes],
        cf_name: str,
        offset: int,
        ttl_stamped: bool = False,
    ):
        """
        Updates state from a given changelog message.

        :param key: changelog message key
        :param value: changelog message value
        :param cf_name: column family name
        :param offset: changelog message offset
        :param ttl_stamped: True when the changelog record carries the
            ``__ttl_stamped__`` header (spec §8.7) — i.e. the value is a stamped
            default-CF record. Absent/False = legacy / un-stamped.
        """

    def complete_recovery(self) -> None:
        """
        Recovery-finalize hook, called once by the recovery manager after this
        partition has replayed every changelog message up to the high-watermark
        and before it is handed to live processing.

        The default is a no-op. The RocksDB backend overrides this to complete an
        interrupted legacy-TTL migration whose changelog replayed as MIXED
        (some ``__ttl_stamped__``-header records + some header-absent legacy
        records) — see ``RocksDBStorePartition.complete_recovery`` and
        ``spec-incomplete-migration-recovery.md`` (spec §8.8).
        """
        return None

    def has_incomplete_ttl_migration(self) -> bool:
        """
        Whether this partition has a durably-recorded, not-yet-finished legacy-TTL
        migration that must be completed via :meth:`complete_recovery` (Fix B,
        shortcut 73191 review).

        True iff the partition is persisted-flipped into TTL mode AND its
        ``__ttl_backfill_pending__`` census still holds leftover legacy keys AND
        no durable "migration done" marker is present. The recovery layer consults
        this so a restart whose changelog offset is already caught up
        (``highwater-1 == offset``, so the normal "state behind" check is False)
        still runs the completion pass instead of stranding the leftovers.

        The default is ``False`` — only the RocksDB backend has the durable
        pending census + flip flag on disk that this detects. The memory backend
        re-recovers from the changelog on every open (full replay), so the
        offset-caught-up crash window does not apply to it.
        """
        return False

    @abstractmethod
    def begin(self) -> PartitionTransaction:
        """
        Start a new `PartitionTransaction`

        Using `PartitionTransaction` is a recommended way for accessing the data.
        """
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
