import logging
import time
from typing import Dict, List, Literal, Optional, Union, cast

from rocksdict import AccessType, ColumnFamily, Rdict, WriteBatch

from quixstreams.state.base import PartitionTransactionCache, StorePartition
from quixstreams.state.exceptions import ColumnFamilyDoesNotExist
from quixstreams.state.metadata import METADATA_CF_NAME, Marker
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import (
    int_from_int64_bytes,
    int_to_int64_bytes,
)

from .exceptions import ColumnFamilyAlreadyExists
from .metadata import (
    CHANGELOG_OFFSET_KEY,
)
from .options import RocksDBOptions
from .types import RocksDBOptionsType

__all__ = ("RocksDBStorePartition",)

logger = logging.getLogger(__name__)


class RocksDBStorePartition(StorePartition):
    """
    A base class to access state in RocksDB.
    It represents a single RocksDB database.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB

    It opens the RocksDB on `__init__`. If the db is locked by another process,
    it will retry according to `open_max_retries` and `open_retry_backoff` options.

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        if not options:
            options = RocksDBOptions()

        super().__init__(options.dumps, options.loads, changelog_producer)
        self._path = path
        self._options = options
        self._rocksdb_options = self._options.to_options()
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._db = self._init_rocksdb()
        self._cf_cache: Dict[str, Rdict] = {}
        self._cf_handle_cache: Dict[str, ColumnFamily] = {}

    def recover_from_changelog_message(
        self, key: bytes, value: Optional[bytes], cf_name: str, offset: int
    ):
        cf_handle = self.get_column_family_handle(cf_name)
        batch = WriteBatch(raw_mode=True)
        if value is None:
            batch.delete(key, cf_handle)
        else:
            batch.put(key, value, cf_handle)

        # Update the changelog offset and flush outstanding writes.
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def write(
        self,
        cache: PartitionTransactionCache,
        changelog_offset: Optional[int],
        batch: Optional[WriteBatch] = None,
    ):
        """
        Write data to RocksDB

        :param cache: The modified data
        :param changelog_offset: The changelog message offset of the data.
        :param batch: prefilled `rocksdict.WriteBatch`, optional.
        """
        if batch is None:
            batch = WriteBatch(raw_mode=True)

        # Iterate over the transaction update cache
        column_families = cache.get_column_families()
        for cf_name in column_families:
            cf_handle = self.get_column_family_handle(cf_name)

            updates = cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    batch.put(key, value, cf_handle)

            deletes = cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                batch.delete(key, cf_handle)

        # Save the latest changelog topic offset to know where to recover from
        # It may be None if changelog topics are disabled
        if changelog_offset is not None:
            self._update_changelog_offset(batch=batch, offset=changelog_offset)
        logger.debug(
            f"Flushing state changes to the disk "
            f'path="{self.path}" '
            f"changelog_offset={changelog_offset}"
        )

        self._write(batch)

    def _write(self, batch: WriteBatch):
        """
        Write `WriteBatch` to RocksDB
        :param batch: an instance of `rocksdict.WriteBatch`
        """
        self._db.write(batch)

    def get(
        self, key: bytes, cf_name: str = "default"
    ) -> Union[bytes, Literal[Marker.UNDEFINED]]:
        """
        Get a key from RocksDB.

        :param key: a key encoded to `bytes`
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the DB. Otherwise, `default`
        """
        result = self.get_column_family(cf_name).get(key, default=Marker.UNDEFINED)

        # RDict accept Any type as value but we only write bytes so we should only get bytes back.
        return cast(Union[bytes, Literal[Marker.UNDEFINED]], result)

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the DB.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        cf_dict = self.get_column_family(cf_name)
        return key in cf_dict

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        metadata_cf = self.get_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)
        if offset_bytes is None:
            return None

        return int_from_int64_bytes(offset_bytes)

    def write_changelog_offset(self, offset: int):
        """
        Write a new changelog offset to the db.

        To be used when we simply need to update the changelog offset without touching
        the actual data.

        :param offset: new changelog offset
        """
        batch = WriteBatch(raw_mode=True)
        self._update_changelog_offset(batch=batch, offset=offset)
        self._write(batch)

    def close(self):
        """
        Close the underlying RocksDB
        """
        logger.debug(f'Closing rocksdb partition on "{self._path}"')
        # Clean the column family caches to drop references
        # Otherwise the Rocksdb won't close properly
        self._cf_handle_cache = {}
        self._cf_cache = {}
        self._db.close()
        logger.debug(f'Closed rocksdb partition on "{self._path}"')

    @property
    def path(self) -> str:
        """
        Absolute path to RocksDB database folder
        :return: file path
        """
        return self._path

    @classmethod
    def destroy(cls, path: str):
        """
        Delete underlying RocksDB database

        The database must be closed first.

        :param path: an absolute path to the RocksDB folder
        """
        Rdict.destroy(path=path)

    def get_column_family_handle(self, cf_name: str) -> ColumnFamily:
        """
        Get a column family handle to pass to it WriteBatch.
        This method will cache the CF handle instance to avoid creating them
        repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.ColumnFamily`
        """
        if (cf_handle := self._cf_handle_cache.get(cf_name)) is None:
            try:
                cf_handle = self._db.get_column_family_handle(cf_name)
                self._cf_handle_cache[cf_name] = cf_handle
            except Exception as exc:
                if "does not exist" in str(exc):
                    raise ColumnFamilyDoesNotExist(
                        f'Column family "{cf_name}" does not exist'
                    )
                raise
        return cf_handle

    def get_column_family(self, cf_name: str) -> Rdict:
        """
        Get a column family instance.
        This method will cache the CF instance to avoid creating them repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.Rdict` for the given column family
        """
        if (cf := self._cf_cache.get(cf_name)) is None:
            try:
                cf = self._db.get_column_family(cf_name)
                self._cf_cache[cf_name] = cf
            except Exception as exc:
                if "does not exist" in str(exc):
                    raise ColumnFamilyDoesNotExist(
                        f'Column family "{cf_name}" does not exist'
                    )
                raise
        return cf

    def create_column_family(self, cf_name: str):
        try:
            cf = self._db.create_column_family(cf_name, options=self._rocksdb_options)
        except Exception as exc:
            if "column family already exists" in str(exc).lower():
                raise ColumnFamilyAlreadyExists(
                    f'Column family already exists: "{cf_name}"'
                )
            raise

        self._cf_cache[cf_name] = cf

    def drop_column_family(self, cf_name: str):
        self._cf_cache.pop(cf_name, None)
        self._cf_handle_cache.pop(cf_name, None)
        try:
            self._db.drop_column_family(cf_name)
        except Exception as exc:
            if "invalid column family:" in str(exc).lower():
                raise ColumnFamilyDoesNotExist(
                    f'Column family does not exist: "{cf_name}"'
                )
            raise

    def list_column_families(self) -> List[str]:
        return self._db.list_cf(self._path)

    def _open_rocksdict(self) -> Rdict:
        options = self._rocksdb_options
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        rdict = Rdict(
            path=self._path,
            options=options,
            access_type=AccessType.read_write(),
        )
        # Ensure metadata column family is created without defining it upfront
        try:
            rdict.get_column_family(METADATA_CF_NAME)
        except Exception as exc:
            if "does not exist" in str(exc):
                rdict.create_column_family(METADATA_CF_NAME, options=options)
            else:
                raise

        return rdict

    def _init_rocksdb(self) -> Rdict:
        attempt = 1
        while True:
            logger.debug(
                f'Opening rocksdb partition on "{self._path}" attempt={attempt}',
            )
            try:
                db = self._open_rocksdict()
                logger.debug(
                    f'Successfully opened rocksdb partition on "{self._path}"',
                )
                return db
            except Exception as exc:
                is_locked = str(exc).lower().startswith("io error")
                if not is_locked:
                    raise

                if self._open_max_retries <= 0 or attempt >= self._open_max_retries:
                    raise

                logger.warning(
                    f"Failed to open rocksdb partition, cannot acquire a lock. "
                    f"Retrying in {self._open_retry_backoff}sec."
                )

                attempt += 1
                time.sleep(self._open_retry_backoff)

    def _update_changelog_offset(self, batch: WriteBatch, offset: int):
        batch.put(
            CHANGELOG_OFFSET_KEY,
            int_to_int64_bytes(offset),
            self.get_column_family_handle(METADATA_CF_NAME),
        )
