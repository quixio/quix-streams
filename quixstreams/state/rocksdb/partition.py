import logging
import time
from typing import Any, Union, Optional, List

from rocksdict import WriteBatch, Rdict, ColumnFamily, AccessType

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.state.types import (
    StorePartition,
)
from .exceptions import (
    ColumnFamilyAlreadyExists,
    ColumnFamilyDoesNotExist,
)
from .metadata import (
    METADATA_CF_NAME,
    PROCESSED_OFFSET_KEY,
    CHANGELOG_OFFSET_KEY,
)
from .options import RocksDBOptions
from .serialization import (
    int_from_int64_bytes,
)
from .transaction import (
    RocksDBPartitionTransaction,
    RocksDBPartitionRecoveryTransaction,
)
from .types import RocksDBOptionsType
from ..recovery import ChangelogProducer

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
        self._path = path
        self._options = options or RocksDBOptions()
        self._rocksdb_options = self._options.to_options()
        self._dumps = self._options.dumps
        self._loads = self._options.loads
        self._open_max_retries = self._options.open_max_retries
        self._open_retry_backoff = self._options.open_retry_backoff
        self._db = self._init_rocksdb()
        self._cf_cache = {}
        self._cf_handle_cache = {}
        self._changelog_producer = changelog_producer

    def begin(
        self,
    ) -> RocksDBPartitionTransaction:
        """
        Create a new `RocksDBTransaction` object.
        Using `RocksDBTransaction` is a recommended way for accessing the data.

        :return: an instance of `RocksDBTransaction`
        """
        return RocksDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def recover(self, changelog_message: ConfluentKafkaMessageProto):
        """
        Updates state from a given changelog message.

        :param changelog_message: A raw Confluent message read from a changelog topic.
        """
        with RocksDBPartitionRecoveryTransaction(
            partition=self,
            changelog_message=changelog_message,
        ) as partition:
            partition.recover()

    def set_changelog_offset(self, changelog_message: ConfluentKafkaMessageProto):
        """
        Set the changelog offset based on a message (usually an "offset-only" message).

        Used during recovery.

        :param changelog_message: A Confluent-like message, usually only with offset
        """
        RocksDBPartitionRecoveryTransaction(
            partition=self, changelog_message=changelog_message
        ).flush()

    def write(self, batch: WriteBatch):
        """
        Write `WriteBatch` to RocksDB
        :param batch: an instance of `rocksdict.WriteBatch`
        """
        self._db.write(batch)

    def get(
        self, key: bytes, default: Any = None, cf_name: str = "default"
    ) -> Union[None, bytes, Any]:
        """
        Get a key from RocksDB.

        :param key: a key encoded to `bytes`
        :param default: a default value to return if the key is not found.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the DB. Otherwise, `default`
        """
        cf_dict = self.get_column_family(cf_name)
        return cf_dict.get(key, default)

    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the DB.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """
        cf_dict = self.get_column_family(cf_name)
        return key in cf_dict

    def get_processed_offset(self) -> Optional[int]:
        """
        Get last processed offset for the given partition
        :return: offset or `None` if there's no processed offset yet
        """
        metadata_cf = self.get_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(PROCESSED_OFFSET_KEY)
        if offset_bytes is None:
            offset_bytes = self._db.get(PROCESSED_OFFSET_KEY)
        if offset_bytes is not None:
            return int_from_int64_bytes(offset_bytes)

    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        metadata_cf = self.get_column_family(METADATA_CF_NAME)
        offset_bytes = metadata_cf.get(CHANGELOG_OFFSET_KEY)
        if offset_bytes is None:
            offset_bytes = self._db.get(CHANGELOG_OFFSET_KEY)
        if offset_bytes is not None:
            return int_from_int64_bytes(offset_bytes)

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
        cached_cf_handle = self._cf_handle_cache.get(cf_name)
        if cached_cf_handle is not None:
            return cached_cf_handle
        new_cf_handle = self._db.get_column_family_handle(cf_name)
        self._cf_handle_cache[cf_name] = new_cf_handle
        return new_cf_handle

    def get_column_family(self, cf_name: str) -> Rdict:
        """
        Get a column family instance.
        This method will cache the CF instance to avoid creating them repeatedly.

        :param cf_name: column family name
        :return: instance of `rocksdict.Rdict` for the given column family
        """
        cached_cf = self._cf_cache.get(cf_name)
        if cached_cf is not None:
            return cached_cf
        try:
            new_cf = self._db.get_column_family(cf_name)
        except Exception as exc:
            if "does not exist" in str(exc):
                raise ColumnFamilyDoesNotExist(
                    f'Column family "{cf_name}" does not exist'
                )
            raise

        self._cf_cache[cf_name] = new_cf
        return new_cf

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
