import contextlib
import functools
import logging
import time
from typing import Any, Union, Optional, List

from rocksdict import WriteBatch, Rdict, ColumnFamily, AccessType
from typing_extensions import Self

from quixstreams.state.types import (
    DumpsFunc,
    LoadsFunc,
    PartitionTransaction,
    StorePartition,
)
from .exceptions import (
    StateTransactionError,
    NestedPrefixError,
    ColumnFamilyAlreadyExists,
    ColumnFamilyDoesNotExist,
)
from .metadata import METADATA_CF_NAME, PROCESSED_OFFSET_KEY, PREFIX_SEPARATOR
from .options import RocksDBOptions
from .serialization import (
    serialize,
    deserialize,
    int_from_int64_bytes,
    int_to_int64_bytes,
)
from .types import RocksDBOptionsType
from ..state import TransactionState

__all__ = (
    "RocksDBStorePartition",
    "RocksDBPartitionTransaction",
)


logger = logging.getLogger(__name__)

_undefined = object()
_deleted = object()

_DEFAULT_PREFIX = b""


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

    def begin(self) -> "RocksDBPartitionTransaction":
        """
        Create a new `RocksDBTransaction` object.
        Using `RocksDBTransaction` is a recommended way for accessing the data.

        :return: an instance of `RocksDBTransaction`
        """
        return RocksDBPartitionTransaction(
            partition=self, dumps=self._dumps, loads=self._loads
        )

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


def _validate_transaction_state(func):
    """
    Check that the state of `RocksDBTransaction` is valid before calling a method
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: "RocksDBPartitionTransaction" = args[0]
        if self.failed:
            raise StateTransactionError(
                "Transaction is failed, create a new one to proceed"
            )
        if self.completed:
            raise StateTransactionError(
                "Transaction is already finished, create a new one to proceed"
            )

        return func(*args, **kwargs)

    return wrapper


class RocksDBPartitionTransaction(PartitionTransaction):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single RocksDB partition.

    Serialization
    *************
    `RocksDBTransaction` automatically serializes keys and values to bytes.

    Prefixing
    *********
    `RocksDBTransaction` allows to set prefixes for the keys in the given code block
    using :meth:`with_prefix()` context manager.
    Normally, `StreamingDataFrame` class will use message keys as prefixes
    in order to namespace the stored keys across different messages.

    Transactional properties
    ************************
    `RocksDBTransaction` uses a combination of in-memory update cache
    and RocksDB's WriteBatch in order to accumulate all the state mutations
    in a single batch, flush them atomically, and allow the updates be visible
    within the transaction before it's flushed (aka "read-your-own-writes" problem).

    If any mutation fails during the transaction
    (e.g. we failed to write the updates to the RocksDB), the whole transaction
    will be marked as failed and cannot be used anymore.
    In this case, a new `RocksDBTransaction` should be created.

    `RocksDBTransaction` can be used only once.

    :param partition: instance of `RocksDBStatePartition` to be used for accessing
        the underlying RocksDB
    :param dumps: a function to serialize data to bytes.
    :param loads: a function to deserialize data from bytes.
    """

    __slots__ = (
        "_partition",
        "_update_cache",
        "_batch",
        "_prefix",
        "_failed",
        "_completed",
        "_dumps",
        "_loads",
        "_state",
    )

    def __init__(
        self,
        partition: RocksDBStorePartition,
        dumps: DumpsFunc,
        loads: LoadsFunc,
    ):
        self._partition = partition
        self._update_cache = {}
        self._batch = WriteBatch(raw_mode=True)
        self._prefix = _DEFAULT_PREFIX
        self._failed = False
        self._completed = False
        self._dumps = dumps
        self._loads = loads
        self._state = TransactionState(transaction=self)

    @property
    def state(self) -> TransactionState:
        return self._state

    @contextlib.contextmanager
    def with_prefix(self, prefix: Any = b"") -> Self:
        """
        A context manager set the prefix for all keys in the scope.

        Normally, it's called by Streaming DataFrames engine to ensure that every
        message key is stored separately.

        The `with_prefix` calls should not be nested.
        Only one prefix can be set at a time.

        :param prefix: a prefix string to be used.
            Should be either `bytes` or object serializable to `bytes`
            by `dumps` function.
            The prefix doesn't need to contain the separator, it will be added
            automatically between the key and the prefix if the prefix
            is not empty.
        """
        if self._prefix != _DEFAULT_PREFIX:
            raise NestedPrefixError("The transaction already has a prefix")
        self._prefix = (
            prefix if isinstance(prefix, bytes) else self._serialize_value(prefix)
        )

        try:
            yield self
        finally:
            self._prefix = _DEFAULT_PREFIX

    @_validate_transaction_state
    def get(
        self, key: Any, default: Any = None, cf_name: str = "default"
    ) -> Optional[Any]:
        """
        Get a key from the store.

        It first looks up the key in the update cache in case it has been updated
        but not flushed yet.

        It returns `None` if the key is not found and `default` is not provided.

        :param key: a key to get from DB
        :param default: value to return if the key is not present in the state.
            It can be of any type.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: value or `default`
        """

        # First, check the update cache in case the value was previously written
        # Use _undefined sentinel as default because the actual value can be "None"
        key_serialized = self._serialize_key(key)
        cached = self._update_cache.get(cf_name, {}).get(key_serialized, _undefined)
        if cached is _deleted:
            return default

        if cached is not _undefined:
            return self._deserialize_value(cached)

        # The value is not found in cache, check the db
        stored = self._partition.get(key_serialized, _undefined, cf_name=cf_name)
        if stored is not _undefined:
            return self._deserialize_value(stored)
        return default

    @_validate_transaction_state
    def set(self, key: Any, value: Any, cf_name: str = "default"):
        """
        Set a key to the store.

        It first updates the key in the update cache.

        :param key: key to store in DB
        :param value: value to store in DB
        :param cf_name: rocksdb column family name. Default - "default"
        """

        key_serialized = self._serialize_key(key)
        value_serialized = self._serialize_value(value)

        try:
            cf_handle = self._partition.get_column_family_handle(cf_name)
            self._batch.put(key_serialized, value_serialized, cf_handle)
            self._update_cache.setdefault(cf_name, {})[
                key_serialized
            ] = value_serialized
        except Exception:
            self._failed = True
            raise

    @_validate_transaction_state
    def delete(self, key: Any, cf_name: str = "default"):
        """
        Delete a key from the store.

        It first deletes the key from the update cache.

        :param key: key to delete from DB
        :param cf_name: rocksdb column family name. Default - "default"
        """
        key_serialized = self._serialize_key(key)
        try:
            cf_handle = self._partition.get_column_family_handle(cf_name)
            self._batch.delete(key_serialized, cf_handle)

            if cf_name not in self._update_cache:
                self._update_cache[cf_name] = {}
            self._update_cache[cf_name][key_serialized] = _deleted

        except Exception:
            self._failed = True
            raise

    @_validate_transaction_state
    def exists(self, key: Any, cf_name: str = "default") -> bool:
        """
        Check if a key exists in the store.

        It first looks up the key in the update cache.

        :param key: a key to check in DB
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key exists, `False` otherwise.
        """

        key_serialized = self._serialize_key(key)
        cached = self._update_cache.get(cf_name, {}).get(key_serialized, _undefined)
        if cached is _deleted:
            return False

        if cached is not _undefined:
            return True

        return self._partition.exists(key_serialized, cf_name=cf_name)

    @property
    def completed(self) -> bool:
        """
        Check if the transaction is completed.

        It doesn't indicate whether transaction is successful or not.
        Use `RocksDBTransaction.failed` for that.

        The completed transaction should not be re-used.

        :return: `True` if transaction is completed, `False` otherwise.
        """
        return self._completed

    @property
    def failed(self) -> bool:
        """
        Check if the transaction has failed.

        The failed transaction should not be re-used because the update cache
        and

        :return: `True` if transaction is failed, `False` otherwise.
        """
        return self._failed

    @_validate_transaction_state
    def maybe_flush(self, offset: Optional[int] = None):
        """
        Flush the recent updates to the database and empty the update cache.
        It writes the WriteBatch to RocksDB and marks itself as finished.

        If writing fails, the transaction will be also marked as "failed" and
        cannot be used anymore.

        >***NOTE:*** If no keys have been modified during the transaction
            (i.e. no "set" or "delete" have been called at least once), it will
            not flush ANY data to the database including the offset in order to optimize
            I/O.

        :param offset: offset of the last processed message, optional.
        """
        try:
            # Don't write batches if this transaction doesn't change any keys
            if len(self._batch):
                if offset is not None:
                    cf_handle = self._partition.get_column_family_handle(
                        METADATA_CF_NAME
                    )
                    self._batch.put(
                        PROCESSED_OFFSET_KEY, int_to_int64_bytes(offset), cf_handle
                    )
                self._partition.write(self._batch)
        except Exception:
            self._failed = True
            raise
        finally:
            self._completed = True

    def _serialize_value(self, value: Any) -> bytes:
        return serialize(value, dumps=self._dumps)

    def _deserialize_value(self, value: bytes) -> Any:
        return deserialize(value, loads=self._loads)

    def _serialize_key(self, key: Any) -> bytes:
        key_bytes = serialize(key, dumps=self._dumps)
        prefix = self._prefix + PREFIX_SEPARATOR if self._prefix else b""
        return prefix + key_bytes

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None and not self._failed:
            self.maybe_flush()
