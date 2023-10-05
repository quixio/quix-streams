import contextlib
import logging
import time
from typing import Any, Union, Optional

import rocksdict
from typing_extensions import Self

from .exceptions import (
    StateTransactionError,
    NestedPrefixError,
)
from .options import RocksDBOptions
from .serialization import serialize, deserialize, serialize_key
from .types import RocksDBOptionsProto, DumpsFunc, LoadsFunc

__all__ = (
    "RocksDBStorage",
    "TransactionStore",
)

logger = logging.getLogger(__name__)

_sentinel = object()

_PREFIX_SEPARATOR = b"|"

_DEFAULT_PREFIX = b""


class RocksDBStorage:
    """
    A base class to for accessing state in RocksDB.
    It represents a single partition of the state.

    Responsibilities:
     1. Managing access to the RocksDB instance
     2. Creating transactions to interact with data
     3. Flushing WriteBatches to the RocksDB

    It opens the RocksDB on `__init__`. If the db is locked by another process,
    it will retry opening according to `open_max_retries` and `open_retry_backoff`

    :param path: an absolute path to the RocksDB folder
    :param options: RocksDB options. If `None`, the default options will be used.
    :param open_max_retries: number of times to retry opening the database
        if it's locked by another process. To disable retrying, pass 0.
    :param open_retry_backoff: number of seconds to wait between each retry.
    :param dumps: the function used to serialize keys & values to bytes in transactions.
        Default - `json.dumps`
    :param loads: the function used to deserialize keys & values from bytes to objects
        in transactions. Default - `json.loads`.
    """

    def __init__(
        self,
        path: str,
        options: Optional[RocksDBOptionsProto] = None,
        open_max_retries: int = 10,
        open_retry_backoff: float = 3.0,
        dumps: Optional[DumpsFunc] = None,
        loads: Optional[LoadsFunc] = None,
    ):
        self._path = path
        self._options = options or RocksDBOptions()
        self._open_max_retries = open_max_retries
        self._open_retry_backoff = open_retry_backoff
        self._dumps = dumps
        self._loads = loads
        self._db = self._open_db()

    def begin(self) -> "TransactionStore":
        """
        Create a new `TransactionStore` object.
        Using `TransactionStore` is a recommended way for accessing the data.

        :return: an instance of `TransactionStore`
        """
        return TransactionStore(storage=self, dumps=self._dumps, loads=self._loads)

    def write(self, batch: rocksdict.WriteBatch):
        """
        Write `WriteBatch` to RocksDB
        :param batch: an instance of `rocksdict.WriteBatch`
        """
        self._db.write(batch)

    def get(self, key: bytes, default: Any = None) -> Union[None, bytes, Any]:
        """
        Get a key from RocksDB.

        :param key: a key encoded to `bytes`
        :param default: a default value to return if the key is not found.
        :return: a value if the key is present in the DB. Otherwise, `None` or `default`
        """
        return self._db.get(key, default)

    def exists(self, key: bytes) -> bool:
        """
        Check if a key is present in the DB.

        :param key: a key encoded to `bytes`.
        :return: `True` if the key is present, `False` otherwise.
        """
        return key in self._db

    def flush(self, wait: bool = False):
        """
        Manually flush the current mem-table.

        :param wait: whether to wait for the flush to finish
        """
        self._db.flush(wait)

    def close(self):
        """
        Close the underlying RocksDB
        """
        logger.info(f'Closing db partition on "{self._path}"')
        self._db.close()
        logger.info(f'Successfully closed db partition on "{self._path}"')

    @property
    def path(self) -> str:
        """
        Get absolute path to RocksDB database folder
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
        rocksdict.Rdict.destroy(path=path)

    def _open_db(self) -> rocksdict.Rdict:
        attempt = 1
        while True:
            logger.info(
                f'Opening db partition on "{self._path}" attempt={attempt}',
            )
            try:
                db = rocksdict.Rdict(
                    path=self._path,
                    options=self._options.to_options(),
                    access_type=rocksdict.AccessType.read_write(),
                )
                logger.info(
                    f'Successfully opened db partition on "{self._path}"',
                )
                return db
            except Exception as exc:
                is_locked = str(exc).lower().startswith("io error: lock")
                if not is_locked:
                    raise

                if self._open_max_retries <= 0 or attempt >= self._open_max_retries:
                    raise

                logger.warning(
                    f"Failed to open db partition, cannot acquire a lock. "
                    f"Retrying in {self._open_retry_backoff}sec."
                )

                attempt += 1
                time.sleep(self._open_retry_backoff)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class TransactionStore:
    """
    A transaction-based store to perform simple key-value operations like
    "get", "set", "delete" and "exists".
    It is supposed to be used by stateful operators in Streaming DataFrames.


    Serialization
    *************
    `TransactionStore` automatically serializes keys and values to JSON.

    Prefixing
    *********
    `TransactionStore` allows to set prefixes for the keys in the given code block
    using :meth:`with_prefix()` context manager.
    Normally, Streaming DataFrames will set prefixes order to scope
    the store operations

    Transactional properties
    ************************
    `TransactionStore` uses a combination of in-memory update cache
    and RocksDB's WriteBatch in order to accumulate all the state mutations
    in a single batch, flush them atomically, and allow the updates be visible
    within the transaction before it's flushed (aka "read-your-own-writes" problem).

    A single transaction can span across multiple incoming messages,

    If any mutation fails during the transaction
    (e.g. we failed to write the updates to the RocksDB), the whole transaction is now
    invalid and cannot be used anymore.
    In this case, a new `TransactionStore` should be created.

    The `TransactionStore` also cannot be used after it's flushed, and a new
    instance of `TransactionStore` should be used instead.

    :param storage: instance of `StateStorage` to be used for accessing
        the underlying RocksDB

    """

    __slots__ = (
        "_storage",
        "_update_cache",
        "_batch",
        "_prefix",
        "_failed",
        "_completed",
        "_dumps",
        "_loads",
    )

    def __init__(
        self,
        storage: RocksDBStorage,
        dumps: Optional[DumpsFunc] = None,
        loads: Optional[LoadsFunc] = None,
    ):
        self._storage = storage
        self._update_cache = {}
        self._batch = rocksdict.WriteBatch(raw_mode=True)
        self._prefix = b""
        self._failed = False
        self._completed = False
        self._dumps = dumps
        self._loads = loads

    @contextlib.contextmanager
    def with_prefix(self, prefix: Any = b"") -> Self:
        """
        Prefix all the keys in the given scope.

        Normally, it's called by Streaming DataFrames engine to ensure that every
        message key is stored separately.

        The `with_prefix` calls should not be nested.
        Only one prefix can be set at a time.

        :param prefix: a prefix string to be used.
            Should be either `bytes` or JSON-serializable object.
            The prefix doesn't need to contain the separator, it will be added
            automatically between the key and the prefix itself if the prefix
            is present.
        """
        if self._prefix != _DEFAULT_PREFIX:
            raise NestedPrefixError("The transaction already has a prefix")
        self._prefix = (
            prefix if isinstance(prefix, bytes) else self._serialize_value(prefix)
        )
        if self._prefix:
            self._prefix += _PREFIX_SEPARATOR

        try:
            yield self
        finally:
            self._prefix = b""

    def get(self, key: Any, default: Any = None) -> Optional[Any]:
        """
        Get a key from the store.

        It first looks up the key in the update cache in case it has been updated
        but not flushed yet.

        It returns `None` if the key is not found and `default` is not provided.

        :param key: a JSON-deserializable key.
        :param default: value to return if the key is not present in the state.
            It can be of any type.
        :return: a JSON-deserialized value
        """
        self._validate_transaction_state()

        # First, check the update cache in case the value was previously written
        # Use sentinel as default because the actual value can be "None"
        key_serialized = self._serialize_key(key)
        cached = self._update_cache.get(key_serialized, _sentinel)
        if cached is not _sentinel:
            return self._deserialize_value(cached)

        # The value is not found in cache, check the db
        stored = self._storage.get(key_serialized, _sentinel)
        if stored is not _sentinel:
            return self._deserialize_value(stored)
        return default

    def set(self, key: Any, value: Any):
        """
        Set a key to the store.

        It first updates the key in the update cache.

        :param key: a JSON-deserializable key.
        :param value: a JSON-serializable value
        """
        self._validate_transaction_state()

        key_serialized = self._serialize_key(key)
        value_serialized = self._serialize_value(value)

        try:
            self._batch.put(key_serialized, value_serialized)
            self._update_cache[key_serialized] = value_serialized
        except Exception:
            self._failed = True
            raise

    def delete(self, key: Any):
        """
        Delete a key from the store.

        It first deletes the key from the update cache.

        :param key: a JSON-deserializable key.
        :return:
        """
        self._validate_transaction_state()
        key_serialized = self._serialize_key(key)
        try:
            self._batch.delete(key_serialized)
            self._update_cache.pop(key_serialized, None)
        except Exception:
            self._failed = True
            raise

    def exists(self, key: Any) -> bool:
        """
        Check if a key exists in the store.

        It first looks up the key in the update cache.

        :param key: a JSON-deserializable key
        :return: `True` if the key exists, `False` otherwise.
        """
        self._validate_transaction_state()

        key_serialized = self._serialize_key(key)
        if key_serialized in self._update_cache:
            return True
        return self._storage.exists(key_serialized)

    @property
    def completed(self) -> bool:
        """
        Check if the transaction is completed.

        It doesn't indicate whether transaction is successful or not.
        Use `TransactionStore.failed` for that.

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

    def _validate_transaction_state(self):
        if self._failed:
            raise StateTransactionError(
                "Transaction is failed, create a new one to proceed"
            )
        if self._completed:
            raise StateTransactionError(
                "Transaction is already finished, create a new one to proceed"
            )

    def _flush(self):
        # TODO: Does Flush sometimes need to be called manually?
        """
        Flush the recent updates to the database and empty the update cache.
        It writes the WriteBatch to RocksDB and marks itself as finished.

        If writing fails, the transaction will be also marked as "failed" and
        cannot be used anymore.
        """
        self._validate_transaction_state()

        try:
            # Don't write batches if this transaction doesn't change any keys
            if len(self._batch):
                self._storage.write(self._batch)
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
        return serialize_key(key, prefix=self._prefix, dumps=self._dumps)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._failed:
            self._flush()
