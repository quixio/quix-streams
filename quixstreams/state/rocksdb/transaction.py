import contextlib
import functools
import logging
from typing import Any, Union, Optional, Dict, NewType, TYPE_CHECKING

from rocksdict import WriteBatch, ColumnFamily
from typing_extensions import Self

from quixstreams.state.types import (
    DumpsFunc,
    LoadsFunc,
    PartitionTransaction,
)
from .exceptions import (
    NestedPrefixError,
    StateTransactionError,
)
from .metadata import (
    METADATA_CF_NAME,
    PROCESSED_OFFSET_KEY,
    CHANGELOG_OFFSET_KEY,
    PREFIX_SEPARATOR,
    CHANGELOG_CF_MESSAGE_HEADER,
)
from .serialization import (
    serialize,
    deserialize,
    int_to_int64_bytes,
)
from ..state import TransactionState

if TYPE_CHECKING:
    from .partition import RocksDBStorePartition

logger = logging.getLogger(__name__)


Undefined = NewType("Undefined", object)

_undefined = Undefined(object())
_deleted = Undefined(object())

_DEFAULT_PREFIX = b""


__all__ = ("RocksDBPartitionTransaction",)


def _validate_transaction_state(func):
    """
    Check that the state of `RocksDBTransaction` is valid before calling a method
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self: RocksDBPartitionTransaction = args[0]
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
        partition: "RocksDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
    ):
        """
        :param partition: instance of `RocksDBStatePartition` to be used for accessing
            the underlying RocksDB
        :param dumps: a function to serialize data to bytes.
        :param loads: a function to deserialize data from bytes.
        """
        self._partition = partition
        self._update_cache: Dict[str, Dict[bytes, Union[bytes, Undefined]]] = {}
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

    def _update_changelog(self, meta_cf_handle: ColumnFamily):
        logger.debug("Flushing state changes to the changelog topic...")
        offset = self._partition.get_changelog_offset() or 0

        for cf_name in self._update_cache:
            headers = {CHANGELOG_CF_MESSAGE_HEADER: cf_name}
            for k, v in self._update_cache[cf_name].items():
                self._partition.produce_to_changelog(
                    key=k, value=v if v is not _deleted else None, headers=headers
                )
                offset += 1

        self._batch.put(
            CHANGELOG_OFFSET_KEY, int_to_int64_bytes(offset), meta_cf_handle
        )
        logger.debug(f"Changelog offset set to {offset}")

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
                cf_handle = self._partition.get_column_family_handle(METADATA_CF_NAME)
                if offset is not None:
                    self._batch.put(
                        PROCESSED_OFFSET_KEY, int_to_int64_bytes(offset), cf_handle
                    )
                if self._partition.using_changelogs:
                    self._update_changelog(cf_handle)
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
