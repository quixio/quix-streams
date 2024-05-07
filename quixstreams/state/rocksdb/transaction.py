import functools
import logging
from typing import Any, Union, Optional, Dict, NewType, TYPE_CHECKING, Tuple

from rocksdict import WriteBatch
from quixstreams.utils.json import dumps as json_dumps
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.types import (
    DumpsFunc,
    LoadsFunc,
    PartitionTransaction,
    PartitionTransactionStatus,
)
from .exceptions import StateTransactionError, InvalidChangelogOffset
from .metadata import (
    METADATA_CF_NAME,
    PROCESSED_OFFSET_KEY,
    CHANGELOG_OFFSET_KEY,
    PREFIX_SEPARATOR,
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER,
)
from .serialization import serialize, deserialize, int_to_int64_bytes
from ..state import TransactionState

if TYPE_CHECKING:
    from .partition import RocksDBStorePartition

__all__ = ("RocksDBPartitionTransaction", "DEFAULT_PREFIX", "DELETED")

logger = logging.getLogger(__name__)

Undefined = NewType("Undefined", object)

UNDEFINED = Undefined(object())
DELETED = Undefined(object())

DEFAULT_PREFIX = b""


def _validate_transaction_status(*allowed: PartitionTransactionStatus):
    """
    Check that the status of `RocksDBTransaction` is valid before calling a method
    """

    def wrapper(func):
        @functools.wraps(func)
        def _wrapper(tx: "RocksDBPartitionTransaction", *args, **kwargs):
            if tx.status not in allowed:
                raise StateTransactionError(
                    f"Invalid transaction status {tx.status}, " f"allowed: {allowed}"
                )

            return func(tx, *args, **kwargs)

        return _wrapper

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
    Methods `get()`, `set()`, `delete()` and `exists()` methods require prefixes for
    the keys.
    Normally, the Kafka message keys are supposed to be used as prefixes.

    Transactional properties
    ************************
    `RocksDBTransaction` uses a combination of in-memory update cache
    and RocksDB's WriteBatch in order to accumulate all the state mutations
    in a single batch, flush them atomically, and allow the updates be visible
    within the transaction before it's flushed (aka "read-your-own-writes" problem).

    If any mutation fails during the transaction
    (e.g., failed to write the updates to the RocksDB), the whole transaction
    will be marked as failed and cannot be used anymore.
    In this case, a new `RocksDBTransaction` should be created.

    `RocksDBTransaction` can be used only once.
    """

    __slots__ = (
        "_partition",
        "_update_cache",
        "_batch",
        "_dumps",
        "_loads",
        "_status",
    )

    def __init__(
        self,
        partition: "RocksDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional[ChangelogProducer] = None,
    ):
        """
        :param partition: instance of `RocksDBStatePartition` to be used for accessing
            the underlying RocksDB
        :param dumps: a function to serialize data to bytes.
        :param loads: a function to deserialize data from bytes.
        """
        self._partition = partition
        self._update_cache: Dict[
            str, Dict[bytes, Dict[bytes, Union[bytes, Undefined]]]
        ] = {"default": {}}
        self._batch = WriteBatch(raw_mode=True)
        self._dumps = dumps
        self._loads = loads
        self._status = PartitionTransactionStatus.STARTED
        self._changelog_producer = changelog_producer

    @_validate_transaction_status(PartitionTransactionStatus.STARTED)
    def get(
        self, key: Any, prefix: bytes, default: Any = None, cf_name: str = "default"
    ) -> Optional[Any]:
        """
        Get a key from the store.

        It first looks up the key in the update cache in case it has been updated
        but not flushed yet.

        It returns `None` if the key is not found and `default` is not provided.

        :param key: a key to get from DB
        :param prefix: a key prefix
        :param default: value to return if the key is not present in the state.
            It can be of any type.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: value or `default`
        """

        # First, check the update cache in case the value was previously written
        # Use _undefined sentinel as default because the actual value can be "None"
        key_serialized = self._serialize_key(key, prefix=prefix)
        cached = (
            self._update_cache.get(cf_name, {})
            .get(prefix, {})
            .get(key_serialized, UNDEFINED)
        )
        if cached is DELETED:
            return default

        if cached is not UNDEFINED:
            return self._deserialize_value(cached)

        # The value is not found in cache, check the db
        stored = self._partition.get(key_serialized, UNDEFINED, cf_name=cf_name)
        if stored is not UNDEFINED:
            return self._deserialize_value(stored)
        return default

    @_validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set(self, key: Any, value: Any, prefix: bytes, cf_name: str = "default"):
        """
        Set a key to the store.

        It first updates the key in the update cache.

        :param key: key to store in DB
        :param prefix: a key prefix
        :param value: value to store in DB
        :param cf_name: rocksdb column family name. Default - "default"
        """

        try:
            key_serialized = self._serialize_key(key, prefix=prefix)
            value_serialized = self._serialize_value(value)
            self._update_cache.setdefault(cf_name, {}).setdefault(prefix, {})[
                key_serialized
            ] = value_serialized
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    @_validate_transaction_status(PartitionTransactionStatus.STARTED)
    def delete(self, key: Any, prefix: bytes, cf_name: str = "default"):
        """
        Delete a key from the store.

        It first deletes the key from the update cache.

        :param key: a key to delete from DB
        :param prefix: a key prefix
        :param cf_name: rocksdb column family name. Default - "default"
        """
        try:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._update_cache.setdefault(cf_name, {}).setdefault(prefix, {})[
                key_serialized
            ] = DELETED

        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    @_validate_transaction_status(PartitionTransactionStatus.STARTED)
    def exists(self, key: Any, prefix: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key exists in the store.

        It first looks up the key in the update cache.

        :param key: a key to check in DB
        :param prefix: a key prefix
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key exists, `False` otherwise.
        """

        key_serialized = self._serialize_key(key, prefix=prefix)
        cached = (
            self._update_cache.get(cf_name, {})
            .get(prefix, {})
            .get(key_serialized, UNDEFINED)
        )
        if cached is DELETED:
            return False

        if cached is not UNDEFINED:
            return True

        return self._partition.exists(key_serialized, cf_name=cf_name)

    @_validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offset: int):
        """
        Produce changelog messages to the changelog topic for all changes accumulated
        in this transaction and prepare transaction to flush its state to the state
        store.

        After successful `prepare()`, the transaction status is changed to PREPARED,
        and it cannot receive updates anymore.

        If changelog is disabled for this application, no updates will be produced
        to the changelog topic.

        :param processed_offset: the offset of the latest processed message
        """
        try:
            self._produce_changelog(processed_offset=processed_offset)
            self._status = PartitionTransactionStatus.PREPARED
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    @_validate_transaction_status(
        PartitionTransactionStatus.STARTED, PartitionTransactionStatus.PREPARED
    )
    def flush(
        self,
        processed_offset: Optional[int] = None,
        changelog_offset: Optional[int] = None,
    ):
        """
        Flush the recent updates to the database.
        It writes the WriteBatch to RocksDB and marks itself as finished.

        If writing fails, the transaction is marked as failed and
        cannot be used anymore.

        >***NOTE:*** If no keys have been modified during the transaction
            (i.e. no "set" or "delete" have been called at least once), it will
            not flush ANY data to the database including the offset to optimize
            I/O.

        :param processed_offset: offset of the last processed message, optional.
        :param changelog_offset: offset of the last produced changelog message,
            optional.
        """
        try:
            self._flush_state(
                processed_offset=processed_offset, changelog_offset=changelog_offset
            )
            self._status = PartitionTransactionStatus.COMPLETE
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    @property
    def status(self) -> PartitionTransactionStatus:
        return self._status

    @property
    def completed(self) -> bool:
        """
        Check if the transaction is completed.

        It doesn't indicate whether transaction is successful or not.
        Use `RocksDBTransaction.failed` for that.

        The completed transaction should not be re-used.

        :return: `True` if transaction is completed, `False` otherwise.
        """
        return self._status == PartitionTransactionStatus.COMPLETE

    @property
    def prepared(self) -> bool:
        """
        Check if the transaction is in PREPARED status.

        Prepared transaction successfully flushed its changelog and cannot receive
        updates anymore, but its state is not yet flushed to the disk

        :return: `True` if transaction is prepared, `False` otherwise.
        """
        return self._status == PartitionTransactionStatus.PREPARED

    @property
    def failed(self) -> bool:
        """
        Check if the transaction has failed.

        The failed transaction should not be re-used because the update cache
        and

        :return: `True` if transaction is failed, `False` otherwise.
        """
        return self._status == PartitionTransactionStatus.FAILED

    @property
    def changelog_topic_partition(self) -> Optional[Tuple[str, int]]:
        """
        Return the changelog topic-partition for the StorePartition of this transaction.

        Returns `None` if changelog_producer is not provided.

        :return: (topic, partition) or None
        """
        if self._changelog_producer is not None:
            return (
                self._changelog_producer.changelog_name,
                self._changelog_producer.partition,
            )

    def as_state(self, prefix: Any = DEFAULT_PREFIX) -> TransactionState:
        """
        Create a one-time use `TransactionState` object with a limited CRUD interface
        to be provided to `StreamingDataFrame` operations.

        The `TransactionState` will prefix all the keys with the supplied `prefix`
        for all underlying operations.

        :param prefix: a prefix to be used for all keys
        :return: an instance of `TransactionState`
        """
        return TransactionState(
            transaction=self,
            prefix=(
                prefix
                if isinstance(prefix, bytes)
                else serialize(prefix, dumps=self._dumps)
            ),
        )

    def _produce_changelog(self, processed_offset: Optional[int] = None):
        changelog_producer = self._changelog_producer
        if changelog_producer is None:
            return

        changelog_topic, partition = (
            changelog_producer.changelog_name,
            changelog_producer.partition,
        )
        logger.debug(
            f"Flushing state changes to the changelog topic "
            f'topic_name="{changelog_topic}" '
            f"partition={partition} "
            f"processed_offset={processed_offset}"
        )
        # Iterate over the transaction update cache
        for cf_name, cf_update_cache in self._update_cache.items():
            source_tp_offset_header = json_dumps(processed_offset)
            headers = {
                CHANGELOG_CF_MESSAGE_HEADER: cf_name,
                CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER: source_tp_offset_header,
            }
            for _prefix, prefix_update_cache in cf_update_cache.items():
                for key, value in prefix_update_cache.items():
                    # Produce changes to the changelog topic
                    self._changelog_producer.produce(
                        key=key,
                        value=value if value is not DELETED else None,
                        headers=headers,
                    )

    def _flush_state(
        self,
        processed_offset: Optional[int] = None,
        changelog_offset: Optional[int] = None,
    ):
        meta_cf_handle = self._partition.get_column_family_handle(METADATA_CF_NAME)
        # Iterate over the transaction update cache
        for cf_name, cf_update_cache in self._update_cache.items():
            cf_handle = self._partition.get_column_family_handle(cf_name)
            for _prefix, prefix_update_cache in cf_update_cache.items():
                for key, value in prefix_update_cache.items():
                    # Apply changes to the Writebatch
                    if value is DELETED:
                        self._batch.delete(key, cf_handle)
                    else:
                        self._batch.put(key, value, cf_handle)

        if not len(self._batch):
            # Exit early if transaction doesn't update anything
            return

        # Save the latest processed input topic offset
        if processed_offset is not None:
            self._batch.put(
                PROCESSED_OFFSET_KEY,
                int_to_int64_bytes(processed_offset),
                meta_cf_handle,
            )
        # Save the latest changelog topic offset to know where to recover from
        # It may be None if changelog topics are disabled
        if changelog_offset is not None:
            current_changelog_offset = self._partition.get_changelog_offset()
            if (
                current_changelog_offset is not None
                and changelog_offset < current_changelog_offset
            ):
                raise InvalidChangelogOffset(
                    f"Cannot set changelog offset lower than already saved one"
                )
            self._batch.put(
                CHANGELOG_OFFSET_KEY,
                int_to_int64_bytes(changelog_offset),
                meta_cf_handle,
            )
        logger.debug(
            f"Flushing state changes to the disk "
            f'path="{self._partition.path}" '
            f"processed_offset={processed_offset} "
            f"changelog_offset={changelog_offset}"
        )
        self._partition.write(self._batch)

    def _serialize_value(self, value: Any) -> bytes:
        return serialize(value, dumps=self._dumps)

    def _deserialize_value(self, value: bytes) -> Any:
        return deserialize(value, loads=self._loads)

    def _serialize_key(self, key: Any, prefix: bytes) -> bytes:
        key_bytes = serialize(key, dumps=self._dumps)
        prefix = prefix + PREFIX_SEPARATOR if prefix else b""
        return prefix + key_bytes

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Note: with state transactions, context manager interface is meant
        to be used mostly in unit tests.

        Normally, the Checkpoint class is responsible for managing and flushing
        the transactions.
        """

        if exc_val is None and not self.failed:
            self.flush()
