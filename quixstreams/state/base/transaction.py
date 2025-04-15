import enum
import functools
import logging
from abc import ABC
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from quixstreams.models import Headers
from quixstreams.state.exceptions import (
    InvalidChangelogOffset,
    StateSerializationError,
    StateTransactionError,
)
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    DEFAULT_PREFIX,
    SEPARATOR,
    Marker,
)
from quixstreams.state.serialization import DumpsFunc, LoadsFunc, deserialize, serialize
from quixstreams.utils.json import dumps as json_dumps

from .state import State, TransactionState

if TYPE_CHECKING:
    from quixstreams.state.recovery import ChangelogProducer

    from .partition import StorePartition

__all__ = (
    "PartitionTransactionStatus",
    "PartitionTransaction",
    "PartitionTransactionCache",
)

logger = logging.getLogger(__name__)


class PartitionTransactionCache:
    """
    A cache with the data updated in the current PartitionTransaction.
    It is used to read-your-own-writes before the transaction is committed to the Store.

    Internally, updates and deletes are separated into two separate structures
    to simplify the querying over them.
    """

    def __init__(self) -> None:
        # A map with updated keys in format {<cf>: {<prefix>: {<key>: <value>}}}
        # Note: "updates" are bucketed per prefix to speed up iterating over the
        # specific set of keys when we merge updates with data from the stores.
        # Using a prefix like that allows us to perform fewer iterations.
        self._updated: dict[str, dict[bytes, dict[bytes, bytes]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        # Dict of sets with deleted keys in format {<cf>: set[<key1>, <key2>]}
        # Deletes are stored without prefixes because we don't need to iterate over
        # them.
        self._deleted: dict[str, set[bytes]] = defaultdict(set)
        self._empty = True

    def get(
        self,
        key: bytes,
        prefix: bytes,
        cf_name: str = "default",
    ) -> Union[bytes, Marker]:
        """
        Get a value for the key.

        Returns the key value if it has been updated during the transaction.

        If the key has already been deleted, returns "DELETED" sentinel
        (we don't need to check the actual store).
        If the key is not present in the cache, returns "UNDEFINED sentinel
        (we need to check the store).

        :param: key: key as bytes
        :param: prefix: key prefix as bytes
        :param: cf_name: column family name
        """
        # Check if the key has been deleted
        if key in self._deleted[cf_name]:
            # The key is deleted and the store doesn't need to be checked
            return Marker.DELETED

        # Check if the key has been updated
        # If the key is not present in the cache, we need to check the store and return
        # UNDEFINED to signify that
        return self._updated[cf_name][prefix].get(key, Marker.UNDEFINED)

    def set(self, key: bytes, value: bytes, prefix: bytes, cf_name: str = "default"):
        """
        Set a value for the key.

        :param: key: key as bytes
        :param: value: value as bytes
        :param: prefix: key prefix as bytes
        :param: cf_name: column family name
        """
        self._updated[cf_name][prefix][key] = value
        self._deleted[cf_name].discard(key)
        self._empty = False

    def delete(self, key: Any, prefix: bytes, cf_name: str = "default"):
        """
        Delete a key.

        :param: key: key as bytes
        :param: prefix: key prefix as bytes
        :param: cf_name: column family name
        """
        self._updated[cf_name][prefix].pop(key, None)
        self._deleted[cf_name].add(key)
        self._empty = False

    def is_empty(self) -> bool:
        """
        Return True if any changes have been made (updates or deletes), otherwise
        return False.
        """
        return self._empty

    def get_column_families(self) -> Set[str]:
        """
        Get all update column families.
        """
        return set(self._updated.keys()) | set(self._deleted.keys())

    def get_updates(self, cf_name: str = "default") -> Dict[bytes, Dict[bytes, bytes]]:
        """
        Get all updated keys (excluding deleted)
        in the format "{<prefix>: {<key>: <value>}}".

        :param: cf_name: column family name
        """
        return self._updated.get(cf_name, {})

    def get_deletes(self, cf_name: str = "default") -> Set[bytes]:
        """
        Get all deleted keys (excluding updated) as a set.
        """
        return self._deleted[cf_name]


class PartitionTransactionStatus(enum.Enum):
    STARTED = 1  # Transaction is started and accepts updates

    PREPARED = 2  # Transaction is prepared, it can no longer receive updates
    # and can only be flushed

    COMPLETE = 3  # Transaction is fully completed, it cannot be used anymore

    FAILED = 4  # Transaction is failed, it cannot be used anymore


def validate_transaction_status(*allowed: PartitionTransactionStatus):
    """
    Check that the status of `RocksDBTransaction` is valid before calling a method
    """

    def wrapper(func):
        @functools.wraps(func)
        def _wrapper(tx: "PartitionTransaction", *args, **kwargs):
            if tx.status not in allowed:
                raise StateTransactionError(
                    f"Invalid transaction status {tx.status}, " f"allowed: {allowed}"
                )

            return func(tx, *args, **kwargs)

        return _wrapper

    return wrapper


K = TypeVar("K")
V = TypeVar("V")


class PartitionTransaction(ABC, Generic[K, V]):
    """
    A transaction class to perform simple key-value operations like
    "get", "set", "delete" and "exists" on a single storage partition.
    """

    def __init__(
        self,
        partition: "StorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"] = None,
    ) -> None:
        super().__init__()
        self._changelog_producer = changelog_producer
        self._status = PartitionTransactionStatus.STARTED

        self._dumps = dumps
        self._loads = loads
        self._partition = partition

        self._update_cache = PartitionTransactionCache()

    @property
    def changelog_producer(self) -> Optional["ChangelogProducer"]:
        return self._changelog_producer

    @property
    def status(self) -> PartitionTransactionStatus:
        return self._status

    @property
    def failed(self) -> bool:
        """
        Return `True` if transaction failed to update data at some point.

        Failed transactions cannot be re-used.
        :return: bool
        """
        return self._status == PartitionTransactionStatus.FAILED

    @property
    def completed(self) -> bool:
        """
        Return `True` if transaction is successfully completed.

        Completed transactions cannot be re-used.
        :return: bool
        """
        return self._status == PartitionTransactionStatus.COMPLETE

    @property
    def prepared(self) -> bool:
        """
        Return `True` if transaction is prepared completed.

        Prepared transactions cannot receive new updates, but can be flushed.
        :return: bool
        """
        return self._status == PartitionTransactionStatus.PREPARED

    @property
    def changelog_topic_partition(self) -> Optional[Tuple[str, int]]:
        """
        Return the changelog topic-partition for the StorePartition of this transaction.

        Returns `None` if changelog_producer is not provided.

        :return: (topic, partition) or None
        """
        if self.changelog_producer is None:
            return None

        return (
            self.changelog_producer.changelog_name,
            self.changelog_producer.partition,
        )

    def _serialize_value(self, value: V) -> bytes:
        return serialize(value, dumps=self._dumps)

    def _deserialize_value(self, value: bytes) -> V:
        return deserialize(value, loads=self._loads)

    def _serialize_key(self, key: K, prefix: bytes) -> bytes:
        key_bytes = serialize(key, dumps=self._dumps)
        prefix = prefix + SEPARATOR if prefix else b""
        return prefix + key_bytes

    def as_state(self, prefix: Any = DEFAULT_PREFIX) -> State[K, V]:
        """
        Create an instance implementing the `State` protocol to be provided
        to `StreamingDataFrame` functions.
        All operations called on this State object will be prefixed with
        the supplied `prefix`.

        :return: an instance implementing the `State` protocol
        """
        return TransactionState(
            transaction=self,
            prefix=(
                prefix
                if isinstance(prefix, bytes)
                else serialize(prefix, dumps=self._dumps)
            ),
        )

    @overload
    def get(
        self, key: K, prefix: bytes, *, cf_name: str = "default"
    ) -> Optional[V]: ...

    @overload
    def get(self, key: K, prefix: bytes, default: V, cf_name: str = "default") -> V: ...

    def get(
        self,
        key: K,
        prefix: bytes,
        default: Optional[V] = None,
        cf_name: str = "default",
    ) -> Optional[V]:
        """
        Get a key from the store.

        It returns `None` if the key is not found and `default` is not provided.

        :param key: key
        :param prefix: a key prefix
        :param default: default value to return if the key is not found
        :param cf_name: column family name
        :return: value or None if the key is not found and `default` is not provided
        """

        data = self._get_bytes(key, prefix, cf_name)
        if data is Marker.DELETED or data is Marker.UNDEFINED:
            return default

        return self._deserialize_value(data)

    @overload
    def get_bytes(
        self,
        key: K,
        prefix: bytes,
        default: Literal[None] = None,
        cf_name: str = "default",
    ) -> Optional[bytes]: ...

    @overload
    def get_bytes(
        self, key: K, prefix: bytes, default: bytes, cf_name: str = "default"
    ) -> bytes: ...

    def get_bytes(
        self,
        key: K,
        prefix: bytes,
        default: Optional[bytes] = None,
        cf_name: str = "default",
    ) -> Optional[bytes]:
        """
        Get a key from the store.

        It returns `None` if the key is not found and `default` is not provided.

        :param key: key
        :param prefix: a key prefix
        :param default: default value to return if the key is not found
        :param cf_name: column family name
        :return: value as bytes or None if the key is not found and `default` is not provided
        """
        data = self._get_bytes(key, prefix, cf_name)
        if data is Marker.DELETED or data is Marker.UNDEFINED:
            return default

        return data

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def _get_bytes(
        self,
        key: K,
        prefix: bytes,
        cf_name: str = "default",
    ) -> Union[bytes, Literal[Marker.DELETED, Marker.UNDEFINED]]:
        key_serialized = self._serialize_key(key, prefix=prefix)

        cached = self._update_cache.get(
            key=key_serialized, prefix=prefix, cf_name=cf_name
        )

        if cached is Marker.UNDEFINED:
            return self._partition.get(key_serialized, cf_name)

        return cached

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set(self, key: K, value: V, prefix: bytes, cf_name: str = "default") -> None:
        """
        Set value for the key.
        :param key: key
        :param prefix: a key prefix
        :param value: value
        :param cf_name: column family name
        """

        try:
            value_serialized = self._serialize_value(value)
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

        self.set_bytes(key, value_serialized, prefix, cf_name=cf_name)

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set_bytes(
        self, key: K, value: bytes, prefix: bytes, cf_name: str = "default"
    ) -> None:
        """
        Set bytes value for the key.
        :param key: key
        :param prefix: a key prefix
        :param value: value
        :param cf_name: column family name
        """
        try:
            if not isinstance(value, bytes):
                raise StateSerializationError("Value must be bytes")

            key_serialized = self._serialize_key(key, prefix=prefix)
            self._update_cache.set(
                key=key_serialized,
                value=value,
                prefix=prefix,
                cf_name=cf_name,
            )
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def delete(self, key: K, prefix: bytes, cf_name: str = "default"):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        :param prefix: a key prefix
        :param cf_name: column family name
        """
        try:
            key_serialized = self._serialize_key(key, prefix=prefix)
            self._update_cache.delete(
                key=key_serialized, prefix=prefix, cf_name=cf_name
            )
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def exists(self, key: K, prefix: bytes, cf_name: str = "default") -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :param prefix: a key prefix
        :param cf_name: column family name
        :return: True if key exists, False otherwise
        """
        key_serialized = self._serialize_key(key, prefix=prefix)
        cached = self._update_cache.get(
            key=key_serialized, prefix=prefix, cf_name=cf_name
        )
        if cached is Marker.DELETED:
            return False
        elif cached is not Marker.UNDEFINED:
            return True
        else:
            return self._partition.exists(key_serialized, cf_name=cf_name)

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offsets: Optional[dict[str, int]]):
        """
        Produce changelog messages to the changelog topic for all changes accumulated
        in this transaction and prepare transaction to flush its state to the state
        store.

        After successful `prepare()`, the transaction status is changed to PREPARED,
        and it cannot receive updates anymore.

        If changelog is disabled for this application, no updates will be produced
        to the changelog topic.

        :param processed_offsets: the dict with <topic: offset> of the latest processed message
        """

        try:
            self._prepare(processed_offsets=processed_offsets)
            self._status = PartitionTransactionStatus.PREPARED
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    def _prepare(self, processed_offsets: Optional[dict[str, int]]):
        if self._changelog_producer is None:
            return

        logger.debug(
            f"Flushing state changes to the changelog topic "
            f'topic_name="{self._changelog_producer.changelog_name}" '
            f"partition={self._changelog_producer.partition}"
        )
        source_tp_offset_header = json_dumps(processed_offsets)
        column_families = self._update_cache.get_column_families()

        for cf_name in column_families:
            headers: Headers = {
                CHANGELOG_CF_MESSAGE_HEADER: cf_name,
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER: source_tp_offset_header,
            }

            updates = self._update_cache.get_updates(cf_name=cf_name)
            for prefix_update_cache in updates.values():
                for key, value in prefix_update_cache.items():
                    self._changelog_producer.produce(
                        key=key,
                        value=value,
                        headers=headers,
                    )

            deletes = self._update_cache.get_deletes(cf_name=cf_name)
            for key in deletes:
                self._changelog_producer.produce(
                    key=key,
                    value=None,
                    headers=headers,
                )

    @validate_transaction_status(
        PartitionTransactionStatus.STARTED, PartitionTransactionStatus.PREPARED
    )
    def flush(
        self,
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

        :param changelog_offset: offset of the last produced changelog message,
            optional.
        """
        try:
            self._flush(changelog_offset)
            self._status = PartitionTransactionStatus.COMPLETE
        except Exception:
            self._status = PartitionTransactionStatus.FAILED
            raise

    def _flush(self, changelog_offset: Optional[int]):
        if self._update_cache.is_empty():
            return

        if changelog_offset is not None:
            current_changelog_offset = self._partition.get_changelog_offset()
            if (
                current_changelog_offset is not None
                and changelog_offset < current_changelog_offset
            ):
                raise InvalidChangelogOffset(
                    "Cannot set changelog offset lower than already saved one"
                )

        self._partition.write(
            cache=self._update_cache,
            changelog_offset=changelog_offset,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None and not self.failed:
            self.flush()
