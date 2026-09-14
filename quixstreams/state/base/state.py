import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, Literal, Optional, TypeVar, overload

if TYPE_CHECKING:
    from .transaction import PartitionTransaction

__all__ = ("State", "TransactionState")

logger = logging.getLogger(__name__)


K = TypeVar("K")
V = TypeVar("V")


class State(ABC, Generic[K, V]):
    """
    Primary interface for working with key-value state data from `StreamingDataFrame`
    """

    @overload
    def get(self, key: K, default: Literal[None] = None) -> Optional[V]: ...

    @overload
    def get(self, key: K, default: V) -> V: ...

    @abstractmethod
    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        ...

    @overload
    def get_bytes(self, key: K, default: Literal[None] = None) -> Optional[bytes]: ...

    @overload
    def get_bytes(self, key: K, default: bytes) -> bytes: ...

    def get_bytes(self, key: K, default: Optional[bytes] = None) -> Optional[bytes]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param default: default value to return if the key is not found
        :return: value as bytes or None if the key is not found and `default` is not provided
        """

    @abstractmethod
    def set(self, key: K, value: V, ttl: Optional[timedelta] = None) -> None:
        """
        Set value for the key, optionally with a per-write expiry.

        :param key: key
        :param value: value
        :param ttl: optional event-time TTL. When set, the entry expires
            ``ttl`` after the current record's event-time and is filtered
            from subsequent reads. ``None`` (default) writes a sentinel
            stamp meaning "never expires", overwriting any prior TTL on
            the same key.
        """
        ...

    @abstractmethod
    def set_bytes(self, key: K, value: bytes, ttl: Optional[timedelta] = None) -> None:
        """
        Set bytes value for the key, optionally with a per-write expiry.

        :param key: key
        :param value: value as bytes
        :param ttl: see :meth:`set`.
        """
        ...

    @abstractmethod
    def delete(self, key: K):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        """
        ...

    @abstractmethod
    def exists(self, key: K) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :return: True if key exists, False otherwise
        """
        ...


class TransactionState(State):
    __slots__ = (
        "_transaction",
        "_prefix",
        "_timestamp",
    )

    def __init__(
        self,
        prefix: bytes,
        transaction: "PartitionTransaction",
        timestamp: Optional[int] = None,
    ):
        """
        Simple key-value state to be provided into `StreamingDataFrame` functions

        :param transaction: instance of `PartitionTransaction`
        :param prefix: serialized key prefix shared across calls
        :param timestamp: optional event-time of the current record (ms).
            Used by TTL-aware partitions to stamp values on ``set()`` with
            ``record.timestamp + ttl`` and to filter expired entries on
            ``get()``. The framework injects this on every record via the
            ``StreamingDataFrame`` stateful wrapper.
        """
        self._prefix = prefix
        self._transaction = transaction
        self._timestamp = timestamp

    @overload
    def get(self, key: K, default: Literal[None] = None) -> Optional[V]: ...

    @overload
    def get(self, key: K, default: V) -> V: ...

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        return self._transaction.get(
            key=key,
            prefix=self._prefix,
            default=default,
            timestamp=self._timestamp,
        )

    @overload
    def get_bytes(self, key: K, default: Literal[None] = None) -> Optional[bytes]: ...

    @overload
    def get_bytes(self, key: K, default: bytes) -> bytes: ...

    def get_bytes(self, key: K, default: Optional[bytes] = None) -> Optional[bytes]:
        """
        Get the bytes value for key if key is present in the state, else default

        :param key: key
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        return self._transaction.get_bytes(
            key=key,
            prefix=self._prefix,
            default=default,
            timestamp=self._timestamp,
        )

    def set(self, key: K, value: V, ttl: Optional[timedelta] = None) -> None:
        """
        Set value for the key, optionally with a per-write expiry.

        :param key: key
        :param value: value
        :param ttl: optional event-time TTL. See :class:`State.set`.
        """
        return self._transaction.set(
            key=key,
            value=value,
            prefix=self._prefix,
            timestamp=self._timestamp,
            ttl=ttl,
        )

    def set_bytes(self, key: K, value: bytes, ttl: Optional[timedelta] = None) -> None:
        """
        Set bytes value for the key, optionally with a per-write expiry.

        :param key: key
        :param value: value as bytes
        :param ttl: optional event-time TTL. See :class:`State.set`.
        """
        return self._transaction.set_bytes(
            key=key,
            value=value,
            prefix=self._prefix,
            timestamp=self._timestamp,
            ttl=ttl,
        )

    def delete(self, key: K):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        """
        return self._transaction.delete(key=key, prefix=self._prefix)

    def exists(self, key: K) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :return: True if key exists, False otherwise
        """

        return self._transaction.exists(
            key=key, prefix=self._prefix, timestamp=self._timestamp
        )
