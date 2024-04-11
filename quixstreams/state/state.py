from typing import Any, Optional

from .types import State, PartitionTransaction


class TransactionState(State):
    __slots__ = (
        "_transaction",
        "_prefix",
    )

    def __init__(self, prefix: bytes, transaction: PartitionTransaction):
        """
        Simple key-value state to be provided into `StreamingDataFrame` functions

        :param transaction: instance of `PartitionTransaction`
        """
        self._prefix = prefix
        self._transaction = transaction

    def get(self, key: Any, default: Any = None) -> Optional[Any]:
        """
        Get the value for key if key is present in the state, else default

        :param key: key
        :param default: default value to return if the key is not found
        :return: value or None if the key is not found and `default` is not provided
        """
        return self._transaction.get(key=key, prefix=self._prefix, default=default)

    def set(self, key: Any, value: Any):
        """
        Set value for the key.
        :param key: key
        :param value: value
        """
        return self._transaction.set(key=key, value=value, prefix=self._prefix)

    def delete(self, key: Any):
        """
        Delete value for the key.

        This function always returns `None`, even if value is not found.
        :param key: key
        """
        return self._transaction.delete(key=key, prefix=self._prefix)

    def exists(self, key: Any) -> bool:
        """
        Check if the key exists in state.
        :param key: key
        :return: True if key exists, False otherwise
        """

        return self._transaction.exists(key=key, prefix=self._prefix)
