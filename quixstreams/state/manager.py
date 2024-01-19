import contextlib
import logging
import shutil
from pathlib import Path
from typing import List, Dict, Optional, Iterator

from quixstreams.types import TopicPartition
from .exceptions import (
    StoreNotRegisteredError,
    InvalidStoreTransactionStateError,
    PartitionStoreIsUsed,
    WindowedStoreAlreadyRegisteredError,
)
from .rocksdb import RocksDBStore, RocksDBOptionsType
from .rocksdb.windowed.store import WindowedRocksDBStore
from .types import (
    Store,
    PartitionTransaction,
    StorePartition,
)

__all__ = ("StateStoreManager",)

logger = logging.getLogger(__name__)

_DEFAULT_STATE_STORE_NAME = "default"


class StateStoreManager:
    """
    Class for managing state stores and partitions.

    StateStoreManager is responsible for:
     - reacting to rebalance callbacks
     - managing the individual state stores
     - providing access to store transactions
    """

    def __init__(
        self,
        group_id: str,
        state_dir: str,
        rocksdb_options: Optional[RocksDBOptionsType] = None,
    ):
        self._group_id = group_id
        self._state_dir = (Path(state_dir) / group_id).absolute()
        self._rocksdb_options = rocksdb_options
        self._stores: Dict[str, Dict[str, Store]] = {}
        self._transaction: Optional[_MultiStoreTransaction] = None

    def _init_state_dir(self):
        logger.info(f'Initializing state directory at "{self._state_dir}"')
        if self._state_dir.exists():
            if not self._state_dir.is_dir():
                raise FileExistsError(
                    f'Path "{self._state_dir}" already exists, '
                    f"but it is not a directory"
                )
            logger.debug(f'State directory already exists at "{self._state_dir}"')
        else:
            self._state_dir.mkdir(parents=True)
            logger.debug(f'Created state directory at "{self._state_dir}"')

    @property
    def stores(self) -> Dict[str, Dict[str, Store]]:
        """
        Map of registered state stores
        :return: dict in format {topic: {store_name: store}}
        """
        return self._stores

    def get_store(
        self, topic: str, store_name: str = _DEFAULT_STATE_STORE_NAME
    ) -> Store:
        """
        Get a store for given name and topic
        :param topic: topic name
        :param store_name: store name
        :return: instance of `Store`
        """
        store = self._stores.get(topic, {}).get(store_name)
        if store is None:
            raise StoreNotRegisteredError(
                f'Store "{store_name}" (topic "{topic}") is not registered'
            )
        return store

    def register_store(
        self, topic_name: str, store_name: str = _DEFAULT_STATE_STORE_NAME
    ):
        """
        Register a state store to be managed by StateStoreManager.

        During processing, the StateStoreManager will react to rebalancing callbacks
        and assign/revoke the partitions for registered stores.

        Each store can be registered only once for each topic.

        :param topic_name: topic name
        :param store_name: store name
        """
        store = self._stores.get(topic_name, {}).get(store_name)
        if store is None:
            self._stores.setdefault(topic_name, {})[store_name] = RocksDBStore(
                name=store_name,
                topic=topic_name,
                base_dir=str(self._state_dir),
                options=self._rocksdb_options,
            )

    def register_windowed_store(self, topic_name: str, store_name: str):
        """
        Register a windowed state store to be managed by StateStoreManager.

        During processing, the StateStoreManager will react to rebalancing callbacks
        and assign/revoke the partitions for registered stores.

        Each window store can be registered only once for each topic.

        :param topic_name: topic name
        :param store_name: store name
        """
        store = self._stores.get(topic_name, {}).get(store_name)
        if store:
            raise WindowedStoreAlreadyRegisteredError()

        self._stores.setdefault(topic_name, {})[store_name] = WindowedRocksDBStore(
            name=store_name,
            topic=topic_name,
            base_dir=str(self._state_dir),
            options=self._rocksdb_options,
        )

    def clear_stores(self):
        """
        Delete all state stores managed by StateStoreManager.
        """
        if any(
            store.partitions
            for topic_stores in self._stores.values()
            for store in topic_stores.values()
        ):
            raise PartitionStoreIsUsed(
                "Cannot clear stores with active partitions assigned"
            )

        shutil.rmtree(self._state_dir)

    def on_partition_assign(self, tp: TopicPartition) -> List[StorePartition]:
        """
        Assign store partitions for each registered store for the given `TopicPartition`
        and return a list of assigned `StorePartition` objects.

        :param tp: `TopicPartition` from Kafka consumer
        :return: list of assigned `StorePartition`
        """

        store_partitions = []
        for store in self._stores.get(tp.topic, {}).values():
            store_partitions.append(store.assign_partition(tp.partition))
        return store_partitions

    def on_partition_revoke(self, tp: TopicPartition):
        """
        Revoke store partitions for each registered store for the given `TopicPartition`

        :param tp: `TopicPartition` from Kafka consumer
        """
        for store in self._stores.get(tp.topic, {}).values():
            store.revoke_partition(tp.partition)

    def on_partition_lost(self, tp: TopicPartition):
        """
        Revoke and close store partitions for each registered store for the given
        `TopicPartition`

        :param tp: `TopicPartition` from Kafka consumer
        """
        for store in self._stores.get(tp.topic, {}).values():
            store.revoke_partition(tp.partition)

    def init(self):
        """
        Initialize `StateStoreManager` and create a store directory
        :return:
        """
        self._init_state_dir()

    def close(self):
        """
        Close all registered stores
        """
        for topic_stores in self._stores.values():
            for store in topic_stores.values():
                store.close()

    def get_store_transaction(
        self, store_name: str = _DEFAULT_STATE_STORE_NAME
    ) -> PartitionTransaction:
        """
        Get active `PartitionTransaction` for the store
        :param store_name:
        :return:
        """
        if self._transaction is None:
            raise InvalidStoreTransactionStateError(
                "Store transaction is not started yet"
            )
        return self._transaction.get_store_transaction(store_name=store_name)

    @contextlib.contextmanager
    def start_store_transaction(
        self, topic: str, partition: int, offset: int
    ) -> Iterator["_MultiStoreTransaction"]:
        """
        Starting the multi-store transaction for the Kafka message.

        This transaction will keep track of all used stores and flush them in the end.
        If any exception is catched during this transaction, none of them
        will be flushed as a best effort to keep stores consistent in "at-least-once" setting.

        There can be only one active transaction at a time. Starting a new transaction
        before the end of the current one will fail.


        :param topic: message topic
        :param partition: message partition
        :param offset: message offset
        """
        if not self._stores.get(topic):
            raise StoreNotRegisteredError(
                f'Topic "{topic}" does not have stores registered'
            )

        if self._transaction is not None:
            raise InvalidStoreTransactionStateError(
                "Another transaction is already in progress"
            )
        self._transaction = _MultiStoreTransaction(
            manager=self, topic=topic, partition=partition, offset=offset
        )
        try:
            yield self._transaction
            self._transaction.flush()
        finally:
            self._transaction = None

    def __enter__(self):
        self.init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class _MultiStoreTransaction:
    """
    A transaction-like class to manage flushing of multiple state partitions for each
    processed message.

    It is responsible for:
    - Keeping track of actual DBTransactions for the individiual stores
    - Flushing of the opened transactions in the end

    """

    def __init__(
        self, manager: "StateStoreManager", topic: str, partition: int, offset: int
    ):
        self._manager = manager
        self._transactions: Dict[str, PartitionTransaction] = {}
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def get_store_transaction(
        self, store_name: str = _DEFAULT_STATE_STORE_NAME
    ) -> PartitionTransaction:
        """
        Get a PartitionTransaction for the given store

        It will return already started transaction if there's one.

        :param store_name: store name
        :return: instance of `PartitionTransaction`
        """
        transaction = self._transactions.get(store_name)
        if transaction is not None:
            return transaction

        store = self._manager.get_store(topic=self._topic, store_name=store_name)
        transaction = store.start_partition_transaction(partition=self._partition)
        self._transactions[store_name] = transaction
        return transaction

    def flush(self):
        """
        Flush all `PartitionTransaction` instances for each registered store and
        save the last processed offset for each partition.

        Empty transactions without any updates will not be flushed.

        If there are any failed transactions, no transactions will be flushed
        to keep the stores consistent.
        """
        for store_name, transaction in self._transactions.items():
            if transaction.failed:
                logger.warning(
                    f'Detected failed transaction for store "{store_name}" '
                    f'(topic "{self._topic}" partition "{self._partition}" '
                    f'offset "{self._offset}), state transactions will not be flushed"'
                )
                return

        for transaction in self._transactions.values():
            transaction.maybe_flush(offset=self._offset)
