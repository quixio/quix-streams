import logging
import shutil
from pathlib import Path
from typing import List, Dict, Optional

from quixstreams.rowproducer import RowProducer
from .exceptions import (
    StoreNotRegisteredError,
    PartitionStoreIsUsed,
    WindowedStoreAlreadyRegisteredError,
)
from .recovery import RecoveryManager, ChangelogProducerFactory
from .rocksdb import RocksDBStore, RocksDBOptionsType
from .rocksdb.windowed.store import WindowedRocksDBStore
from .types import Store, StorePartition

__all__ = ("StateStoreManager", "DEFAULT_STATE_STORE_NAME")

logger = logging.getLogger(__name__)

DEFAULT_STATE_STORE_NAME = "default"


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
        producer: Optional[RowProducer] = None,
        recovery_manager: Optional[RecoveryManager] = None,
    ):
        self._state_dir = (Path(state_dir) / group_id).absolute()
        self._rocksdb_options = rocksdb_options
        self._stores: Dict[str, Dict[str, Store]] = {}
        self._producer = producer
        self._recovery_manager = recovery_manager

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

    @property
    def recovery_required(self) -> bool:
        """
        Whether recovery needs to be done.
        """
        if self._recovery_manager:
            return self._recovery_manager.has_assignments
        return False

    @property
    def using_changelogs(self) -> bool:
        """
        Whether the StateStoreManager is using changelog topics

        :return: using changelogs, as bool
        """
        return bool(self._recovery_manager)

    def do_recovery(self):
        """
        Perform a state recovery, if necessary.
        """
        return self._recovery_manager.do_recovery()

    def stop_recovery(self):
        """
        Stop recovery (called during app shutdown).
        """
        return self._recovery_manager.stop_recovery()

    def get_store(
        self, topic: str, store_name: str = DEFAULT_STATE_STORE_NAME
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

    def _setup_changelogs(
        self, topic_name: str, store_name: str
    ) -> ChangelogProducerFactory:
        if self._recovery_manager:
            logger.debug(
                f'State Manager: registering changelog for store "{store_name}" '
                f'(topic "{topic_name}")'
            )
            changelog_topic = self._recovery_manager.register_changelog(
                topic_name=topic_name,
                store_name=store_name,
            )
            return ChangelogProducerFactory(
                changelog_name=changelog_topic.name,
                producer=self._producer,
            )

    def register_store(
        self, topic_name: str, store_name: str = DEFAULT_STATE_STORE_NAME
    ):
        """
        Register a state store to be managed by StateStoreManager.

        During processing, the StateStoreManager will react to rebalancing callbacks
        and assign/revoke the partitions for registered stores.

        Each store can be registered only once for each topic.

        :param topic_name: topic name
        :param store_name: store name
        """
        if self._stores.get(topic_name, {}).get(store_name) is None:
            self._stores.setdefault(topic_name, {})[store_name] = RocksDBStore(
                name=store_name,
                topic=topic_name,
                base_dir=str(self._state_dir),
                changelog_producer_factory=self._setup_changelogs(
                    topic_name, store_name
                ),
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
            changelog_producer_factory=self._setup_changelogs(topic_name, store_name),
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

    def on_partition_assign(
        self, topic: str, partition: int, committed_offset: int
    ) -> List[StorePartition]:
        """
        Assign store partitions for each registered store for the given `TopicPartition`
        and return a list of assigned `StorePartition` objects.

        :param topic: Kafka topic name
        :param partition: Kafka topic partition
        :param committed_offset: latest committed offset for the partition
        :return: list of assigned `StorePartition`
        """

        store_partitions = {}
        for name, store in self._stores.get(topic, {}).items():
            store_partition = store.assign_partition(partition)
            store_partitions[name] = store_partition
        if self._recovery_manager and store_partitions:
            self._recovery_manager.assign_partition(
                topic=topic,
                partition=partition,
                committed_offset=committed_offset,
                store_partitions=store_partitions,
            )
        return list(store_partitions.values())

    def on_partition_revoke(self, topic: str, partition: int):
        """
        Revoke store partitions for each registered store for the given `TopicPartition`

        :param topic: Kafka topic name
        :param partition: Kafka topic partition
        """
        if stores := self._stores.get(topic, {}).values():
            if self._recovery_manager:
                self._recovery_manager.revoke_partition(partition_num=partition)
            for store in stores:
                store.revoke_partition(partition=partition)

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

    def __enter__(self):
        self.init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
