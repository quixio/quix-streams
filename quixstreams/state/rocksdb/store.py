import logging
from pathlib import Path
from typing import Dict, Optional

from quixstreams.state.exceptions import PartitionNotAssignedError
from quixstreams.state.types import Store
from .partition import (
    RocksDBStorePartition,
    RocksDBPartitionTransaction,
)
from .types import RocksDBOptionsType

logger = logging.getLogger(__name__)

__all__ = ("RocksDBStore",)


class RocksDBStore(Store):
    def __init__(
        self,
        name: str,
        topic: str,
        base_dir: str,
        options: Optional[RocksDBOptionsType] = None,
    ):
        """
        RocksDB-based state store.

        It keeps track of individual store partitions and provides access to the
        partitions' transactions.

        :param name: a unique store name
        :param topic: a topic name for this store
        :param base_dir: path to a directory with the state
        :param options: RocksDB options. If `None`, the default options will be used.
        """
        self._name = name
        self._topic = topic
        self._partitions_dir = Path(base_dir).absolute() / self._name / self._topic
        self._transactions: Dict[int, RocksDBPartitionTransaction] = {}
        self._partitions: Dict[int, RocksDBStorePartition] = {}
        self._options = options

    @property
    def topic(self) -> str:
        """
        Store topic name
        """
        return self._topic

    @property
    def name(self) -> str:
        """
        Store name
        """
        return self._name

    @property
    def partitions(self) -> Dict[int, RocksDBStorePartition]:
        """
        Mapping of assigned store partitions
        """
        return self._partitions

    def create_new_partition(self, path: str) -> RocksDBStorePartition:
        return RocksDBStorePartition(path=path, options=self._options)

    def assign_partition(self, partition: int) -> RocksDBStorePartition:
        """
        Open and assign store partition.

        If the partition is already assigned, it will not re-open it and return
        the existing partition instead.

        :param partition: partition number
        :return: instance of`RocksDBStorePartition`
        """
        if partition in self._partitions:
            logger.debug(
                f'Partition "{partition}" for store "{self._name}" '
                f'(topic "{self._topic}") '
                f"is already assigned"
            )
            return self._partitions[partition]

        path = str((self._partitions_dir / str(partition)).absolute())
        store_partition = self.create_new_partition(path)

        self._partitions[partition] = store_partition
        logger.debug(
            f'Assigned store partition "%s[%s]" (topic "%s")',
            self._name,
            partition,
            self._topic,
        )
        return store_partition

    def revoke_partition(self, partition: int):
        """
        Revoke and close the assigned store partition.

        If the partition is not assigned, it will log the message and return.

        :param partition: partition number
        """
        store_partition = self._partitions.get(partition)
        if store_partition is None:
            return

        store_partition.close()
        self._partitions.pop(partition)
        logger.debug(
            'Revoked store partition "%s[%s]" topic("%s")',
            self._name,
            partition,
            self._topic,
        )

    def start_partition_transaction(
        self, partition: int
    ) -> RocksDBPartitionTransaction:
        """
        Start a new partition transaction.

        `RocksDBPartitionTransaction` is the primary interface for working with data in
        the underlying RocksDB.

        :param partition: partition number
        :return: instance of `RocksDBPartitionTransaction`
        """
        if partition not in self._partitions:
            # Requested partition has not been assigned. Something went completely wrong
            raise PartitionNotAssignedError(
                f'Store partition "{self._name}[{partition}]" '
                f'(topic "{self._topic}") is not assigned'
            )

        store_partition = self._partitions[partition]
        return store_partition.begin()

    def close(self):
        """
        Close the store and revoke all assigned partitions
        """
        logger.debug(f'Closing store "{self._name}" (topic "{self._topic}")')
        partitions = list(self._partitions.keys())
        for partition in partitions:
            self.revoke_partition(partition)
        logger.debug(f'Closed store "{self._name}" (topic "{self._topic}")')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
