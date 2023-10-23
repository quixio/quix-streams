import logging
from pathlib import Path
from typing import Dict, Optional

from streamingdataframes.state.exceptions import PartitionNotAssignedError
from streamingdataframes.state.types import DumpsFunc, LoadsFunc, Store
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
        open_max_retries: int = 10,
        open_retry_backoff: float = 3.0,
        dumps: Optional[DumpsFunc] = None,
        loads: Optional[LoadsFunc] = None,
    ):
        """
        RocksDB-based state store.

        It keeps track of individual store partitions and provides access to the
        partitions' transactions.

        :param name: a unique store name
        :param topic: a topic name for this store
        :param base_dir: path to a directory with the state
        :param options: RocksDB options. If `None`, the default options will be used.
        :param open_max_retries: number of times to retry opening the database
            if it's locked by another process. To disable retrying, pass 0.
        :param open_retry_backoff: number of seconds to wait between each retry.
        :param dumps: the function used to serialize keys & values to bytes in
            transactions. Default - `json.dumps`
        :param loads: the function used to deserialize keys & values from bytes
            to objects in transactions. Default - `json.loads`.
        """
        self._name = name
        self._topic = topic
        self._partitions_dir = Path(base_dir).absolute() / self._name / self._topic
        self._transactions: Dict[int, RocksDBPartitionTransaction] = {}
        self._partitions: Dict[int, RocksDBStorePartition] = {}
        self._options = options
        self._dumps = dumps
        self._loads = loads
        self._open_max_retries = open_max_retries
        self._open_retry_backoff = open_retry_backoff

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
        store_partition = RocksDBStorePartition(
            path=path,
            options=self._options,
            dumps=self._dumps,
            loads=self._loads,
            open_max_retries=self._open_max_retries,
            open_retry_backoff=self._open_retry_backoff,
        )

        self._partitions[partition] = store_partition
        logger.debug(
            f'Assigned partition "{partition}" '
            f'for store "{self._name}" (topic "{self._topic}")'
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
            logger.debug(
                f'Partition for store "{self._name}" (topic "{self._topic}") '
                f"is not assigned"
            )
            return

        store_partition.close()
        self._partitions.pop(partition)
        logger.debug(f'Revoked partition "{partition}" for store "{self._name}"')

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
                f'Partition "{partition}" is not assigned '
                f'to the store "{self._name}" (topic "{self._topic}")'
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
