import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from quixstreams.state.exceptions import PartitionNotAssignedError

from .partition import StorePartition
from .transaction import PartitionTransaction

__all__ = ("Store",)

logger = logging.getLogger(__name__)


class Store(ABC):
    """
    Abstract state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
    ) -> None:
        super().__init__()

        self._name = name
        self._stream_id = stream_id
        self._partitions: Dict[int, StorePartition] = {}

    @abstractmethod
    def create_new_partition(self, partition: int) -> StorePartition:
        pass

    @property
    def stream_id(self) -> Optional[str]:
        """
        Topic name
        """
        return self._stream_id

    @property
    def name(self) -> str:
        """
        Store name
        """
        return self._name

    @property
    def partitions(self) -> Dict[int, StorePartition]:
        """
        Mapping of assigned store partitions
        :return: dict of "{partition: <StorePartition>}"
        """
        return self._partitions

    def assign_partition(self, partition: int) -> StorePartition:
        """
        Assign new store partition

        :param partition: partition number
        :return: instance of `StorePartition`
        """
        store_partition = self._partitions.get(partition)
        if store_partition is not None:
            logger.debug(
                f'Partition "{partition}" for store "{self._name}" '
                f'(stream "{self._stream_id}") '
                f"is already assigned"
            )
            return store_partition

        store_partition = self.create_new_partition(partition)

        self._partitions[partition] = store_partition
        logger.debug(
            'Assigned store partition "%s[%s]" (stream "%s")',
            self._name,
            partition,
            self._stream_id,
        )
        return store_partition

    def revoke_partition(self, partition: int):
        """
        Revoke assigned store partition

        :param partition: partition number
        """
        store_partition = self._partitions.pop(partition, None)
        if store_partition is None:
            return

        store_partition.close()
        logger.debug(
            'Revoked store partition "%s[%s]" (stream "%s")',
            self._name,
            partition,
            self._stream_id,
        )

    def start_partition_transaction(self, partition: int) -> PartitionTransaction:
        """
        Start a new partition transaction.

        `PartitionTransaction` is the primary interface for working with data in Stores.
        :param partition: partition number
        :return: instance of `PartitionTransaction`
        """
        store_partition = self._partitions.get(partition)
        if store_partition is None:
            # Requested partition has not been assigned. Something went completely wrong
            raise PartitionNotAssignedError(
                f'Store partition "{self._name}[{partition}]" '
                f'(stream "{self._stream_id}") is not assigned'
            )

        return store_partition.begin()

    def close(self):
        """
        Close store and revoke all store partitions
        """
        logger.debug(f'Closing store "{self.name}" (stream "{self.stream_id}")')
        for partition in list(self._partitions.keys()):
            self.revoke_partition(partition)
        logger.debug(f'Closed store "{self.name}" (stream "{self.stream_id}")')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
