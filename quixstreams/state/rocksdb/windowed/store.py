from threading import Event
from typing import Optional

from ...recovery import ChangelogProducer, ChangelogProducerFactory
from ..open_deadline import OpenDeadline
from ..store import RocksDBStore
from ..types import RocksDBOptionsType
from .partition import WindowedRocksDBStorePartition


class WindowedRocksDBStore(RocksDBStore):
    """
    RocksDB-based windowed state store.

    It keeps track of individual store partitions and provides access to the
    partitions' transactions.
    """

    def __init__(
        self,
        name: str,
        stream_id: str,
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[RocksDBOptionsType] = None,
        stop_event: Optional[Event] = None,
        open_deadline: Optional[OpenDeadline] = None,
    ):
        """
        :param name: a unique store name
        :param stream_id: a topic name for this store
        :param base_dir: path to a directory with the state
        :param changelog_producer_factory: a ChangelogProducerFactory instance
            if using changelogs
        :param options: RocksDB options. If `None`, the default options will be used.
        :param stop_event: an application stop signal shared with the store
            partitions so their open-retry loop can abort promptly on shutdown.
        :param open_deadline: a shared per-assign open-time budget shared with
            the store partitions so their open-retry loop can honour the total
            RocksDB-open budget for a single rebalance assignment.
        """
        super().__init__(
            name=name,
            stream_id=stream_id,
            base_dir=base_dir,
            changelog_producer_factory=changelog_producer_factory,
            options=options,
            stop_event=stop_event,
            open_deadline=open_deadline,
        )

    def create_new_partition(self, partition) -> WindowedRocksDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())

        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return WindowedRocksDBStorePartition(
            path=path,
            options=self._options,
            changelog_producer=changelog_producer,
            stop_event=self._stop_event,
            open_deadline=self._open_deadline,
        )
