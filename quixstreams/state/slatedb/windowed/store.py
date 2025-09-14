from typing import Optional

from ...recovery import ChangelogProducer, ChangelogProducerFactory
from ..options import SlateDBOptionsType
from ..store import SlateDBStore
from .partition import WindowedSlateDBStorePartition


class WindowedSlateDBStore(SlateDBStore):
    def __init__(
        self,
        name: str,
        stream_id: str,
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[SlateDBOptionsType] = None,
    ):
        super().__init__(
            name=name,
            stream_id=stream_id,
            base_dir=base_dir,
            changelog_producer_factory=changelog_producer_factory,
            options=options,
        )

    def create_new_partition(self, partition) -> WindowedSlateDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())

        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )

        return WindowedSlateDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
