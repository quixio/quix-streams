from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from quixstreams.state.base import Store
from quixstreams.state.recovery import ChangelogProducer, ChangelogProducerFactory

from .options import SlateDBOptionsType
from .partition import SlateDBStorePartition

logger = logging.getLogger(__name__)

__all__ = ("SlateDBStore",)


class SlateDBStore(Store):
    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[SlateDBOptionsType] = None,
    ) -> None:
        super().__init__(name, stream_id)
        partitions_dir = Path(base_dir).absolute() / self._name
        if self._stream_id:
            partitions_dir = partitions_dir / self._stream_id
        self._partitions_dir = partitions_dir
        self._changelog_producer_factory = changelog_producer_factory
        self._options = options

    def create_new_partition(self, partition: int) -> SlateDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())
        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )
        return SlateDBStorePartition(
            path=path, options=self._options, changelog_producer=changelog_producer
        )
