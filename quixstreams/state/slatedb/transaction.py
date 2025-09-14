from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from quixstreams.state.base.transaction import PartitionTransaction
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc

if TYPE_CHECKING:
    from .partition import SlateDBStorePartition


class SlateDBPartitionTransaction(PartitionTransaction[bytes, Any]):
    def __init__(
        self,
        partition: "SlateDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"] = None,
    ) -> None:
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
