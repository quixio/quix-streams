from typing import Any, Optional

from quixstreams.state.base.partition import StorePartition
from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc

from .metadata import GLOBAL_COUNTER_CF_NAME, GLOBAL_COUNTER_KEY

__all__ = ["RocksDBPartitionTransaction"]

MAX_UINT64 = 2**64 - 1  # 18446744073709551615


class RocksDBPartitionTransaction(PartitionTransaction[bytes, Any]):
    def __init__(
        self,
        partition: StorePartition,
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
        self._counter: Optional[int] = None

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def prepare(self, processed_offsets: Optional[dict[str, int]] = None) -> None:
        """
        This method first persists the counter and then calls the parent class's
        `prepare()` to prepare the transaction for flush.

        :param processed_offsets: the dict with <topic: offset> of the latest processed message
        """
        self._persist_counter()
        super().prepare(processed_offsets=processed_offsets)

    def _increment_counter(self) -> int:
        """
        Increment the global counter.

        The counter will reset to 0 if it reaches the maximum unsigned 64-bit
        integer value (18446744073709551615) to prevent overflow.

        :return: Next sequential counter value
        """
        if self._counter is None:
            self._counter = self.get(
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                default=-1,
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
        self._counter = self._counter + 1 if self._counter < MAX_UINT64 else 0
        return self._counter

    def _persist_counter(self) -> None:
        if self._counter is not None:
            self.set(
                value=self._counter,
                key=GLOBAL_COUNTER_KEY,
                prefix=b"",
                cf_name=GLOBAL_COUNTER_CF_NAME,
            )
