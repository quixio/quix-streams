from typing import Any, Optional

from quixstreams.state.base.partition import StorePartition
from quixstreams.state.base.transaction import PartitionTransaction
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc

from .cache import CounterCache
from .metadata import GLOBAL_COUNTER_CF_NAME, GLOBAL_COUNTER_KEY

__all__ = ["RocksDBPartitionTransaction"]


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
        self._global_counter = CounterCache(
            key=GLOBAL_COUNTER_KEY,
            cf_name=GLOBAL_COUNTER_CF_NAME,
        )

    def _get_next_count(self) -> int:
        """
        Get the next unique global counter value.

        This method maintains a global counter in RocksDB to ensure unique
        identifiers for values collected within the same timestamp. The counter
        is cached to reduce database reads.

        The counter will reset to 0 if it reaches the maximum unsigned 64-bit
        integer value (18446744073709551615) to prevent overflow.

        :return: Next sequential counter value
        """
        cache = self._global_counter

        if cache.counter is None:
            cache.counter = self.get(
                key=cache.key, prefix=b"", default=-1, cf_name=cache.cf_name
            )

        cache.counter += 1

        try:
            value = self._serialize_value(cache.counter)
        except StateSerializationError:
            cache.counter = 0
            value = self._serialize_value(cache.counter)

        self.set_bytes(value=value, key=cache.key, prefix=b"", cf_name=cache.cf_name)
        return cache.counter
