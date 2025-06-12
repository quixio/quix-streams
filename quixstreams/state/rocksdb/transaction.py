from itertools import chain
from typing import TYPE_CHECKING, Any, Optional, cast

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.serialization import DumpsFunc, LoadsFunc, append_integer

from .metadata import GLOBAL_COUNTER_CF_NAME, GLOBAL_COUNTER_KEY

if TYPE_CHECKING:
    from .partition import RocksDBStorePartition

__all__ = ("RocksDBPartitionTransaction",)

MAX_UINT64 = 2**64 - 1  # 18446744073709551615


class RocksDBPartitionTransaction(PartitionTransaction[bytes, Any]):
    def __init__(
        self,
        partition: "RocksDBStorePartition",
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
        self._partition: RocksDBStorePartition = cast(
            "RocksDBStorePartition", self._partition
        )
        self._counter: Optional[int] = None

    def _get_items(
        self,
        start: int,
        end: int,
        prefix: bytes,
        backwards: bool = False,
        cf_name: str = "default",
    ) -> list[tuple[bytes, bytes]]:
        """
        Get all items that start between `start` and `end`
        within the specified prefix.

        This function also checks the update cache for any updates not yet
        committed to RocksDB.

        :param start: Start of the range, inclusive.
        :param end: End of the range, exclusive.
        :param prefix: The key prefix for filtering items.
        :param backwards: If True, returns items in reverse order.
        :param cf_name: The RocksDB column family name.
        :return: A sorted list of key-value pairs.
        """
        start = max(start, 0)
        if start > end:
            return []

        seek_from_key = append_integer(base_bytes=prefix, integer=start)
        seek_to_key = append_integer(base_bytes=prefix, integer=end)

        # Create an iterator over the state store
        db_items = self._partition.iter_items(
            lower_bound=seek_from_key,
            upper_bound=seek_to_key,
            cf_name=cf_name,
        )

        cache = self._update_cache
        update_cache = cache.get_updates(cf_name=cf_name).get(prefix, {})
        delete_cache = cache.get_deletes(cf_name=cf_name)

        # Get cached updates with matching keys
        updated_items = (
            (key, value)
            for key, value in update_cache.items()
            if seek_from_key < key <= seek_to_key
        )

        # Iterate over stored and cached items and merge them to a single dict
        merged_items = {}
        for key, value in chain(db_items, updated_items):
            if key not in delete_cache:
                merged_items[key] = value

        # Sort items merged from the cache and store
        return sorted(merged_items.items(), key=lambda kv: kv[0], reverse=backwards)

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
