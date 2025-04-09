from typing import Any, Optional

from quixstreams.state.base.transaction import (
    PartitionTransaction,
    PartitionTransactionStatus,
    validate_transaction_status,
)

from .partition import RocksDBStorePartition
from .store import RocksDBStore

__all__ = (
    "TimestampedStore",
    "TimestampedStorePartition",
    "TimestampedPartitionTransaction",
)


class TimestampedPartitionTransaction(PartitionTransaction):
    _partition: "TimestampedStorePartition"  # TODO: this is a rather ugly mypy hack

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def get(
        self,
        timestamp: int,
        prefix: bytes,
        cf_name: str = "default",
    ) -> Optional[Any]:
        key = self._serialize_key(timestamp + 1, prefix=prefix)

        cached_value: Optional[bytes] = None
        cached_key: Optional[bytes] = None

        cached = self._update_cache.iter_items(
            prefix=prefix,
            backwards=True,
            cf_name=cf_name,
        )
        for cached_key, cached_value in cached:
            if prefix < cached_key < key:
                break

        stored = self._partition.iter_items(
            lower_bound=prefix,
            upper_bound=key,
            backwards=True,
            cf_name=cf_name,
        )
        for stored_key, stored_value in stored:
            if cached_key is None or cached_key < stored_key:
                return self._deserialize_value(stored_value)

        if cached_value is not None:
            return self._deserialize_value(cached_value)
        return None

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set(self, timestamp: int, value: Any, prefix: bytes, cf_name: str = "default"):
        super().set(timestamp, value, prefix, cf_name)


class TimestampedStorePartition(RocksDBStorePartition):
    partition_transaction_class = TimestampedPartitionTransaction


class TimestampedStore(RocksDBStore):
    store_partition_class = TimestampedStorePartition
