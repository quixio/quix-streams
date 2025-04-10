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
    def get_last(
        self,
        timestamp: int,
        prefix: bytes,
        cf_name: str = "default",
    ) -> Optional[Any]:
        key = self._serialize_key(timestamp + 1, prefix=prefix)
        value: Optional[bytes] = None

        cached = self._update_cache.iter_items(
            prefix=prefix,
            backwards=True,
            cf_name=cf_name,
        )
        for cached_key, cached_value in cached:
            if prefix < cached_key < key:
                value = cached_value
                break

        stored = self._partition.iter_items(
            lower_bound=prefix,
            upper_bound=key,
            backwards=True,
            cf_name=cf_name,
        )
        for stored_key, stored_value in stored:
            if value is None or cached_key < stored_key:
                value = stored_value
                break

        return self._deserialize_value(value) if value is not None else None

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set(self, timestamp: int, value: Any, prefix: bytes, cf_name: str = "default"):
        super().set(timestamp, value, prefix, cf_name)


class TimestampedStorePartition(RocksDBStorePartition):
    partition_transaction_class = TimestampedPartitionTransaction


class TimestampedStore(RocksDBStore):
    store_partition_class = TimestampedStorePartition
