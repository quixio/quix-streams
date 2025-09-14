from typing import Any, Optional, cast

from quixstreams.state.base.transaction import (
    PartitionTransactionStatus,
    validate_transaction_status,
)
from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.recovery import ChangelogProducer, ChangelogProducerFactory
from quixstreams.state.rocksdb.transaction import RocksDBPartitionTransaction
from quixstreams.state.serialization import (
    DumpsFunc,
    LoadsFunc,
    encode_integer_pair,
    int_to_bytes,
    serialize,
)

from .options import SlateDBOptionsType
from .partition import SlateDBStorePartition
from .store import SlateDBStore

__all__ = (
    "TimestampedSlateDBStore",
    "TimestampedSlateDBStorePartition",
    "TimestampedSlateDBPartitionTransaction",
)

MIN_ELIGIBLE_TIMESTAMPS_CF_NAME = "__min-eligible-timestamps__"
MIN_ELIGIBLE_TIMESTAMPS_KEY = b"__min_eligible_timestamps__"


class TimestampedSlateDBPartitionTransaction(RocksDBPartitionTransaction):
    def __init__(
        self,
        partition: "TimestampedSlateDBStorePartition",
        dumps: DumpsFunc,
        loads: LoadsFunc,
        grace_ms: int,
        keep_duplicates: bool,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> None:
        super().__init__(
            partition=partition,
            dumps=dumps,
            loads=loads,
            changelog_producer=changelog_producer,
        )
        self._partition: TimestampedSlateDBStorePartition = cast(
            "TimestampedSlateDBStorePartition", self._partition
        )
        self._grace_ms = grace_ms
        self._keep_duplicates = keep_duplicates

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def get_latest(self, timestamp: int, prefix: Any) -> Optional[Any]:
        prefix = self._ensure_bytes(prefix)
        # bounds: [0..timestamp]
        lower = self._serialize_key(int_to_bytes(0), prefix)
        upper = self._serialize_key(int_to_bytes(timestamp + 1), prefix)

        deletes = self._update_cache.get_deletes()
        updates = self._update_cache.get_updates().get(prefix, {})
        cached = sorted(updates.items(), reverse=True)
        value: Optional[bytes] = None
        cached_key: Optional[bytes] = None
        if cached and cached[0][0] >= lower and cached[-1][0] < upper:
            for ck, cv in cached:
                if lower <= ck < upper and ck not in deletes:
                    value, cached_key = cv, ck
                    break

        stored = self._partition.iter_items(
            lower_bound=lower,
            upper_bound=upper,
            backwards=True,
        )
        for sk, sv in stored:
            if sk in deletes:
                continue
            if value is None or (cached_key and cached_key < sk):
                value = sv
            break

        return self._deserialize_value(value) if value is not None else None

    @validate_transaction_status(PartitionTransactionStatus.STARTED)
    def set_for_timestamp(self, timestamp: int, value: Any, prefix: Any) -> None:
        prefix = self._ensure_bytes(prefix)
        counter = self._increment_counter() if self._keep_duplicates else 0
        key = encode_integer_pair(timestamp, counter)
        self.set(key, value, prefix)

    def _ensure_bytes(self, prefix: Any) -> bytes:
        if isinstance(prefix, bytes):
            return prefix
        return serialize(prefix, dumps=self._dumps)

    def _serialize_key(self, key: bytes, prefix: bytes) -> bytes:
        return prefix + SEPARATOR + key


class TimestampedSlateDBStorePartition(SlateDBStorePartition):
    def __init__(
        self,
        path: str,
        grace_ms: int,
        keep_duplicates: bool,
        options: Optional[SlateDBOptionsType] = None,
        changelog_producer: Optional[ChangelogProducer] = None,
    ) -> None:
        super().__init__(
            path=path, options=options, changelog_producer=changelog_producer
        )
        self._grace_ms = grace_ms
        self._keep_duplicates = keep_duplicates

    def begin(self) -> TimestampedSlateDBPartitionTransaction:
        return TimestampedSlateDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            grace_ms=self._grace_ms,
            keep_duplicates=self._keep_duplicates,
            changelog_producer=self._changelog_producer,
        )


class TimestampedSlateDBStore(SlateDBStore):
    def __init__(
        self,
        name: str,
        stream_id: Optional[str],
        base_dir: str,
        grace_ms: int,
        keep_duplicates: bool,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[SlateDBOptionsType] = None,
    ) -> None:
        super().__init__(
            name=name,
            stream_id=stream_id,
            base_dir=base_dir,
            changelog_producer_factory=changelog_producer_factory,
            options=options,
        )
        self._grace_ms = grace_ms
        self._keep_duplicates = keep_duplicates

    def create_new_partition(self, partition: int) -> TimestampedSlateDBStorePartition:
        path = str((self._partitions_dir / str(partition)).absolute())
        changelog_producer: Optional[ChangelogProducer] = None
        if self._changelog_producer_factory:
            changelog_producer = (
                self._changelog_producer_factory.get_partition_producer(partition)
            )
        return TimestampedSlateDBStorePartition(
            path=path,
            grace_ms=self._grace_ms,
            keep_duplicates=self._keep_duplicates,
            options=self._options,
            changelog_producer=changelog_producer,
        )
