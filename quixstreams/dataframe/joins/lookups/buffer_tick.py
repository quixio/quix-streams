"""
The clock-driven half of the buffer: settles deadlines on a partition with no
traffic, so `on_timeout` holds with zero input records.

`DataFrameRegistry.run_periodic_tasks()` calls `BufferTicker.tick()` on every
iteration of the Application's run loop, which is also its granularity - a
deadline is observed at most once per `consumer_poll_timeout`.
"""

import logging
import time
from typing import TYPE_CHECKING, Optional, cast

from quixstreams.context import copy_context, set_message_context
from quixstreams.core.stream import VoidExecutor
from quixstreams.models.messagecontext import MessageContext
from quixstreams.state.base import Store, StorePartition
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .buffer_state import MAX_RECEIVE_MS, PendingIndex
from .buffer_sweep import BufferSweeper

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("NO_DEADLINE", "TICK_SWEEP_BUDGET", "TICK_TIME_BUDGET_S", "BufferTicker")

logger = logging.getLogger(__name__)

# Wall-clock time one tick may spend across all partitions. The run loop cannot
# poll while it runs, so this is a fraction of `max.poll.interval.ms`, not of
# the poll timeout.
TICK_TIME_BUDGET_S = 0.2

# Keys settled per sweep inside the tick, which loops until the time budget runs
# out or nothing is due.
TICK_SWEEP_BUDGET = 64

NO_DEADLINE = MAX_RECEIVE_MS


class BufferTicker:
    """Settles buffer deadlines from the Application's run loop."""

    def __init__(
        self,
        *,
        dataframe: "StreamingDataFrame",
        store_name: str,
        grace_ms: int,
        sweeper: BufferSweeper,
    ) -> None:
        self._dataframe = dataframe
        self._store_name = store_name
        self._grace_ms = grace_ms
        self._sweeper = sweeper

        self._downstream: Optional[VoidExecutor] = None
        self._store: Optional[Store] = None
        self._topic: Optional[str] = None

        self._known: dict[int, StorePartition] = {}
        self._deadline: dict[int, int] = {}
        self._earliest: int = NO_DEADLINE

    def bind_downstream(self, downstream: VoidExecutor) -> None:
        """
        :param downstream: The composed executor emissions are sent through.
            Bound once at compose time, so it survives across ticks.
        """
        self._downstream = downstream

    def note_deadline(self, partition: int, deadline_ms: int) -> None:
        """
        Lower a partition's next deadline. The only caller is `_buffer()`, so a
        deadline can otherwise only move later, as records leave.

        :param partition: The partition the record was withheld on.
        :param deadline_ms: Its arrival time plus `grace_ms`.
        """
        if deadline_ms < self._deadline.get(partition, NO_DEADLINE):
            self._deadline[partition] = deadline_ms
        if deadline_ms < self._earliest:
            self._earliest = deadline_ms

    def tick(self) -> None:
        """Settle every partition whose earliest deadline has passed."""
        downstream = self._downstream
        if downstream is None:
            return

        # A partition is in `store.partitions` before its changelog is recovered
        # into it. Seeding deadlines from it then would cache NO_DEADLINE over a
        # backlog that is not in the store yet, and `_resync`'s identity check
        # cannot see the contents of an unchanged `StorePartition` object change.
        if self._dataframe.processing_context.state_manager.recovery_required:
            return

        partitions = self._get_store().partitions
        if partitions != self._known:
            self._resync(partitions)

        now_ms = int(time.time() * 1000)
        if now_ms < self._earliest:
            return

        copy_context().run(self._settle, now_ms, downstream)

    def _resync(self, partitions: dict[int, StorePartition]) -> None:
        # A deadline of 0 means "sweep this partition on the next tick", which is
        # how a newly assigned partition's backlog is discovered.
        for partition in list(self._deadline):
            if partition not in partitions:
                del self._deadline[partition]
        for partition, store_partition in partitions.items():
            if self._known.get(partition) is not store_partition:
                self._deadline[partition] = 0
        self._known = dict(partitions)
        self._earliest = min(self._deadline.values(), default=NO_DEADLINE)

    def _settle(self, now_ms: int, downstream: VoidExecutor) -> None:
        cutoff = now_ms - self._grace_ms
        budget_ends = time.monotonic() + TICK_TIME_BUDGET_S

        for partition in sorted(self._deadline):
            if partition not in self._known:
                del self._deadline[partition]
                continue
            if now_ms < self._deadline[partition]:
                continue
            self._sweep_partition(
                partition=partition,
                cutoff=cutoff,
                now_ms=now_ms,
                budget_ends=budget_ends,
                downstream=downstream,
            )
            if time.monotonic() >= budget_ends:
                break

        self._earliest = min(self._deadline.values(), default=NO_DEADLINE)

    def _sweep_partition(
        self,
        *,
        partition: int,
        cutoff: int,
        now_ms: int,
        budget_ends: float,
        downstream: VoidExecutor,
    ) -> None:
        transaction = self._get_transaction(partition)
        index = PendingIndex(transaction)
        emitted = 0
        truncated = False

        while True:
            result = self._sweeper.sweep(
                transaction,
                index,
                cutoff,
                skip=None,
                prefix_budget=TICK_SWEEP_BUDGET,
            )
            index.flush()

            # No input record is in hand here, so each emission's context is
            # rebuilt from the origin stored in its envelope.
            for emission in result.emissions:
                set_message_context(
                    MessageContext(
                        topic=emission.topic or self._get_topic(),
                        partition=partition,
                        offset=emission.offset,
                        size=0,
                    )
                )
                downstream(
                    emission.value,
                    emission.key,
                    emission.timestamp,
                    emission.headers,
                )
            emitted += len(result.emissions)

            if not result.swept:
                break
            if time.monotonic() >= budget_ends:
                truncated = True
                break

        if truncated:
            self._deadline[partition] = now_ms
            logger.debug(
                "Lookup buffer deadline tick ran out of its %.2fs budget on "
                "partition %s after emitting %s records; the remainder is "
                "carried to the next iteration",
                TICK_TIME_BUDGET_S,
                partition,
                emitted,
            )
        else:
            earliest = index.earliest()
            self._deadline[partition] = (
                NO_DEADLINE if earliest is None else earliest + self._grace_ms
            )

    def _get_store(self) -> Store:
        if self._store is None:
            self._store = self._dataframe.processing_context.state_manager.get_store(
                stream_id=self._dataframe.stream_id,
                store_name=self._store_name,
            )
        return self._store

    def _get_topic(self) -> str:
        if self._topic is None:
            self._topic = self._dataframe.topics[0].name
        return self._topic

    def _get_transaction(self, partition: int) -> TimestampedPartitionTransaction:
        return cast(
            TimestampedPartitionTransaction,
            self._dataframe.processing_context.checkpoint.get_store_transaction(
                stream_id=self._dataframe.stream_id,
                partition=partition,
                store_name=self._store_name,
            ),
        )
