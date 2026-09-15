"""
The deadline tick: settling buffered records on the clock, not on traffic.

`BufferSweeper` settles the records of keys that have gone quiet, but until now
it was reachable from exactly one place - the record path, `buffer_operator.py`.
A partition whose traffic stops therefore stopped settling deadlines, and
`on_timeout` silently meant "…once another record shows up".

`BufferTicker` is the second driver. `DataFrameRegistry.run_periodic_tasks()`
calls it once per Application loop iteration, and that loop wakes at least once
per `consumer_poll_timeout` (1.0 s by default) even with zero traffic on the
whole assignment. The tick is therefore main-thread, in-checkpoint work: it may
use the live `PartitionTransaction` and produce records inside the same
checkpoint the very next statement of the loop may commit.

Three things make this affordable and safe.

**The gate is an integer comparison.** Per partition the ticker keeps a cached
*lower bound* on the earliest wall-clock millisecond at which anything on that
partition can be due, and the minimum of those bounds across partitions. While
the clock is below that minimum the tick reads two attributes and returns:
no transaction, no RocksDB access, nothing added to `Checkpoint._store_transactions`.
Too low a bound costs one wasted range read; too high would hide records
forever, so every write site in this file is checked against that rule - the
same lower-bound discipline `PendingIndex`'s earliest-arrival marker runs on.

**The settlement semantics are not re-derived.** The tick calls the existing
`BufferSweeper.sweep()`, with a bigger prefix budget and `skip=None` (there is
no current record to leave alone). Deadline ordering, the
never-split-a-millisecond-group rule, the partial-index repair, the bookkeeping
and the drop-mode logging all come along unchanged.

**Nothing is deleted that is not also emitted.** The sweep's deletes and the
downstream emissions have to land in the same checkpoint or the buffer either
duplicates or loses records. So the tick's wall-clock budget is checked
*between sweep passes*, never between the records of one pass: every record a
pass deleted is emitted before the budget can stop anything. The budget then
decides whether to ask for another pass. The bound is wall-clock rather than a
record count because the cost of an emission is dominated by the downstream
executor - a `to_topic` serialization, a sink's HTTP call - which varies by
orders of magnitude between pipelines.
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

# Wall-clock budget, in seconds, for one tick across all partitions. Checked
# between sweep passes. 0.2 s is a rounding error against `max.poll.interval.ms`
# (300 s by default) - the limit whose breach got the blocking `grace_ms`
# withdrawn - and small against the 1.0 s poll timeout, so the tick never
# becomes the reason a poll is late.
TICK_TIME_BUDGET_S = 0.2

# How many overdue prefixes one sweep pass may settle on the tick path. Larger
# than the record path's `SWEEP_BUDGET` because a tick is not competing with a
# record for the callback, but finite so that a pass is bounded under
# `on_timeout="drop"` too, where no emit budget is consumed.
TICK_SWEEP_BUDGET = 64

# "Nothing on this partition can ever be due." Reuses the buffer's existing
# never-a-real-arrival-time sentinel.
NO_DEADLINE = MAX_RECEIVE_MS


class BufferTicker:
    """
    The clock-driven settler for one `join_lookup(..., buffer=...)` operator.

    Holds the per-partition deadline cache and the binding to the operator's
    downstream executor. One instance per operator, so nothing here is shared
    between two operators or two streams.
    """

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

        # Bound at compose time by `BufferTransformFunction.get_executor()`.
        # `None` means the topology has not been composed yet, which outside
        # `Application.run()` is a perfectly ordinary state.
        self._downstream: Optional[VoidExecutor] = None
        self._store: Optional[Store] = None
        self._topic: Optional[str] = None

        # The store partitions last observed as assigned, and per partition a
        # LOWER BOUND on the earliest wall-clock millisecond at which something
        # can be due there. `_earliest` is their minimum, kept so the hot path is
        # one integer comparison.
        #
        # `_known` maps partition -> the `StorePartition` OBJECT, not just the
        # number, and `tick()` compares the whole mapping. A rebalance that
        # revokes and re-assigns the same partition number inside one callback
        # leaves the number set identical, but the revoke popped the entry so
        # the assign builds a fresh `StorePartition` - whose buffer has just
        # been recovered from the changelog. It must take a real pass rather
        # than inherit the old instance's "nothing is due here" verdict.
        self._known: dict[int, StorePartition] = {}
        self._deadline: dict[int, int] = {}
        self._earliest: int = NO_DEADLINE

    def bind_downstream(self, downstream: VoidExecutor) -> None:
        """
        Capture the executor the tick emits into.

        Called from `BufferTransformFunction.get_executor()`, i.e. once per
        `Stream.compose()`. Composing more than once (tests, several roots)
        simply overwrites it; last call wins, which is the same executor the
        record path is using.

        :param downstream: The resolved child executor for this operator's node.
        """
        self._downstream = downstream

    def note_deadline(self, partition: int, deadline_ms: int) -> None:
        """
        Lower a partition's cached deadline to account for a record just
        withheld.

        Only ever lowers. The cache is a lower bound, so a value that is too low
        costs one wasted range read while a value that is too high would hide
        the record from the tick forever.

        :param partition: The partition number.
        :param deadline_ms: The withheld record's own deadline, in milliseconds.
        """
        if deadline_ms < self._deadline.get(partition, NO_DEADLINE):
            self._deadline[partition] = deadline_ms
        if deadline_ms < self._earliest:
            self._earliest = deadline_ms

    def tick(self) -> None:
        """
        Settle everything past its deadline on every assigned partition.

        This is the registered periodic task. It runs on every Application loop
        iteration, so the first two lines of work are the cost every application
        using a lookup buffer pays per iteration: one dict comparison over the
        assigned partitions (typically <= 8 entries) and one integer comparison
        against the cached watermark.

        An exception raised by a downstream operator propagates. The tick has no
        `Row` to hand to the Application's processing-error callback, and
        swallowing it would drop records whose deletes are already in the
        transaction.
        """
        downstream = self._downstream
        if downstream is None:
            return

        partitions = self._get_store().partitions
        if partitions != self._known:
            # The store's assigned-partition map is the authoritative "what do I
            # own": `StateStoreManager.on_partition_revoke` removes the entry
            # inside the revoke callback, which runs inside `consumer.poll()`,
            # which has already returned by the time the loop reaches the tick.
            # So a revoked partition is simply absent here.
            self._resync(partitions)

        now_ms = int(time.time() * 1000)
        if now_ms < self._earliest:
            return

        # The same isolation `Application._process_message` uses, so neither the
        # message context set below nor any contextvar a downstream operator
        # writes escapes into the main loop's context.
        copy_context().run(self._settle, now_ms, downstream)

    def _resync(self, partitions: dict[int, StorePartition]) -> None:
        """
        Reconcile the deadline cache with the store's assigned partitions.

        A partition whose `StorePartition` this ticker has not seen before is
        seeded with `0`, forcing one real pass: its contents are unknown - a
        changelog-recovered buffer may hold anything - and `0` is trivially a
        valid lower bound. This covers a first assignment and a re-assignment
        alike. It is also why the ticker needs no assignment or recovery
        callback: a fresh process starts with an empty cache, so every partition
        takes one pass and the cache is pure optimisation from then on.

        :param partitions: The store's live assigned-partition mapping.
        """
        for partition in list(self._deadline):
            if partition not in partitions:
                del self._deadline[partition]
        for partition, store_partition in partitions.items():
            if self._known.get(partition) is not store_partition:
                self._deadline[partition] = 0
        self._known = dict(partitions)
        self._earliest = min(self._deadline.values(), default=NO_DEADLINE)

    def _settle(self, now_ms: int, downstream: VoidExecutor) -> None:
        """
        Run one pass over every partition whose cached deadline has passed.

        `tick()` has just reconciled `_known` with the store, so `_known` is the
        assignment as of this instant. Anything in `_deadline` that is not in it
        is dropped rather than swept: `note_deadline()` writes from the record
        path, which can add an entry for a partition the ticker has not synced
        yet, and that partition may be gone by the time it looks. Sweeping it
        would ask the store for a transaction on an unassigned partition.

        :param now_ms: The tick's wall-clock time, in milliseconds.
        :param downstream: The executor to emit into.
        """
        # Byte-for-byte the expression the record path uses: arrival times at or
        # below the cutoff are past their deadline.
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
                # The partitions not reached keep their already-passed
                # deadlines, so the next loop iteration starts with them.
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
        """
        Settle one partition and recompute its cached deadline.

        :param partition: The partition number.
        :param cutoff: Arrival times at or below this are past their deadline.
        :param now_ms: The tick's wall-clock time, in milliseconds.
        :param budget_ends: `time.monotonic()` value at which this tick stops.
        :param downstream: The executor to emit into.
        """
        transaction = self._get_transaction(partition)
        index = PendingIndex(transaction)
        emitted = 0
        truncated = False

        while True:
            result = self._sweeper.sweep(
                transaction,
                index,
                partition,
                cutoff,
                skip=None,
                prefix_budget=TICK_SWEEP_BUDGET,
            )
            # Persist this pass's index changes before the next `due()`, which
            # reads the deadline queue back through the same transaction and
            # would otherwise return the entries this pass has just retired.
            index.flush()

            for emission in result.emissions:
                # Rebuilt per record, and per record deliberately: `to_topic`
                # reads the context, `sdf.sink` reads its topic/partition/offset,
                # and a downstream stateful operator keys its transaction on
                # `message_context().partition`. A composed Stream has no
                # batch entry point to amortise this against.
                #
                # `partition` is correct by construction. `topic` and `offset`
                # are the record's own originals, carried in its envelope, so a
                # sink writes a truthful triple rather than a synthetic one.
                # `size` is 0 and `leader_epoch` unset because the buffer never
                # had them and nothing in the SDK reads them for correctness.
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
            # Known outstanding work: resume on the very next loop iteration
            # rather than waiting for another deadline to pass.
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
            # Everything due is settled, so the queue's next entry - if any - is
            # the earliest anything can come due. The queue is deadline-ordered,
            # so nothing can beat it.
            earliest = index.earliest()
            self._deadline[partition] = (
                NO_DEADLINE if earliest is None else earliest + self._grace_ms
            )

    def _get_store(self) -> Store:
        """
        Return the buffer's store, memoised.

        :return: The store registered by `LookupBuffer.register_store()`.
        """
        if self._store is None:
            self._store = self._dataframe.processing_context.state_manager.get_store(
                stream_id=self._dataframe.stream_id,
                store_name=self._store_name,
            )
        return self._store

    def _get_topic(self) -> str:
        """
        Return the fallback topic name for a record whose envelope predates
        `ENVELOPE_TOPIC`, memoised.

        :return: The first topic feeding this dataframe.
        """
        if self._topic is None:
            self._topic = self._dataframe.topics[0].name
        return self._topic

    def _get_transaction(self, partition: int) -> TimestampedPartitionTransaction:
        """
        Return the live store transaction for a partition.

        Raises `PartitionNotAssignedError` for a partition this instance does not
        own. That is a real invariant violation - the tick iterates the store's
        own assignment map - and is deliberately left to propagate.

        :param partition: The partition number.
        :return: The transaction, memoised by the current checkpoint.
        """
        return cast(
            TimestampedPartitionTransaction,
            self._dataframe.processing_context.checkpoint.get_store_transaction(
                stream_id=self._dataframe.stream_id,
                partition=partition,
                store_name=self._store_name,
            ),
        )
