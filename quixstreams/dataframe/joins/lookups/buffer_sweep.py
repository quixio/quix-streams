"""
The sweep: settling the deadlines of keys that have gone quiet.

The release path can only fire when a record for the same key arrives. A key
that stops producing therefore has nothing to trigger it, and the state store
will not clean up after it either: `TimestampedPartitionTransaction._expire()`
only walks prefixes written in the current transaction, and timestamped store
partitions opt out of the background TTL sweep. Without this module a silent
key's withheld records would sit in RocksDB and in the changelog forever - and
under `on_timeout="emit"` would never be delivered at all.

So every record processed by the operator, whatever its key, settles a bounded
slice of its partition's overdue prefixes. Healthy keys' traffic is what pays
for the unconfigured ones.

The slice is taken off the pending index's deadline queue
(`PendingIndex.due()`), which returns the overdue prefixes oldest deadline
first. That ordering is also the fairness guarantee: a swept prefix leaves the
queue, so the next record's slice starts with whatever is now oldest and nothing
can be starved by a busier neighbour. No cursor is needed, and a key that is
waiting but not yet overdue costs nothing at all.

Emitting another key's records from this record's callback is safe precisely
because the operator is an expanded transform: each emitted tuple carries its
own key, and a swept prefix is by construction on the same partition as the
current record, so downstream partitioning, `to_topic` and per-key state all
stay correct.
"""

import logging
from typing import Any

from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .buffer_bookkeeping import BufferBookkeeping
from .buffer_envelope import ENVELOPE_RECEIVED, emit_tuple
from .buffer_state import (
    MAX_RECEIVE_MS,
    PendingIndex,
    decode_prefix,
    key_from_prefix,
)

__all__ = ("MAX_RECEIVE_MS", "SWEEP_BUDGET", "SWEEP_EMIT_BUDGET", "BufferSweeper")

logger = logging.getLogger(__name__)

# How many overdue prefixes one record may settle. Keeps the per-record cost of
# other keys' expiry bounded and predictable.
SWEEP_BUDGET = 4

# How many timed-out records one record may emit on behalf of other keys. The
# rest wait for the next record's slice.
SWEEP_EMIT_BUDGET = 256


class BufferSweeper:
    """
    The settler of other keys' expired records, for one operator.

    It carries no state: which prefixes are overdue, and in what order, is
    entirely a property of the durable pending index, so nothing here has to
    survive a restart or be reconciled after a rebalance.
    """

    def __init__(
        self,
        *,
        emit_on_timeout: bool,
        bookkeeping: BufferBookkeeping,
    ) -> None:
        self._emit_on_timeout = emit_on_timeout
        self._bookkeeping = bookkeeping

    def sweep(
        self,
        transaction: TimestampedPartitionTransaction,
        index: PendingIndex,
        partition: int,
        cutoff: int,
        skip: bytes,
    ) -> list[tuple[Any, Any, int, Any]]:
        """
        Settle up to `SWEEP_BUDGET` keys whose records are past their deadline.

        :param transaction: The live store transaction.
        :param index: The partition's pending index.
        :param partition: The partition number.
        :param cutoff: Arrival times at or below this are past their deadline.
        :param skip: The current record's prefix, which the record path handles
            itself in the same callback.
        :return: The records to emit, always empty under `on_timeout="drop"`.
        """
        overdue = index.due(cutoff)
        if not overdue:
            return []

        emit_budget = SWEEP_EMIT_BUDGET
        out: list[tuple[Any, Any, int, Any]] = []
        swept = 0

        for encoded, queued_ms in overdue:
            if swept >= SWEEP_BUDGET:
                break

            entry = index.entry(encoded)
            # The marker is read anyway - the emitted key is rebuilt from its
            # kind - so checking it costs nothing, and it is the guard against a
            # queue entry that outlived its marker. `PendingIndex.flush()` writes
            # the two together, but a checkpoint whose changelog messages were
            # only partly produced before the process died replays as a partial
            # index. Such an entry is repaired rather than skipped, or every
            # later record would read it again.
            if entry is None or entry[0] > cutoff:
                index.unqueue(encoded, queued_ms)
                continue

            prefix = decode_prefix(encoded)
            if prefix == skip:
                continue

            if self._emit_on_timeout and emit_budget <= 0:
                # Out of room this cycle; the rest stay queued for the next
                # record, still oldest first.
                break

            swept += 1
            emit_budget -= self._sweep_prefix(
                transaction=transaction,
                index=index,
                partition=partition,
                prefix=prefix,
                kind=entry[1],
                cutoff=cutoff,
                emit_budget=emit_budget,
                out=out,
            )

        if swept:
            logger.debug(
                "Lookup buffer swept %s of %s overdue keys, emitting %s records",
                swept,
                len(overdue),
                len(out),
            )
        return out

    def _sweep_prefix(
        self,
        *,
        transaction: TimestampedPartitionTransaction,
        index: PendingIndex,
        partition: int,
        prefix: bytes,
        kind: str,
        cutoff: int,
        emit_budget: int,
        out: list[tuple[Any, Any, int, Any]],
    ) -> int:
        """
        Settle one swept prefix and update its index entry.

        The stored value is emitted verbatim: it was already resolved through
        every field's `missing()` when it was written, and a record that ran out
        of grace is never enriched afterwards.

        :param transaction: The live store transaction.
        :param index: The partition's pending index.
        :param partition: The partition number.
        :param prefix: The swept prefix.
        :param kind: The key-kind tag recorded in the index.
        :param cutoff: Arrival times at or below this are past their deadline.
        :param emit_budget: How many records may still be emitted this cycle.
        :param out: The output list to append emitted records to.
        :return: The number of records emitted for this prefix.
        """
        key = key_from_prefix(prefix, kind)

        if not self._emit_on_timeout:
            dropped = transaction.delete_interval(
                start=0,
                end=cutoff + 1,
                prefix=prefix,
            )
            self._bookkeeping.decrement(partition, prefix, dropped)
            self._bookkeeping.log_dropped(prefix, key, dropped)
            self._reindex(transaction, index, partition, prefix, cutoff)
            return 0

        envelopes = transaction.get_interval(start=0, end=cutoff + 1, prefix=prefix)
        if not envelopes:
            # The recorded earliest arrival was a stale lower bound. Recompute it
            # so this prefix stops being selected on every sweep.
            self._reindex(transaction, index, partition, prefix, cutoff)
            return 0

        cut = len(envelopes)
        if cut > emit_budget:
            cut = emit_budget
            # Never split a group of records that arrived in the same
            # millisecond: `delete_interval()` addresses whole milliseconds, so
            # half a group would be either re-emitted or silently deleted.
            last_receive_ms = envelopes[cut - 1][ENVELOPE_RECEIVED]
            while (
                cut < len(envelopes)
                and envelopes[cut][ENVELOPE_RECEIVED] == last_receive_ms
            ):
                cut += 1

        for envelope in envelopes[:cut]:
            out.append(emit_tuple(envelope, key))

        deleted = transaction.delete_interval(
            start=0,
            end=envelopes[cut - 1][ENVELOPE_RECEIVED] + 1,
            prefix=prefix,
        )
        self._bookkeeping.decrement(partition, prefix, deleted)

        if cut < len(envelopes):
            # More are past their deadline than the budget allowed; the next
            # arrival time is already known, so no extra read is needed.
            index.set_earliest(prefix, envelopes[cut][ENVELOPE_RECEIVED])
        else:
            self._reindex(transaction, index, partition, prefix, cutoff)
        return cut

    def _reindex(
        self,
        transaction: TimestampedPartitionTransaction,
        index: PendingIndex,
        partition: int,
        prefix: bytes,
        cutoff: int,
    ) -> None:
        """
        Recompute a swept prefix's index entry from what is actually left.

        Reading the remainder is what lets a prefix leave the index at all: the
        sweep would otherwise have no way to tell "nothing left" from "nothing
        past its deadline", and a dead key's entry would be re-selected on every
        sweep forever. It only runs when a sweep has just emptied a prefix's
        timed-out range, and for a silent key - the case the sweep exists for -
        it reads nothing.

        :param transaction: The live store transaction.
        :param index: The partition's pending index.
        :param partition: The partition number.
        :param prefix: The swept prefix.
        :param cutoff: Arrival times at or below this are past their deadline.
        """
        remaining = transaction.get_interval(
            start=cutoff + 1,
            end=MAX_RECEIVE_MS,
            prefix=prefix,
        )
        if remaining:
            index.set_earliest(prefix, remaining[0][ENVELOPE_RECEIVED])
            self._bookkeeping.set_count(partition, prefix, len(remaining))
        else:
            index.drop(prefix)
            self._bookkeeping.set_count(partition, prefix, 0)
