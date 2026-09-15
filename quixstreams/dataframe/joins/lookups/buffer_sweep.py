import logging
from typing import NamedTuple, Optional

from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .buffer_bookkeeping import BufferBookkeeping
from .buffer_envelope import ENVELOPE_RECEIVED, Emission, emit_tuple
from .buffer_state import (
    MAX_RECEIVE_MS,
    PendingIndex,
    decode_prefix,
    key_from_prefix,
)

__all__ = (
    "MAX_RECEIVE_MS",
    "SWEEP_BUDGET",
    "SWEEP_EMIT_BUDGET",
    "BufferSweeper",
    "SweepResult",
)

logger = logging.getLogger(__name__)

SWEEP_BUDGET = 4

SWEEP_EMIT_BUDGET = 256


class SweepResult(NamedTuple):
    emissions: list[Emission]
    swept: int


class BufferSweeper:
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
        skip: Optional[bytes],
        prefix_budget: int = SWEEP_BUDGET,
    ) -> SweepResult:
        overdue = index.due(cutoff, limit=prefix_budget + 1)
        if not overdue:
            return SweepResult([], 0)

        emit_budget = SWEEP_EMIT_BUDGET
        out: list[Emission] = []
        swept = 0

        for encoded, queued_ms in overdue:
            if swept >= prefix_budget:
                break

            entry = index.entry(encoded)
            if entry is None or entry[0] > cutoff:
                index.unqueue(encoded, queued_ms)
                continue

            prefix = decode_prefix(encoded)
            if skip is not None and prefix == skip:
                continue

            if self._emit_on_timeout and emit_budget <= 0:
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
                "Lookup buffer swept %s overdue keys, emitting %s records",
                swept,
                len(out),
            )
        return SweepResult(out, swept)

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
        out: list[Emission],
    ) -> int:
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
            self._reindex(transaction, index, partition, prefix, cutoff)
            return 0

        cut = len(envelopes)
        if cut > emit_budget:
            cut = emit_budget
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
