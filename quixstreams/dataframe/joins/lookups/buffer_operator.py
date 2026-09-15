"""
The record path of the non-blocking lookup buffer.

`BufferOperator` is the callable appended to the `Stream` by
`StreamingDataFrame.join_lookup(..., buffer=...)`. It runs as an expanded
transform, so one input record can produce any number of output records, each
with its own value, key, timestamp and headers - which is the whole reason this
is a transform and not an `apply(expand=True)`: released records must keep their
*own* event timestamps, and the sweep must be able to emit records that belong
to a different key entirely.

Per record it does three things, in this order:

1. **Join.** Run the lookup once for the incoming record and ask `is_resolved`
   whether it worked.
2. **Sweep.** Settle a bounded slice of the other keys on this partition whose
   withheld records are past their deadline (see `buffer_sweep.py`). It runs
   before the branch below, so resolvable and unresolvable records alike drive
   it.
3. **Buffer, release, or pass through.** A record whose key has nothing withheld
   and whose lookup resolved goes straight out. A record whose lookup failed is
   written to the store and nothing is emitted for it. A record whose key *does*
   have withheld records first settles everything past its deadline, then
   releases the survivors ahead of itself if it resolved, or joins the queue
   behind them if it did not.

The single most important line in the file is the `start=cutoff + 1` lower bound
on the survivor read in `_release()`. `get_interval()` does not consult the
store's expiry floor the way `get_latest()` does, so without that bound a record
that has already passed its deadline would be read back and enriched, which is
exactly what the design forbids.
"""

import logging
import time
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, cast

from quixstreams.context import message_context
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .base import BaseField, BaseLookup
from .buffer_bookkeeping import BufferBookkeeping
from .buffer_envelope import (
    ENVELOPE_TIMESTAMP,
    decode_headers,
    emit_tuple,
    encode_envelope,
    envelope_value,
)
from .buffer_state import MAX_RECEIVE_MS, PendingIndex, prefix_for_key
from .buffer_sweep import BufferSweeper

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("BufferOperator", "LookupBufferOverflowError")

logger = logging.getLogger(__name__)

# A release deserializes a key's entire surviving buffer inside one callback.
# Past this many records that is a stall worth telling the user about.
RELEASE_WARN_RECORDS = 1000


class LookupBufferOverflowError(Exception):
    """
    Raised when a key exceeds `max_buffered_per_key` and the buffer was
    constructed with `on_overflow="raise"`.
    """


class BufferOperator:
    """
    The per-operator record path.

    One instance exists per `join_lookup(..., buffer=...)` call, so the
    in-process state it carries - the buffered counts, the log rate limiters and
    the sweep cursor - is never shared between two operators or two streams.
    """

    def __init__(
        self,
        *,
        dataframe: "StreamingDataFrame",
        lookup: BaseLookup,
        fields: Mapping[str, BaseField],
        on: Callable[[dict[str, Any], Any], str],
        store_name: str,
        grace_ms: int,
        is_resolved: Callable[[dict[str, Any]], bool],
        on_timeout: Literal["emit", "drop"],
        max_buffered_per_key: int,
        on_overflow: Literal["drop-newest", "raise"],
    ) -> None:
        self._dataframe = dataframe
        self._lookup = lookup
        self._fields = fields
        self._on = on
        self._store_name = store_name
        self._grace_ms = grace_ms
        self._is_resolved = is_resolved
        self._emit_on_timeout = on_timeout == "emit"
        self._max_buffered_per_key = max_buffered_per_key
        self._raise_on_overflow = on_overflow == "raise"

        self._bookkeeping = BufferBookkeeping()
        self._sweeper = BufferSweeper(
            emit_on_timeout=self._emit_on_timeout,
            bookkeeping=self._bookkeeping,
        )

    def __call__(
        self,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
    ) -> list[tuple[Any, Any, int, Any]]:
        """
        Handle one record and return everything that should go downstream.

        :param value: The record value.
        :param key: The message key.
        :param timestamp: The event timestamp, in milliseconds.
        :param headers: The record headers.
        :return: A list of `(value, key, timestamp, headers)` tuples, possibly
            empty - an empty list is how a record is withheld.
        """
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self._grace_ms
        partition = message_context().partition
        transaction = self._get_transaction(partition)
        index = PendingIndex(transaction)
        prefix = prefix_for_key(key)

        self._lookup.join(
            self._fields,
            self._on(value, key),
            value,
            key,
            timestamp,
            headers,
        )
        resolved = bool(self._is_resolved(value))

        # Every record drives the sweep - the traffic of healthy keys is what
        # settles the deadlines of unconfigured ones.
        out = self._sweeper.sweep(transaction, index, partition, cutoff, prefix)

        if index.get(prefix) is None:
            if resolved:
                out.append((value, key, timestamp, headers))
            else:
                self._buffer(
                    transaction=transaction,
                    index=index,
                    partition=partition,
                    prefix=prefix,
                    key=key,
                    receive_ms=now_ms,
                    value=value,
                    timestamp=timestamp,
                    headers=headers,
                )
            index.flush()
            return out

        # This key already has withheld records, so this one joins the queue
        # even if its own lookup resolved - that is what keeps within-key order
        # intact, and it is why the index is consulted before `resolved`.
        # Settle everything past its deadline first, so the emission order
        # within the key stays the arrival order across both groups.
        out.extend(
            self._take_timed_out(
                transaction=transaction,
                partition=partition,
                prefix=prefix,
                key=key,
                cutoff=cutoff,
            )
        )

        if not resolved:
            # Nothing to release: queue behind whatever is left. Everything at
            # or below `cutoff` has just been settled, so `cutoff + 1` is a
            # valid lower bound on what remains.
            index.set_earliest(prefix, cutoff + 1)
            self._buffer(
                transaction=transaction,
                index=index,
                partition=partition,
                prefix=prefix,
                key=key,
                receive_ms=now_ms,
                value=value,
                timestamp=timestamp,
                headers=headers,
            )
            index.flush()
            return out

        out.extend(
            self._release(
                transaction=transaction,
                index=index,
                partition=partition,
                prefix=prefix,
                key=key,
                cutoff=cutoff,
            )
        )
        # The releasing record was never in the buffer, so it goes out last.
        out.append((value, key, timestamp, headers))
        index.flush()
        return out

    def _release(
        self,
        *,
        transaction: TimestampedPartitionTransaction,
        index: PendingIndex,
        partition: int,
        prefix: bytes,
        key: Any,
        cutoff: int,
    ) -> list[tuple[Any, Any, int, Any]]:
        """
        Enrich and emit every record still inside its grace window.

        `start=cutoff + 1` is mandatory: `get_interval()` ignores the store's
        expiry floor, so a wider lower bound would read back records that have
        already passed their deadline and enrich them - the one thing the design
        rules out. `[0, cutoff + 1)` and `[cutoff + 1, MAX_RECEIVE_MS)` partition
        the buffer exactly, with no overlap and no gap.

        Each survivor is re-joined under *its own* lookup key, because `on=` may
        derive that key from the value, and with its own event timestamp, because
        configurations are versioned in event time. A survivor whose own
        configuration still does not resolve is emitted with its declared
        defaults: the release is the end of its wait, not a second window.

        :param transaction: The live store transaction.
        :param index: The partition's pending index.
        :param partition: The partition number.
        :param prefix: The releasing key's store prefix.
        :param key: The original message key.
        :param cutoff: Arrival times at or below this are past their deadline.
        :return: The records to emit, oldest first.
        """
        started = time.monotonic()
        survivors = transaction.get_interval(
            start=cutoff + 1,
            end=MAX_RECEIVE_MS,
            prefix=prefix,
        )
        transaction.delete_interval(
            start=cutoff + 1,
            end=MAX_RECEIVE_MS,
            prefix=prefix,
        )
        index.drop(prefix)
        self._bookkeeping.set_count(partition, prefix, 0)

        out: list[tuple[Any, Any, int, Any]] = []
        for envelope in survivors:
            value = envelope_value(envelope)
            timestamp = envelope[ENVELOPE_TIMESTAMP]
            headers = decode_headers(envelope)
            self._lookup.join(
                self._fields,
                self._on(value, key),
                value,
                key,
                timestamp,
                headers,
            )
            out.append((value, key, timestamp, headers))

        if out:
            elapsed_ms = (time.monotonic() - started) * 1000
            if len(out) >= RELEASE_WARN_RECORDS:
                logger.warning(
                    "Lookup buffer released %s records for key %r in a single "
                    "callback (%.1f ms). Lower `max_buffered_per_key` or "
                    "`grace_ms` if this stalls the partition.",
                    len(out),
                    key,
                    elapsed_ms,
                )
            else:
                logger.debug(
                    "Lookup buffer released %s records for key %r in %.1f ms",
                    len(out),
                    key,
                    elapsed_ms,
                )
        return out

    def _take_timed_out(
        self,
        *,
        transaction: TimestampedPartitionTransaction,
        partition: int,
        prefix: bytes,
        key: Any,
        cutoff: int,
    ) -> list[tuple[Any, Any, int, Any]]:
        """
        Settle this key's records that have reached their deadline.

        The stored value is emitted verbatim under `on_timeout="emit"`. It was
        already resolved through every field's `missing()` when it was written,
        so "emit with the declared defaults" and "emit what is stored" are the
        same thing, and `lookup.join()` is deliberately not re-run: a record that
        ran out of grace is never enriched afterwards, even if its configuration
        has since arrived.

        :param transaction: The live store transaction.
        :param partition: The partition number.
        :param prefix: The key's store prefix.
        :param key: The original message key.
        :param cutoff: Arrival times at or below this are past their deadline.
        :return: The records to emit, oldest first - always empty under
            `on_timeout="drop"`.
        """
        if not self._emit_on_timeout:
            dropped = transaction.delete_interval(
                start=0,
                end=cutoff + 1,
                prefix=prefix,
            )
            self._bookkeeping.decrement(partition, prefix, dropped)
            self._bookkeeping.log_dropped(prefix, key, dropped)
            return []

        envelopes = transaction.get_interval(start=0, end=cutoff + 1, prefix=prefix)
        if not envelopes:
            return []

        deleted = transaction.delete_interval(start=0, end=cutoff + 1, prefix=prefix)
        self._bookkeeping.decrement(partition, prefix, deleted)
        return [emit_tuple(envelope, key) for envelope in envelopes]

    def _buffer(
        self,
        *,
        transaction: TimestampedPartitionTransaction,
        index: PendingIndex,
        partition: int,
        prefix: bytes,
        key: Any,
        receive_ms: int,
        value: dict[str, Any],
        timestamp: int,
        headers: Any,
    ) -> None:
        """
        Withhold a record: write it to the store and emit nothing for it.

        :param transaction: The live store transaction.
        :param index: The partition's pending index.
        :param partition: The partition number.
        :param prefix: The key's store prefix.
        :param key: The original message key.
        :param receive_ms: Wall-clock arrival time, in milliseconds.
        :param value: The record value, already joined.
        :param timestamp: The event timestamp, in milliseconds.
        :param headers: The record headers.
        :raises LookupBufferOverflowError: On overflow with
            `on_overflow="raise"`.
        """
        count = self._bookkeeping.count(partition, prefix)
        if count >= self._max_buffered_per_key:
            if self._raise_on_overflow:
                raise LookupBufferOverflowError(
                    f"Lookup buffer for key {key!r} holds {count} records, its "
                    f"`max_buffered_per_key` limit. Raise the limit, lower "
                    f'`grace_ms`, or use `on_overflow="drop-newest"`.'
                )
            self._bookkeeping.log_overflow(prefix, key, count)
            return

        # This is the only call that raises the store's own expiry floor for the
        # prefix, to `receive_ms - grace_ms` - exactly the cutoff the caller has
        # just drained. So every entry left for a prefix always sits at or above
        # its floor, and the store's passive `_expire()` can never delete a
        # record the operator has not already emitted or dropped.
        transaction.set_for_timestamp(
            timestamp=receive_ms,
            value=encode_envelope(value, timestamp, receive_ms, headers),
            prefix=prefix,
        )
        self._bookkeeping.set_count(partition, prefix, count + 1)
        index.ensure(prefix, key, receive_ms)

    def _get_transaction(self, partition: int) -> TimestampedPartitionTransaction:
        """
        Return the live store transaction for a partition.

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
