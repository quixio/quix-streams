"""
The record path of the non-blocking lookup buffer.

`BufferOperator` is the callable behind the `Stream` node
`StreamingDataFrame.join_lookup(..., buffer=...)` appends. One input record can
produce any number of output records, each with its own value, key, timestamp
and headers - which is the whole reason this is a transform and not an
`apply(expand=True)`: released records must keep their *own* event timestamps,
and the sweep must be able to emit records that belong to a different key
entirely.

The node itself is a `BufferTransformFunction` (`buffer_node.py`) rather than a
plain `add_transform(..., expand=True)`, because the deadline tick
(`buffer_tick.py`) has to emit with no input record to expand from and therefore
needs the resolved child executor handed to it at compose time.

Per record it does three things, in this order:

1. **Join.** Run the lookup once for the incoming record and ask `is_resolved`
   whether it worked.
2. **Sweep.** Settle a bounded slice of the other keys on this partition whose
   withheld records are past their deadline (see `buffer_sweep.py`). It runs
   before the branch below, so resolvable and unresolvable records alike drive
   it.
3. **Buffer, release, or pass through.** A record whose key has nothing withheld
   and whose lookup resolved goes straight out. A record whose lookup failed is
   written to the store and nothing is emitted for it - unless the store cannot
   serialize its value, the one case where nothing is withheld at all (below).
   A record whose key *does* have withheld records first settles everything past
   its deadline, then - if it resolved - releases the survivors whose *own*
   lookup key resolves too and goes out behind them, or joins the queue if it
   did not.

A release is therefore per record and not per message key. The buffer is grouped
by the message key while resolvability is decided by the lookup key, so one
message key can carry several of them; a survivor whose own configuration has
still not arrived stays buffered for the rest of its `grace_ms` instead of
leaving with a sibling that resolved something else. `buffer_release.py` carries
that decision into the store, and its module docstring is where the reasoning
lives.

A record can also fail to be withheld at all. `_buffer()` offers the finished
envelope to the store's own serializer *before* the write, and a value that
serializer refuses - an arbitrary object, a dict with non-`str` keys, an integer
outside 64 bits, a cycle - is settled by `on_timeout` there and then instead of
being buffered. Catching the failure afterwards is not an option:
`PartitionTransaction.set()` marks the transaction FAILED before it re-raises,
so the checkpoint fails, the offset is never committed, and redelivery
reproduces the crash on the same record forever - on precisely the path the
buffer exists to serve.

The single most important line in the file is the `start=cutoff + 1` lower bound
on the survivor read in `_release()`. `get_interval()` does not consult the
store's expiry floor the way `get_latest()` does, so without that bound a record
that has already passed its deadline would be read back and enriched, which is
exactly what the design forbids.
"""

import time
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, Optional, cast

from quixstreams.context import message_context
from quixstreams.core.stream import VoidExecutor
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .base import BaseField, BaseLookup
from .buffer_bookkeeping import BufferBookkeeping
from .buffer_envelope import (
    ENVELOPE_HEADERS,
    ENVELOPE_OFFSET,
    ENVELOPE_RECEIVED,
    ENVELOPE_TIMESTAMP,
    ENVELOPE_TOPIC,
    ENVELOPE_VALUE,
    NO_OFFSET,
    Emission,
    decode_headers,
    describe_unstorable,
    emit_tuple,
    encode_envelope,
    envelope_value,
)
from .buffer_release import (
    ReleasePlan,
    Withheld,
    crowded_milliseconds,
    log_release,
    snapshot_for_rewrite,
)
from .buffer_state import MAX_RECEIVE_MS, PendingIndex, prefix_for_key
from .buffer_sweep import BufferSweeper
from .buffer_tick import BufferTicker

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("BufferOperator", "LookupBufferOverflowError")


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
        self._ticker = BufferTicker(
            dataframe=dataframe,
            store_name=store_name,
            grace_ms=grace_ms,
            sweeper=self._sweeper,
        )

    def bind_downstream(self, downstream: VoidExecutor) -> None:
        """
        Hand the resolved child executor to the deadline tick.

        :param downstream: The executor for this operator's Stream node.
        """
        self._ticker.bind_downstream(downstream)

    def tick(self) -> None:
        """
        Settle everything past its deadline, with no input record.

        Registered with `DataFrameRegistry.register_periodic_task()`, so it runs
        once per Application loop iteration.
        """
        self._ticker.tick()

    def __call__(
        self,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
    ) -> list[Emission]:
        """
        Handle one record and return everything that should go downstream.

        :param value: The record value.
        :param key: The message key.
        :param timestamp: The event timestamp, in milliseconds.
        :param headers: The record headers.
        :return: A list of `Emission`s, possibly empty - an empty list is how a
            record is withheld.
        """
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self._grace_ms
        context = message_context()
        partition = context.partition
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
        # settles the deadlines of unconfigured ones. The deadline tick drives
        # the same sweep when there is no traffic at all.
        settled = self._sweeper.sweep(transaction, index, partition, cutoff, prefix)
        out = settled.emissions

        if index.get(prefix) is None:
            if resolved:
                out.append(
                    Emission(
                        value, key, timestamp, headers, context.topic, context.offset
                    )
                )
            else:
                out.extend(
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
                        topic=context.topic,
                        offset=context.offset,
                    )
                )
            index.flush()
            return out

        # This key already has withheld records, which is why the index is
        # consulted before `resolved`: whatever this record does, it goes out
        # behind every record of its own lookup key that is already waiting.
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
            # valid lower bound on what remains - and it stays one even if
            # `_buffer()` then declines to store this record, in which case the
            # marker is merely stale-low and the next sweep of the prefix
            # recomputes or drops it.
            index.set_earliest(prefix, cutoff + 1)
            out.extend(
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
                    topic=context.topic,
                    offset=context.offset,
                )
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
        # The releasing record was never in the buffer, so it goes out behind
        # everything the release emitted. Survivors whose own lookup key did not
        # resolve stayed behind instead, so this record can overtake an older
        # sibling that is still waiting on a different configuration - see
        # `_release()`.
        out.append(
            Emission(value, key, timestamp, headers, context.topic, context.offset)
        )
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
    ) -> list[Emission]:
        """
        Emit the survivors whose own lookup key resolves, and leave the rest
        buffered.

        `start=cutoff + 1` is mandatory: `get_interval()` ignores the store's
        expiry floor, so a wider lower bound would read back records that have
        already passed their deadline and enrich them - the one thing the design
        rules out. `[0, cutoff + 1)` and `[cutoff + 1, MAX_RECEIVE_MS)` partition
        the buffer exactly, with no overlap and no gap.

        Each survivor is re-joined under *its own* lookup key, because `on=` may
        derive that key from the value, and with its own event timestamp, because
        configurations are versioned in event time. The verdict is then that
        record's alone:

        - it resolves: emitted enriched, and its store entry goes;
        - it does not: it stays exactly where it is, at the arrival time it has
          always had, and goes on waiting for the rest of its own `grace_ms`.
          Nothing else can end its wait early - not a sibling's configuration,
          only its own or its deadline.

        The second case is what lets the releasing record leave ahead of an
        older sibling. Arrival order is preserved for every record sharing a
        lookup key, which is the order that can carry meaning, and not across
        lookup keys that merely share a message key. `buffer_release.py` carries
        the verdicts into the store.

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
        crowded = crowded_milliseconds(survivors)

        plan = ReleasePlan()
        out: list[Emission] = []
        for envelope in survivors:
            # Before `envelope_value()` and `lookup.join()` get at it, because
            # both write into the envelope this may have to put back.
            keepsake = snapshot_for_rewrite(envelope, crowded)
            receive_ms = envelope[ENVELOPE_RECEIVED]
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
            if not self._is_resolved(value):
                plan.keep(Withheld(receive_ms, keepsake))
                continue
            plan.release(receive_ms)
            out.append(
                Emission(
                    value,
                    key,
                    timestamp,
                    headers,
                    envelope.get(ENVELOPE_TOPIC),
                    envelope.get(ENVELOPE_OFFSET, NO_OFFSET),
                )
            )

        plan.apply(transaction, prefix)

        earliest = plan.earliest_retained
        if earliest is None:
            index.drop(prefix)
        else:
            # The marker moves FORWARD, onto the oldest record still waiting,
            # and `flush()` moves its deadline-queue entry with it. Dropping it
            # instead would hide these records from the sweep and the tick until
            # their key's next record - the stranding this feature exists to
            # remove. The tick's cached per-partition deadline needs no update:
            # it is a lower bound, it was set from these records' own deadlines
            # when they were withheld, and a marker that only ever moves forward
            # can leave it too low but never too high.
            index.set_earliest(prefix, earliest)
        self._bookkeeping.set_count(partition, prefix, plan.retained_count)

        log_release(
            key=key,
            emitted=len(out),
            retained=plan.retained_count,
            elapsed_ms=(time.monotonic() - started) * 1000,
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
    ) -> list[Emission]:
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
        topic: Optional[str],
        offset: int,
    ) -> list[Emission]:
        """
        Withhold a record: write it to the store and emit nothing for it.

        A value the store cannot serialize is the one case where nothing is
        withheld. The check has to come *before* the write and not around it:
        `PartitionTransaction.set()` catches the serialization error, marks the
        transaction FAILED and re-raises, so by the time an exception is visible
        here the checkpoint is already lost and the offset will never be
        committed - an unattended crash-loop on the exact record the buffer was
        asked to look after. So the envelope is built and then offered to the
        transaction's own value serializer first, and only a value that survives
        both steps is stored.

        The price is one extra serialization per withheld record, on the
        degraded path only: a record whose configuration arrived is never
        buffered and never pays it.

        :param transaction: The live store transaction.
        :param index: The partition's pending index.
        :param partition: The partition number.
        :param prefix: The key's store prefix.
        :param key: The original message key.
        :param receive_ms: Wall-clock arrival time, in milliseconds.
        :param value: The record value, already joined.
        :param timestamp: The event timestamp, in milliseconds.
        :param headers: The record headers.
        :param topic: The name of the topic the record arrived on.
        :param offset: The record's offset on that topic-partition.
        :return: What to emit for this record - empty when it was withheld or
            overflowed, and the record itself when it could not be stored and
            `on_timeout="emit"`.
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
            return []

        # `_serialize_value` is the very method `set()` calls on the way to
        # RocksDB, so this check cannot disagree with the write it guards - not
        # even for a store configured with its own `dumps`. It is also the only
        # serialization `set_for_timestamp()` performs that can fail: the store
        # key is `encode_integer_pair()` bytes, the prefix is already bytes, and
        # the expiry floor is written with `set_bytes()`.
        serialize_value = transaction._serialize_value  # noqa: SLF001
        envelope: Optional[dict[str, Any]] = None
        try:
            envelope = encode_envelope(
                value=value,
                timestamp=timestamp,
                receive_ms=receive_ms,
                headers=headers,
                topic=topic,
                offset=offset,
            )
            serialize_value(envelope)
        except (RecursionError, StateSerializationError):
            # Two ways to be unstorable, one answer. The serializer refuses
            # most shapes itself; a reference cycle never reaches it, because
            # `encode_envelope()` walks the value without cycle detection and
            # exhausts the stack first. Both crash-loop the same partition on
            # the same record, so both are settled the same way. Adding cycle
            # tracking to the encode walk instead would cost every withheld
            # record a set, to catch a shape nobody stores on purpose.
            refused: Mapping[str, Any] = (
                envelope
                if envelope is not None
                else {ENVELOPE_VALUE: value, ENVELOPE_HEADERS: headers}
            )
            self._bookkeeping.log_unstorable(
                prefix,
                key,
                lambda: describe_unstorable(refused, serialize_value),
            )
            return self._settle_unstorable(
                value=value,
                key=key,
                timestamp=timestamp,
                headers=headers,
                topic=topic,
                offset=offset,
            )

        # This is the only call that raises the store's own expiry floor for the
        # prefix, to `receive_ms - grace_ms` - exactly the cutoff the caller has
        # just drained. So every entry left for a prefix always sits at or above
        # its floor, and the store's passive `_expire()` can never delete a
        # record the operator has not already emitted or dropped.
        transaction.set_for_timestamp(
            timestamp=receive_ms,
            value=envelope,
            prefix=prefix,
        )
        self._bookkeeping.set_count(partition, prefix, count + 1)
        index.ensure(prefix, key, receive_ms)
        # Withholding a record is the only event that can leave the tick's
        # cached deadline later than something actually due. Telling it now is
        # one `min` against a scalar, on a path that is already writing to
        # RocksDB, and it only ever lowers the cache - which is the direction
        # that stays a valid lower bound.
        self._ticker.note_deadline(partition, receive_ms + self._grace_ms)
        return []

    def _settle_unstorable(
        self,
        *,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
        topic: Optional[str],
        offset: int,
    ) -> list[Emission]:
        """
        Resolve a record the store cannot hold, exactly as its deadline would.

        There is one answer to "this record is not getting its configuration",
        and `on_timeout` is it. Under `"emit"` the value goes downstream as it
        stands, which is already every field's `missing()` - `lookup.join()` ran
        on it before it reached `_buffer()`, so this is the same value
        `_take_timed_out()` would have emitted `grace_ms` later. Under `"drop"`
        it is discarded, as a timed-out record is.

        What is lost is the wait, and with it ordering: the record leaves now
        rather than at its deadline, so it can overtake records of its own
        lookup key that are still buffered. That is the trade for not
        crash-looping the partition, and `log_unstorable()` is what keeps it
        from being silent.

        :param value: The record value, already joined.
        :param key: The original message key.
        :param timestamp: The event timestamp, in milliseconds.
        :param headers: The record headers.
        :param topic: The name of the topic the record arrived on.
        :param offset: The record's offset on that topic-partition.
        :return: The record, or nothing under `on_timeout="drop"`.
        """
        if not self._emit_on_timeout:
            return []
        return [Emission(value, key, timestamp, headers, topic, offset)]

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
