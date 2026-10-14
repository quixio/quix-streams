"""
The record path of the lookup buffer: everything that happens while a record is
passing through `join_lookup(..., buffer=...)`.

The same work driven by the clock instead of by a record lives in
`buffer_tick.py`, and the part both share in `buffer_sweep.py`.
"""

import time
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, Optional, cast

from quixstreams.context import message_context
from quixstreams.core.stream import VoidExecutor
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .base import BaseField, BaseLookup
from .buffer_bookkeeping import BufferBookkeeping
from .buffer_envelope import (
    ENVELOPE_OFFSET,
    ENVELOPE_RECEIVED,
    ENVELOPE_TIMESTAMP,
    ENVELOPE_TOPIC,
    NO_OFFSET,
    Emission,
    decode_headers,
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

__all__ = ("BufferOperator",)


class BufferOperator:
    """The callable `join_lookup()` installs on the record path."""

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
        Give the deadline tick the executor to emit through.

        :param downstream: The composed executor of everything after this node.
        """
        self._ticker.bind_downstream(downstream)

    def tick(self) -> None:
        """Settle whatever is due on the clock. Registered as a periodic task."""
        self._ticker.tick()

    def __call__(
        self,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: Any,
    ) -> list[Emission]:
        """
        Run one record through the buffer.

        :param value: The record value, enriched in place by `lookup.join()`.
        :param key: The message key, which groups the buffer.
        :param timestamp: The record's event timestamp.
        :param headers: The record headers.
        :return: What to send downstream: other keys' settled records first,
            then this key's released ones in arrival order, then this record
            itself if it is leaving now.
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

        settled = self._sweeper.sweep(transaction, index, cutoff, prefix)
        out = settled.emissions

        marker = index.get(prefix)
        if marker is None:
            if resolved:
                out.append(
                    Emission(
                        value, key, timestamp, headers, context.topic, context.offset
                    )
                )
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
                    topic=context.topic,
                    offset=context.offset,
                )
            index.flush()
            return out

        out.extend(
            self._take_timed_out(
                transaction=transaction,
                index=index,
                prefix=prefix,
                key=key,
                cutoff=cutoff,
            )
        )

        if not resolved:
            # `_take_timed_out` has drained everything at or below `cutoff`, so
            # nothing survives below `cutoff + 1`. The marker only moves forward:
            # lowering it re-queues the key as due earlier than it is.
            index.set_earliest(prefix, max(marker[0], cutoff + 1))
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
            index.flush()
            return out

        out.extend(
            self._release(
                transaction=transaction,
                index=index,
                prefix=prefix,
                key=key,
                cutoff=cutoff,
            )
        )
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
        prefix: bytes,
        key: Any,
        cutoff: int,
    ) -> list[Emission]:
        started = time.monotonic()
        # `ReleasePlan` deletes whole milliseconds, so this read has to cover
        # every survivor: `max_buffered_per_key` is its only bound, and
        # `log_release` warns past RELEASE_WARN_RECORDS.
        survivors = transaction.get_interval(
            start=cutoff + 1,
            end=MAX_RECEIVE_MS,
            prefix=prefix,
        )
        crowded = crowded_milliseconds(survivors)

        plan = ReleasePlan()
        out: list[Emission] = []
        for envelope in survivors:
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
            index.set_earliest(prefix, earliest)
        index.set_count(prefix, plan.retained_count)

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
        index: PendingIndex,
        prefix: bytes,
        key: Any,
        cutoff: int,
    ) -> list[Emission]:
        if not self._emit_on_timeout:
            dropped = transaction.delete_interval(
                start=0,
                end=cutoff + 1,
                prefix=prefix,
            )
            index.decrement(prefix, dropped)
            self._bookkeeping.log_dropped(prefix, key, dropped)
            return []

        envelopes = transaction.get_interval(start=0, end=cutoff + 1, prefix=prefix)
        if not envelopes:
            return []

        deleted = transaction.delete_interval(start=0, end=cutoff + 1, prefix=prefix)
        index.decrement(prefix, deleted)
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
    ) -> None:
        count = index.count(prefix)
        if count >= self._max_buffered_per_key:
            self._bookkeeping.log_overflow(prefix, key, count)
            return

        envelope = encode_envelope(
            value=value,
            timestamp=timestamp,
            receive_ms=receive_ms,
            headers=headers,
            topic=topic,
            offset=offset,
        )
        transaction.set_for_timestamp(
            timestamp=receive_ms,
            value=envelope,
            prefix=prefix,
        )
        index.ensure(prefix, key, receive_ms)
        index.set_count(prefix, count + 1)
        self._ticker.note_deadline(partition, receive_ms + self._grace_ms)

    def _get_transaction(self, partition: int) -> TimestampedPartitionTransaction:
        return cast(
            TimestampedPartitionTransaction,
            self._dataframe.processing_context.checkpoint.get_store_transaction(
                stream_id=self._dataframe.stream_id,
                partition=partition,
                store_name=self._store_name,
            ),
        )
