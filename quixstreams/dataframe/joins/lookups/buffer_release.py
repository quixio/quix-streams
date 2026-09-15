"""
What a release does to the store: emit what resolved, keep what is still waiting.

A release used to be all-or-nothing per message key. The buffer groups by the
**message** key, but resolvability is decided by the **lookup** key, and with an
`on=` that reads that key out of the value one message key can carry several of
them. So a record whose own configuration had not arrived was emitted with its
declared defaults the moment a sibling's did - inside its own grace window,
which `grace_ms` exists to promise it. This module is the half of the fix that
touches the store; `BufferOperator._release()` is the half that classifies.

The classification is three-way, and the operator drives it one survivor at a
time:

1. **Resolved.** Its own lookup key resolves now. It is emitted enriched, and
   its arrival millisecond is handed to `ReleasePlan.release()`.
2. **Past its deadline.** Never reaches here. `BufferOperator._take_timed_out()`
   settles `[0, cutoff + 1)` before the release reads
   `[cutoff + 1, MAX_RECEIVE_MS)`, so every record this module sees is still
   inside its window and `on_timeout` is not this module's business.
3. **Unresolved and still inside its window.** It stays buffered, handed to
   `ReleasePlan.keep()`.

Keeping a record means keeping its *store entry*, not writing an equivalent one.
That entry's key is `(arrival millisecond, duplicate counter)` and both halves
are load-bearing: the millisecond is the record's deadline, and the counter is
what orders same-millisecond arrivals within the key. So `apply()` deletes only
the milliseconds that actually gave a record up, coalescing neighbours so a run
of them costs one range delete, and leaves every other millisecond untouched -
no rewrite, no new counter, no restarted deadline.

One case cannot be expressed as a range delete: a millisecond holding both a
released and a retained record, which is what two records for one message key
arriving inside the same millisecond produce. `delete_interval()` addresses
whole milliseconds, so the released one cannot leave without the retained one
going with it. Those retained records - and only those - are written back
afterwards, from a snapshot `snapshot_for_rewrite()` took *before* the release
restored and re-joined the envelope, so what goes back is the stored bytes and
not a re-encoding of a second `lookup.join()`. `crowded_milliseconds()` decides
in advance which survivors can possibly need one, so the copy is paid for only
where a millisecond really does hold more than one record.

Writing them back is safe on both counts the store key encodes:

- **Arrival time.** The record goes back at its own `receive_ms`, so its
  deadline is the one it has always had and `grace_ms` is not restarted. The
  only side effect is the store's per-prefix expiry floor, which
  `set_for_timestamp()` raises to `receive_ms - grace_ms`; every survivor's
  `receive_ms` is at most "now", so that floor is at or below the cutoff the
  caller has just drained, and strictly below every record that remains.
- **Duplicate counter.** `set_for_timestamp()` takes the next value of the
  partition's persisted global counter (`GLOBAL_COUNTER_KEY`,
  `state/rocksdb/transaction.py`), which is strictly greater than every counter
  ever issued on this partition - so a rewritten key cannot collide with a live
  one. The rewrites run in the order the records were read, which is store
  order, so the new counters ascend exactly as the old ones did and the
  within-key order survives.
"""

import logging
from collections import Counter
from copy import deepcopy
from typing import Any, NamedTuple, Optional

from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .buffer_envelope import ENVELOPE_RECEIVED

__all__ = (
    "RELEASE_WARN_RECORDS",
    "ReleasePlan",
    "Withheld",
    "crowded_milliseconds",
    "log_release",
    "snapshot_for_rewrite",
)

logger = logging.getLogger(__name__)

# A release deserializes and re-joins a key's entire surviving buffer inside one
# callback, whether or not each record then leaves. Past this many records that
# is a stall worth telling the user about.
RELEASE_WARN_RECORDS = 1000


def log_release(*, key: Any, emitted: int, retained: int, elapsed_ms: float) -> None:
    """
    Report what one release cost, if it did anything at all.

    The cost is driven by how many records were *examined* - every one of them
    is deserialized and re-joined - and not by how many then left, so a release
    that keeps everything it read is exactly as expensive as one that emits it.
    That is what the warning counts.

    :param key: The original message key.
    :param emitted: How many records the release emitted.
    :param retained: How many it left in the buffer.
    :param elapsed_ms: How long the release took, in milliseconds.
    """
    examined = emitted + retained
    if not examined:
        return
    if examined >= RELEASE_WARN_RECORDS:
        logger.warning(
            "Lookup buffer examined %s records for key %r in a single callback "
            "(%.1f ms), emitting %s and keeping %s. Lower `max_buffered_per_key` "
            "or `grace_ms` if this stalls the partition.",
            examined,
            key,
            elapsed_ms,
            emitted,
            retained,
        )
    else:
        logger.debug(
            "Lookup buffer released %s records for key %r in %.1f ms, keeping "
            "%s still inside their grace window",
            emitted,
            key,
            elapsed_ms,
            retained,
        )


class Withheld(NamedTuple):
    """
    A survivor whose own lookup key still does not resolve, and which therefore
    stays in the buffer.

    `envelope` is a copy of the stored envelope taken before the release
    restored and re-joined it, and it is `None` for every record that had its
    arrival millisecond to itself. Such a record can never be caught by a range
    delete aimed at a sibling, so it is never rewritten and never needs a copy.
    """

    receive_ms: int
    envelope: Optional[Any]


def crowded_milliseconds(survivors: list[Any]) -> set[int]:
    """
    Return the arrival milliseconds that hold more than one survivor.

    These are the only milliseconds a release can be forced to rewrite, because
    they are the only ones where "delete what left" and "keep what stayed" can
    disagree inside a single `delete_interval()` unit.

    :param survivors: The envelopes read for this release, oldest first.
    :return: The arrival times, in milliseconds, holding two or more of them.
    """
    counts = Counter(envelope[ENVELOPE_RECEIVED] for envelope in survivors)
    return {receive_ms for receive_ms, total in counts.items() if total > 1}


def snapshot_for_rewrite(envelope: Any, crowded: set[int]) -> Optional[Any]:
    """
    Copy an envelope that a release may have to write back, before it is read.

    `envelope_value()` restores the envelope's binary leaves and container types
    **in place**, and `lookup.join()` then writes into the value it returned, so
    by the time a record is classified its envelope is no longer what the store
    holds - it carries `bytes` and `set`s that orjson would reject, and a second
    join's output rather than the arrival-time one. A record that has to go back
    must go back as it was, so the copy is taken first.

    :param envelope: An envelope straight out of `get_interval()`.
    :param crowded: The milliseconds produced by `crowded_milliseconds()`.
    :return: A deep copy, or `None` when this record's millisecond is its own
        and a rewrite is therefore impossible.
    """
    if envelope[ENVELOPE_RECEIVED] not in crowded:
        return None
    return deepcopy(envelope)


class ReleasePlan:
    """
    The per-record verdict of one release, and the store surgery it implies.

    Records are declared in the order they were read, which is store order -
    ascending arrival millisecond, then ascending duplicate counter. Both
    `earliest_retained` and the ordering of the rewrites rest on that, so a
    caller that feeds the plan out of order gets both wrong.
    """

    def __init__(self) -> None:
        # Distinct arrival milliseconds, in the order they were declared.
        self._order: list[int] = []
        # The subset of them that gave up at least one record.
        self._released: set[int] = set()
        self._retained: list[Withheld] = []

    @property
    def retained_count(self) -> int:
        """How many records this release leaves in the buffer."""
        return len(self._retained)

    @property
    def earliest_retained(self) -> Optional[int]:
        """
        The arrival time of the oldest record left behind, or `None` when the
        release emptied the prefix.

        This is what the pending index's marker has to become: the prefix is
        still waiting, and for the record that has waited longest.
        """
        return self._retained[0].receive_ms if self._retained else None

    def release(self, receive_ms: int) -> None:
        """
        Declare that the record arriving at `receive_ms` is leaving the buffer.

        :param receive_ms: Its arrival time, in milliseconds.
        """
        self._note(receive_ms)
        self._released.add(receive_ms)

    def keep(self, withheld: Withheld) -> None:
        """
        Declare that a record stays in the buffer, still inside its own window.

        :param withheld: The record, with the rewrite snapshot that
            `snapshot_for_rewrite()` produced for it.
        """
        self._note(withheld.receive_ms)
        self._retained.append(withheld)

    def _note(self, receive_ms: int) -> None:
        """
        Record an arrival millisecond once, in the order it was declared.

        Comparing against the last entry is enough because the caller declares
        records in store order, so equal milliseconds are adjacent.

        :param receive_ms: The arrival time, in milliseconds.
        """
        if not self._order or self._order[-1] != receive_ms:
            self._order.append(receive_ms)

    def apply(
        self,
        transaction: TimestampedPartitionTransaction,
        prefix: bytes,
    ) -> None:
        """
        Remove what left the buffer and put back what a neighbour's delete took
        with it.

        A release that emitted nothing touches the store not once: nothing left,
        so nothing has to move, and every survivor keeps its entry, its counter
        and its place untouched by construction.

        :param transaction: The live store transaction.
        :param prefix: The releasing key's store prefix.
        """
        if not self._released:
            return
        self._delete_released(transaction, prefix)
        self._rewrite_collateral(transaction, prefix)

    def _delete_released(
        self,
        transaction: TimestampedPartitionTransaction,
        prefix: bytes,
    ) -> None:
        """
        Delete exactly the milliseconds that gave up a record.

        Neighbouring ones are coalesced into a single `delete_interval()`, and a
        millisecond that gave up nothing ends the run - so a release that frees
        the whole buffer still costs one range delete, while one that frees a
        single record out of a thousand costs one too, and the other 999 entries
        are not even read.

        The runs cover the range exactly: `self._order` is every millisecond the
        survivor read returned, and the read returned everything stored in
        `[cutoff + 1, MAX_RECEIVE_MS)`, so nothing released can fall outside a
        run and nothing retained can fall inside one - except in a crowded
        millisecond, which `_rewrite_collateral()` puts back.

        :param transaction: The live store transaction.
        :param prefix: The releasing key's store prefix.
        """
        run_start: Optional[int] = None
        run_end = 0
        for receive_ms in self._order:
            if receive_ms in self._released:
                if run_start is None:
                    run_start = receive_ms
                run_end = receive_ms + 1
            elif run_start is not None:
                transaction.delete_interval(start=run_start, end=run_end, prefix=prefix)
                run_start = None
        if run_start is not None:
            transaction.delete_interval(start=run_start, end=run_end, prefix=prefix)

    def _rewrite_collateral(
        self,
        transaction: TimestampedPartitionTransaction,
        prefix: bytes,
    ) -> None:
        """
        Write back the retained records a neighbour's delete took with them.

        Only records sharing a millisecond with a released one qualify, which is
        exactly the set that has a snapshot. The value written is the snapshot
        itself, so the stored envelope is the one the record arrived with - the
        same thing the timeout path would have emitted a moment earlier.

        :param transaction: The live store transaction.
        :param prefix: The releasing key's store prefix.
        """
        for withheld in self._retained:
            if withheld.envelope is None or withheld.receive_ms not in self._released:
                continue
            transaction.set_for_timestamp(
                timestamp=withheld.receive_ms,
                value=withheld.envelope,
                prefix=prefix,
            )
