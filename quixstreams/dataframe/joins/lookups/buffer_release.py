"""
Bookkeeping for one release: which of a key's withheld records leave, and what
that costs the store.

`delete_interval` addresses a millisecond, not a record, so a millisecond
holding several arrivals is all-or-nothing. A release that frees some of them
and keeps others therefore deletes the whole millisecond and writes the kept
ones back, which is why `ReleasePlan` needs a snapshot of a retained record
taken before the lookup ran on it again.
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

RELEASE_WARN_RECORDS = 1000


def log_release(*, key: Any, emitted: int, retained: int, elapsed_ms: float) -> None:
    """
    Report the size and cost of one release.

    :param key: The message key released.
    :param emitted: Records sent downstream.
    :param retained: Records still inside their grace window.
    :param elapsed_ms: Wall-clock time the release took.
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
    """A record staying in the buffer, with the envelope to rewrite it from."""

    receive_ms: int
    envelope: Optional[Any]


def crowded_milliseconds(survivors: list[Any]) -> set[int]:
    """
    :param survivors: The envelopes a release is about to examine.
    :return: The arrival milliseconds holding more than one of them.
    """
    counts = Counter(envelope[ENVELOPE_RECEIVED] for envelope in survivors)
    return {receive_ms for receive_ms, total in counts.items() if total > 1}


def snapshot_for_rewrite(envelope: Any, crowded: set[int]) -> Optional[Any]:
    """
    Copy an envelope that may have to be written back after its millisecond is
    deleted. Taken before the lookup runs, which mutates the value in place.

    :param envelope: The envelope about to be examined.
    :param crowded: The milliseconds from `crowded_milliseconds()`.
    :return: A deep copy, or `None` if this record's millisecond is its own.
    """
    if envelope[ENVELOPE_RECEIVED] not in crowded:
        return None
    return deepcopy(envelope)


class ReleasePlan:
    """What one release frees and what it keeps, as store operations."""

    def __init__(self) -> None:
        self._order: list[int] = []
        self._released: set[int] = set()
        self._retained: list[Withheld] = []

    @property
    def retained_count(self) -> int:
        return len(self._retained)

    @property
    def earliest_retained(self) -> Optional[int]:
        return self._retained[0].receive_ms if self._retained else None

    def release(self, receive_ms: int) -> None:
        """
        Record that the arrival at `receive_ms` is leaving the buffer.

        :param receive_ms: The record's arrival time.
        """
        self._note(receive_ms)
        self._released.add(receive_ms)

    def keep(self, withheld: Withheld) -> None:
        """
        Record that an arrival stays. Must be called in arrival order.

        :param withheld: The record and the envelope to rewrite it from.
        """
        self._note(withheld.receive_ms)
        self._retained.append(withheld)

    def _note(self, receive_ms: int) -> None:
        # Arrival order, deduplicated, so `_delete_released` can turn runs of
        # released milliseconds into one `delete_interval` each.
        if not self._order or self._order[-1] != receive_ms:
            self._order.append(receive_ms)

    def apply(
        self,
        transaction: TimestampedPartitionTransaction,
        prefix: bytes,
    ) -> None:
        """
        Delete the released records and write back their crowded neighbours.

        :param transaction: The store transaction of the record's partition.
        :param prefix: The store prefix of the key being released.
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
        # A retained record whose millisecond was deleted for a neighbour. The
        # rewrite lands under a fresh duplicate counter, so it keeps its arrival
        # time but sorts after the survivors already there.
        for withheld in self._retained:
            if withheld.envelope is None or withheld.receive_ms not in self._released:
                continue
            transaction.set_for_timestamp(
                timestamp=withheld.receive_ms,
                value=withheld.envelope,
                prefix=prefix,
            )
