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
    receive_ms: int
    envelope: Optional[Any]


def crowded_milliseconds(survivors: list[Any]) -> set[int]:
    counts = Counter(envelope[ENVELOPE_RECEIVED] for envelope in survivors)
    return {receive_ms for receive_ms, total in counts.items() if total > 1}


def snapshot_for_rewrite(envelope: Any, crowded: set[int]) -> Optional[Any]:
    if envelope[ENVELOPE_RECEIVED] not in crowded:
        return None
    return deepcopy(envelope)


class ReleasePlan:
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
        self._note(receive_ms)
        self._released.add(receive_ms)

    def keep(self, withheld: Withheld) -> None:
        self._note(withheld.receive_ms)
        self._retained.append(withheld)

    def _note(self, receive_ms: int) -> None:
        if not self._order or self._order[-1] != receive_ms:
            self._order.append(receive_ms)

    def apply(
        self,
        transaction: TimestampedPartitionTransaction,
        prefix: bytes,
    ) -> None:
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
        for withheld in self._retained:
            if withheld.envelope is None or withheld.receive_ms not in self._released:
                continue
            transaction.set_for_timestamp(
                timestamp=withheld.receive_ms,
                value=withheld.envelope,
                prefix=prefix,
            )
