"""
Per-key counts and rate-limited warnings for the lookup buffer.

The counts are in-memory and per partition: they keep the
`max_buffered_per_key` check off the store, and a stale one self-corrects the
next time the key is swept, through `BufferSweeper._reindex`. They are dropped
on revoke by `BufferTicker._resync`, so a re-assigned partition does not judge
overflow against what it held under a previous assignment.
"""

import logging
import time
from collections import OrderedDict
from typing import Any, Optional

__all__ = ("BufferBookkeeping",)

logger = logging.getLogger(__name__)

# Per key and per warning kind, not global.
LOG_INTERVAL = 60.0

# Cap on the rate limiter's own memory, evicting the least recently warned key.
MAX_RATE_LIMITED_KEYS = 1024


class BufferBookkeeping:
    """Counts what each key holds and rate-limits what the buffer logs."""

    def __init__(self) -> None:
        self._counts: dict[int, dict[bytes, int]] = {}
        self._drop_log: OrderedDict[bytes, list] = OrderedDict()
        self._overflow_log: OrderedDict[bytes, list] = OrderedDict()

    def count(self, partition: int, prefix: bytes) -> int:
        """:return: Records held for a key. Reading never creates state."""
        return self._counts.get(partition, {}).get(prefix, 0)

    def forget(self, partition: int) -> None:
        """Drop every count for a partition, on revoke."""
        self._counts.pop(partition, None)

    def set_count(self, partition: int, prefix: bytes, count: int) -> None:
        """:param count: Records now held for the key. Zero removes the entry."""
        if not count:
            counts = self._counts.get(partition)
            if counts is not None:
                counts.pop(prefix, None)
            return
        self._counts.setdefault(partition, {})[prefix] = count

    def decrement(self, partition: int, prefix: bytes, removed: int) -> None:
        """:param removed: Records that left the store. Clamped at zero."""
        if not removed:
            return
        current = self.count(partition, prefix)
        self.set_count(partition, prefix, max(current - removed, 0))

    def log_dropped(self, prefix: bytes, key: Any, dropped: int) -> None:
        if not dropped:
            return
        total = self._rate_limited(self._drop_log, prefix, dropped)
        if total is not None:
            logger.warning(
                "Lookup buffer dropped %s records for key %r whose "
                'configuration did not arrive within `grace_ms` (on_timeout="drop").',
                total,
                key,
            )

    def log_overflow(self, prefix: bytes, key: Any, count: int) -> None:
        total = self._rate_limited(self._overflow_log, prefix, 1)
        if total is not None:
            logger.warning(
                "Lookup buffer for key %r is full at %s records; dropped %s "
                "newer records. Raise `max_buffered_per_key` or lower `grace_ms`.",
                key,
                count,
                total,
            )

    @staticmethod
    def _rate_limited(
        state: OrderedDict[bytes, list],
        prefix: bytes,
        events: int,
    ) -> Optional[int]:
        now = time.monotonic()
        window = state.get(prefix)
        if window is None:
            state[prefix] = [now, 0]
            while len(state) > MAX_RATE_LIMITED_KEYS:
                state.popitem(last=False)
            return events

        state.move_to_end(prefix)
        window[1] += events
        if now - window[0] < LOG_INTERVAL:
            return None
        total = window[1]
        state[prefix] = [now, 0]
        return total
