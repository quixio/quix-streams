"""Rate-limited warnings for the lookup buffer."""

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
    """Rate-limits what the buffer logs."""

    def __init__(self) -> None:
        self._drop_log: OrderedDict[bytes, list] = OrderedDict()
        self._overflow_log: OrderedDict[bytes, list] = OrderedDict()

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
