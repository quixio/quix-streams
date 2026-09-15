import logging
import time
from collections import OrderedDict
from typing import Any, Callable, Optional

__all__ = ("BufferBookkeeping",)

logger = logging.getLogger(__name__)

LOG_INTERVAL = 60.0

MAX_RATE_LIMITED_KEYS = 1024


class BufferBookkeeping:
    def __init__(self) -> None:
        self._counts: dict[int, dict[bytes, int]] = {}
        self._drop_log: OrderedDict[bytes, list] = OrderedDict()
        self._overflow_log: OrderedDict[bytes, list] = OrderedDict()
        self._unstorable_log: OrderedDict[bytes, list] = OrderedDict()

    def count(self, partition: int, prefix: bytes) -> int:
        return self._counts.setdefault(partition, {}).get(prefix, 0)

    def set_count(self, partition: int, prefix: bytes, count: int) -> None:
        counts = self._counts.setdefault(partition, {})
        if count:
            counts[prefix] = count
        else:
            counts.pop(prefix, None)

    def decrement(self, partition: int, prefix: bytes, removed: int) -> None:
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

    def log_unstorable(
        self,
        prefix: bytes,
        key: Any,
        describe: Callable[[], str],
    ) -> None:
        total = self._rate_limited(self._unstorable_log, prefix, 1)
        if total is not None:
            logger.warning(
                "Lookup buffer could not store %s records for key %r: the state "
                "store's serializer refused %s. Such a record is settled by "
                "`on_timeout` immediately instead of waiting for its "
                "configuration. Reshape the record value if it must be buffered.",
                total,
                key,
                describe(),
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
