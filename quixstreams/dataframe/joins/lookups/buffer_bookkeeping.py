"""
In-process bookkeeping for the non-blocking lookup buffer.

Two things live here that deliberately do *not* live in the state store: how
many records each key currently holds, and the rate limiters that keep the
per-key warnings readable.

The counts are an approximate guard against a pathological rate of unresolvable
records, not the primary bound - the primary bound is `grace_ms`, which caps a
key's buffer at roughly `rate x grace_ms` records. They reset on restart, which
merely re-arms the guard, and the sweep resynchronises them exactly whenever it
reads a prefix. Keeping them out of the store is what stops the buffered path
from turning into an O(N^2) read-per-write.
"""

import logging
import time
from collections import OrderedDict
from typing import Any, Callable, Optional

__all__ = ("BufferBookkeeping",)

logger = logging.getLogger(__name__)

# Rate limit, in seconds, for the per-key "records were dropped", "buffer
# overflowed" and "value cannot be stored" warnings.
LOG_INTERVAL = 60.0

# How many distinct keys each rate limiter tracks at once. A limiter entry is
# never "finished" - a one-off key that drops a record once and is never seen
# again would otherwise keep its entry for the life of the process, so a
# deployment cycling through per-device keys grows the dicts without bound.
# Evicting the least recently used entry costs only that key's next event
# reporting immediately instead of being folded into a window, which is the same
# thing that happens the first time any key is seen.
MAX_RATE_LIMITED_KEYS = 1024


class BufferBookkeeping:
    """
    Per-partition counts and per-key rate-limited warnings for one operator.
    """

    def __init__(self) -> None:
        # {partition: {prefix: number of records currently withheld}}
        self._counts: dict[int, dict[bytes, int]] = {}
        # {prefix: [window start (monotonic), events since the window started]},
        # least-recently-used first, capped at `MAX_RATE_LIMITED_KEYS`.
        self._drop_log: OrderedDict[bytes, list] = OrderedDict()
        self._overflow_log: OrderedDict[bytes, list] = OrderedDict()
        self._unstorable_log: OrderedDict[bytes, list] = OrderedDict()

    def count(self, partition: int, prefix: bytes) -> int:
        """
        Return how many records a prefix is believed to hold.

        :param partition: The partition number.
        :param prefix: The store prefix.
        :return: The count, `0` if the prefix is unknown.
        """
        return self._counts.setdefault(partition, {}).get(prefix, 0)

    def set_count(self, partition: int, prefix: bytes, count: int) -> None:
        """
        Record how many records a prefix currently holds.

        :param partition: The partition number.
        :param prefix: The store prefix.
        :param count: The new count; `0` forgets the prefix.
        """
        counts = self._counts.setdefault(partition, {})
        if count:
            counts[prefix] = count
        else:
            counts.pop(prefix, None)

    def decrement(self, partition: int, prefix: bytes, removed: int) -> None:
        """
        Reduce a prefix's count after records left the buffer.

        :param partition: The partition number.
        :param prefix: The store prefix.
        :param removed: How many records were removed.
        """
        if not removed:
            return
        current = self.count(partition, prefix)
        self.set_count(partition, prefix, max(current - removed, 0))

    def log_dropped(self, prefix: bytes, key: Any, dropped: int) -> None:
        """
        Warn, at most once per key per `LOG_INTERVAL`, that records were dropped.

        `on_timeout="drop"` exists to make records disappear, which makes it the
        mode that most needs a trace in the logs of a service nobody is watching.

        :param prefix: The store prefix.
        :param key: The original message key.
        :param dropped: How many records were just dropped.
        """
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
        """
        Warn, at most once per key per `LOG_INTERVAL`, that a record was not
        buffered because the key is full.

        An overflowed record never entered the buffer, so it has no deadline and
        `on_timeout` does not apply to it - it is simply gone.

        :param prefix: The store prefix.
        :param key: The original message key.
        :param count: How many records the key currently holds.
        """
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
        """
        Warn, at most once per key per `LOG_INTERVAL`, that a record's value
        cannot be written to the state store and was settled by `on_timeout`
        instead of being buffered.

        This is silent data degradation otherwise: the record went downstream
        with its declared defaults, or vanished, while its configuration may
        have been one message away. The value's shape is a property of the
        pipeline and not of the traffic, so the user has to be told the shape,
        not just the count.

        `describe` is a callable and not a string because producing the
        description re-serializes the value: it must not be paid on the records
        this limiter suppresses.

        :param prefix: The store prefix.
        :param key: The original message key.
        :param describe: Produces the description of what could not be stored.
        """
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
        """
        Accumulate events per prefix and report a total once per interval.

        The first event for a prefix reports immediately, so a problem is
        visible without waiting out a window.

        The state is an LRU bounded at `MAX_RATE_LIMITED_KEYS`: a key evicted
        after going quiet simply reports immediately the next time it is seen,
        which is what a never-before-seen key does anyway.

        :param state: The rate limiter's per-prefix state.
        :param prefix: The store prefix.
        :param events: How many events just happened.
        :return: The number of events accumulated since the last report, or
            `None` if it is not time to report yet.
        """
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
