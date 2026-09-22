import time
from typing import Optional

__all__ = ("OpenDeadline",)


class OpenDeadline:
    """
    A tiny shared per-assign wall-clock budget for RocksDB opens.

    Armed at the start of ``Application._on_assign`` and shared (like the
    ``stop_event``) with every store partition opened during that rebalance
    callback, so the *total* serial open time across all contended partitions is
    bounded. When the deadline is exceeded the open-retry loop stops and
    re-raises the underlying lock error, keeping today's failure semantics but
    triggering the restart sooner instead of overrunning ``max.poll.interval.ms``.

    Disarmed outside a rebalance so opens elsewhere (e.g. sources) stay
    unbounded. Uses a monotonic clock so it is immune to wall-clock changes.
    """

    def __init__(self) -> None:
        self._deadline: Optional[float] = None

    def arm(self, seconds: float) -> None:
        """Start the budget: opens must finish within ``seconds`` from now."""
        self._deadline = time.monotonic() + seconds

    def disarm(self) -> None:
        """Clear the budget; ``expired()`` is always ``False`` while disarmed."""
        self._deadline = None

    def expired(self) -> bool:
        """``True`` once the armed deadline has passed; ``False`` while disarmed."""
        if self._deadline is None:
            return False
        return time.monotonic() >= self._deadline

    def remaining(self) -> Optional[float]:
        """
        Seconds left until the deadline, or ``None`` while disarmed. Used to
        decide whether the next retry backoff would cross the deadline.
        """
        if self._deadline is None:
            return None
        return self._deadline - time.monotonic()
