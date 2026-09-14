import time
from typing import Optional

__all__ = ("Deadline",)


class Deadline:
    """
    A single per-callback monotonic wall-clock budget shared by every Kafka op
    on the revoke path (sink flush, producer flush, transaction abort, and
    transaction commit) so their *combined* time is bounded by one budget rather
    than a multiple of it: each op consumes the *remaining* budget via
    :meth:`remaining`.

    Modeled on ``quixstreams.state.rocksdb.OpenDeadline`` (the per-assign
    open budget), but kept in the ``checkpointing`` package because the revoke
    budget is a checkpointing concern shared by both the ``Application`` and the
    ``KafkaReplicatorSource`` checkpoints. Uses a monotonic clock so it is immune
    to wall-clock changes.

    An "unbounded" budget is represented by ``None`` (see :meth:`from_timeout`):
    off the revoke path every op keeps its legacy blocking behavior.
    """

    __slots__ = ("_deadline",)

    def __init__(self, deadline: float) -> None:
        self._deadline = deadline

    @classmethod
    def from_timeout(cls, timeout: Optional[float]) -> Optional["Deadline"]:
        """
        Build a ``Deadline`` that expires ``timeout`` seconds from now, or return
        ``None`` (an unbounded budget) when ``timeout`` is ``None`` or negative
        (the ``-1.0`` librdkafka "infinite" sentinel).
        """
        if timeout is None or timeout < 0:
            return None
        return cls(time.monotonic() + timeout)

    def remaining(self) -> float:
        """Seconds left until the deadline, clamped to ``>= 0``."""
        return max(0.0, self._deadline - time.monotonic())

    def expired(self) -> bool:
        """``True`` once the deadline has passed."""
        return time.monotonic() >= self._deadline
