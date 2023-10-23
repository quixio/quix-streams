import dataclasses
import time

from confluent_kafka import OFFSET_INVALID

DEFAULT_TIMEOUT = 10.0


class Timeout:
    """
    Utility class to create time-limited `while` loops.

    It keeps track of the time passed since its creation, and checks if the timeout
    expired on each `bool(Timeout)` check.

    Use it while testing the `while` loops to make sure they exit at some point.
    """

    def __init__(self, seconds: float = DEFAULT_TIMEOUT):
        self._end = time.monotonic() + seconds

    def __bool__(self):
        expired = time.monotonic() >= self._end
        if expired:
            raise TimeoutError("Timeout expired")
        return True


@dataclasses.dataclass
class TopicPartitionStub:
    topic: str
    partition: int
    offset: int = OFFSET_INVALID
