from enum import Enum


class StreamEndType(Enum):
    Closed = 0
    Aborted = 1
    Terminated = 2
