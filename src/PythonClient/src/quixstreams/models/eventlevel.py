from enum import Enum


class EventLevel(Enum):
    Trace = 0
    Debug = 1
    Information = 2
    Warning = 3
    Error = 4
    Critical = 5
