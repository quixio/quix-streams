from typing import Any, Protocol


class TopicPartition(Protocol):
    topic: str
    partition: int
    offset: int


class SupportsLessThan(Protocol):
    def __lt__(self, other: Any) -> bool: ...
