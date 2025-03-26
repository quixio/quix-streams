from typing import Protocol


class TopicPartition(Protocol):
    topic: str
    partition: int
    offset: int
