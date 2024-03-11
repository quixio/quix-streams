import time
from typing import Dict, List, Optional, Any

from quixstreams.connectors.sinks.base.exceptions import BatchFullError


__all__ = ("Batch", "BatchStore")


# TODO: Docs
# TODO: Wanna make it flexible, but it's probably overkill
# - TODO: Batching by system time is not idempotent and may create duplicates


class Batch:
    def __init__(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        max_messages: int,
        max_interval_seconds: float = 0.0,
    ):
        self._topic = topic
        self._partition = partition
        self._max_messages = max_messages
        self._max_interval_seconds = max_interval_seconds
        self._created_at = time.monotonic()
        self._start_offset = start_offset
        self._end_offset: Optional[int] = None
        self._items: List[Any] = []

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def total_items(self) -> int:
        return len(self._items)

    @property
    def start_offset(self) -> int:
        return self._start_offset

    @property
    def end_offset(self) -> Optional[int]:
        return self._end_offset

    @property
    def items(self) -> List[Any]:
        return self._items

    @property
    def empty(self) -> bool:
        return not bool(self._items)

    @property
    def full(self) -> bool:
        return len(self._items) == self._max_messages

    @property
    def expired(self) -> bool:
        if self._max_interval_seconds <= 0:
            return False

        return time.monotonic() >= (self._created_at + self._max_interval_seconds)

    def add(self, value: Any, offset: int):
        if self.full:
            raise BatchFullError("Batch is full")

        self._items.append(value)
        self._end_offset = offset

    def key(self) -> Dict[str, str]:
        return {
            "topic": self._topic,
            "partition": self._partition,
        }

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} "
            f'topic="{self._topic}[{self._partition}]" '
            f'total_items="{self.total_items}">'
        )


class BatchStore:
    def __init__(
        self, batch_max_messages: int, batch_max_interval_seconds: float = 0.0
    ):
        # TODO: Can the size be 0 but an interval be > 0? E.g. when you don't care about the size, but only about the timer.
        if batch_max_messages <= 0:
            raise ValueError(
                f'"batch_max_messages" should be greater than zero, '
                f"got {batch_max_messages}"
            )
        if batch_max_interval_seconds < 0:
            raise ValueError(
                f'"batch_max_interval_seconds" cannot be negative, '
                f"got {batch_max_interval_seconds}"
            )

        self._batch_max_messages = batch_max_messages
        self._batch_max_interval_seconds = batch_max_interval_seconds
        self._batches = {}

    def get_batch(self, topic: str, partition: int, offset: int) -> Batch:
        key = (topic, partition)
        batch = self._batches.get(key)
        if batch is not None:
            return batch

        new_batch = Batch(
            topic=topic,
            partition=partition,
            start_offset=offset,
            max_messages=self._batch_max_messages,
            max_interval_seconds=self._batch_max_interval_seconds,
        )
        self._batches[key] = new_batch
        return new_batch

    def drop_batch(self, topic: str, partition: int):
        self._batches.pop((topic, partition), None)

    def list_batches(self) -> List[Batch]:
        return list(self._batches.values())
