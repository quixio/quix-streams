from collections import deque
from itertools import islice
from typing import Any, Deque, Iterable, Iterator

from quixstreams.models import HeadersTuples

from .item import SinkItem

__all__ = ("SinkBatch",)


class SinkBatch:
    """
    A batch to accumulate processed data by `BatchingSink` between the checkpoints.

    Batches are created automatically by the implementations of `BatchingSink`.

    :param topic: a topic name
    :param partition: a partition number
    """

    _buffer: Deque[SinkItem]

    def __init__(self, topic: str, partition: int):
        self._buffer = deque()
        self._partition = partition
        self._topic = topic

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def size(self) -> int:
        return len(self._buffer)

    @property
    def start_offset(self) -> int:
        return self._buffer[0].offset

    def append(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        offset: int,
    ):
        self._buffer.append(
            SinkItem(
                value=value,
                key=key,
                timestamp=timestamp,
                headers=headers,
                offset=offset,
            )
        )

    def clear(self):
        self._buffer.clear()

    def empty(self) -> bool:
        return len(self._buffer) == 0

    def iter_chunks(self, n: int) -> Iterable[Iterable[SinkItem]]:
        """
        Iterate over batch data in chunks of length n.
        The last batch may be shorter.
        """
        if n < 1:
            raise ValueError("n must be at least one")
        it_ = iter(self)
        while batch := tuple(islice(it_, n)):
            yield batch

    def __iter__(self) -> Iterator[SinkItem]:
        return iter(self._buffer)
