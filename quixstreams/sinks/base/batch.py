from collections import deque
from typing import Deque, Tuple, List, Any, Iterator

from quixstreams.models import HeaderValue
from .item import SinkItem

__all__ = ("SinkBatch",)


class SinkBatch:
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

    def append(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: List[Tuple[str, HeaderValue]],
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

    @property
    def size(self) -> int:
        return len(self._buffer)

    def __iter__(self) -> Iterator[SinkItem]:
        return iter(self._buffer)
