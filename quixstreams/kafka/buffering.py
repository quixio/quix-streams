import dataclasses
import sys
from collections import deque
from typing import Dict, List, Optional, Iterator, Tuple

from quixstreams.models import Row

_MAX_FLOAT = sys.float_info.max

__all__ = ("MessageBuffer",)


@dataclasses.dataclass()
class PartitionQueue:
    topic: str
    partition: int
    max_size: int
    priority: float = _MAX_FLOAT
    _items: deque = dataclasses.field(compare=False, default_factory=deque, repr=False)
    _latest_offset: int = -1
    _highwater: int = -1

    def append(self, item: Row):
        if self.priority == _MAX_FLOAT:
            self.priority = item.timestamp
        self._items.append(item)
        self._latest_offset = item.context.offset

    def popleft(self) -> Row:
        item = self._items.popleft()
        try:
            self.priority = self._items[0].timestamp
        except IndexError:
            self.priority = _MAX_FLOAT
        return item

    def size(self) -> int:
        return len(self._items)

    def empty(self) -> bool:
        return len(self._items) / self.max_size == 0

    def full(self) -> bool:
        return len(self._items) / self.max_size == 1

    def set_highwater(self, highwater: int):
        self._highwater = highwater

    def idle(self) -> bool:
        return self._highwater > 0 and self._highwater - self._latest_offset <= 1


class MessageBuffer:
    def __init__(self, max_partition_queue_size: int = 10000):
        self._queues_map: Dict[Tuple[str, int], PartitionQueue] = {}
        self._queues_list: List[PartitionQueue] = []
        self._max_partition_queue_size = max_partition_queue_size

    def size(self) -> int:
        if not self._queues_map:
            return 0
        return sum(p.size() for p in self._queues_map.values())

    def update_highwater(self, topic: str, partition: int, highwater: int):
        queue = self._queues_map.get((topic, partition))
        if queue is not None:
            queue.set_highwater(highwater=highwater)

    def add_partition(self, topic: str, partition: int):
        tp = (topic, partition)
        if tp in self._queues_map:
            return
        # New partition, need to create a new buffer and add it to the heap
        partition_buffer = PartitionQueue(
            topic=topic, partition=partition, max_size=self._max_partition_queue_size
        )
        self._queues_list.append(partition_buffer)
        self._queues_map[tp] = partition_buffer

    def drop_partition(self, topic: str, partition: int):
        queue = self._queues_map.pop((topic, partition), None)
        if queue is not None:
            self._queues_list.remove(queue)

    def add_item(self, topic: str, partition: int, item: Row) -> bool:
        partition_buffer = self._queues_map[(topic, partition)]
        partition_buffer.append(item)
        return partition_buffer.full()

    def ready(self) -> bool:
        return self._queues_list and all(
            q.size() or q.idle() for q in self._queues_list
        )

    def pop_item(self) -> Optional[Tuple[Row, bool]]:
        """
        Get an item from the queue with the smallest priority and return it.

        """
        heap = self._queues_list
        if not len(heap):
            # The buffer has no partitions yet
            return

        buffer = min(self._queues_list, key=lambda q: q.priority)
        if buffer.priority == _MAX_FLOAT:
            # The partition with the smallest priority is empty
            return

        return buffer.popleft(), buffer.empty()

    def read(self) -> Iterator[Tuple[Row, bool]]:
        while True:
            result = self.pop_item()
            if result is None:
                break
            yield result
