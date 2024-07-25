import abc
import logging
from typing import Any, Dict, List, Tuple

from quixstreams.models import HeaderValue
from quixstreams.sinks.base.batch import SinkBatch

logger = logging.getLogger(__name__)


class Sink(abc.ABC):
    _batches: Dict[Tuple[str, int], SinkBatch]

    def __init__(self):
        self._batches = {}

    def __repr__(self):
        return f"<Sink: {self.__class__.__name__}>"

    @abc.abstractmethod
    def write(self, batch: SinkBatch): ...

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: List[Tuple[str, HeaderValue]],
        topic: str,
        partition: int,
        offset: int,
    ):
        tp = (topic, partition)
        batch = self._batches.get(tp)
        if batch is None:
            batch = SinkBatch(topic=topic, partition=partition)
            self._batches[tp] = batch
        batch.append(
            value=value, key=key, timestamp=timestamp, headers=headers, offset=offset
        )

    def flush(self, topic: str, partition: int):
        batch = self._batches.get((topic, partition))
        if batch is not None:
            logger.debug(
                f'Flushing sink "{self}" for partition "{topic}[{partition}]; '
                f'total_records={batch.size}"'
            )
            # TODO: Some custom error handling may be needed here
            #   For now simply fail
            try:
                self.write(batch)
            finally:
                # Always drop the batch after flushing it
                self.drop_batch(topic=topic, partition=partition)

    def drop_batch(self, topic: str, partition: int):
        self._batches.pop((topic, partition), None)
