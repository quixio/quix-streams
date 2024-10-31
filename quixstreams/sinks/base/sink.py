import abc
import logging
from typing import Any, Dict, List, Tuple

from quixstreams.models import HeaderValue
from quixstreams.sinks.base.batch import SinkBatch

logger = logging.getLogger(__name__)


class BaseSink(abc.ABC):
    """
    This is a base class for all sinks.

    Subclass it and implement its methods to create your own sink.

    Note that Sinks are currently in beta, and their design may change over time.
    """

    @abc.abstractmethod
    def flush(self, topic: str, partition: int):
        """
        This method is triggered by the Checkpoint class when it commits.

        You can use `flush()` to write the batched data to the destination (in case of
        a batching sink), or confirm the delivery of the previously sent messages
        (in case of a streaming sink).

        If flush() fails, the checkpoint will be aborted.
        """

    @abc.abstractmethod
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
        """
        This method is triggered on every new processed record being sent to this sink.

        You can use it to accumulate batches of data before sending them outside, or
        to send results right away in a streaming manner and confirm a delivery later
        on flush().
        """

    def on_paused(self, topic: str, partition: int):
        """
        This method is triggered when the sink is paused due to backpressure, when
        the `SinkBackpressureError` is raised.

        Here you can react to the backpressure events.
        """


class BatchingSink(BaseSink):
    """
    A base class for batching sinks, that need to accumulate the data first before
    sending it to the external destinatios.

    Examples: databases, objects stores, and other destinations where
    writing every message is not optimal.

    It automatically handles batching, keeping batches in memory per topic-partition.

    You may subclass it and override the `write()` method to implement a custom
    batching sink.
    """

    _batches: Dict[Tuple[str, int], SinkBatch]

    def __init__(self):
        self._batches = {}

    def __repr__(self):
        return f"<BatchingSink: {self.__class__.__name__}>"

    @abc.abstractmethod
    def write(self, batch: SinkBatch):
        """
        This method implements actual writing to the external destination.

        It may also raise `SinkBackpressureError` if the destination cannot accept new
        writes at the moment.
        When this happens, the accumulated batch is dropped and the app pauses the
        corresponding topic partition.
        """

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
        """
        Add a new record to in-memory batch.
        """
        tp = (topic, partition)
        batch = self._batches.get(tp)
        if batch is None:
            batch = SinkBatch(topic=topic, partition=partition)
            self._batches[tp] = batch
        batch.append(
            value=value, key=key, timestamp=timestamp, headers=headers, offset=offset
        )

    def flush(self, topic: str, partition: int):
        """
        Flush an accumulated batch to the destination and drop it afterward.
        """
        batch = self._batches.get((topic, partition))
        if batch is None:
            return

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
            self._batches.pop((topic, partition), None)

    def on_paused(self, topic: str, partition: int):
        """
        When the destination is already backpressure, drop the accumulated batch.
        """
        self._batches.pop((topic, partition), None)
