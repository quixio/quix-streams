import dataclasses
import logging
import time
from typing import Optional

from quixstreams.checkpointing import Checkpoint
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.exceptions import QuixException
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.internal_producer import InternalProducer
from quixstreams.sinks import SinkManager
from quixstreams.state import StateStoreManager
from quixstreams.utils.printing import Printer

__all__ = ("ProcessingContext",)

logger = logging.getLogger(__name__)


class CheckpointNotInitialized(QuixException): ...


@dataclasses.dataclass
class ProcessingContext:
    """
    A class to share processing-related objects
    between `Application` and `StreamingDataFrame` instances.
    """

    commit_interval: float
    producer: InternalProducer
    consumer: InternalConsumer
    state_manager: StateStoreManager
    sink_manager: SinkManager
    dataframe_registry: DataFrameRegistry
    commit_every: int = 0
    exactly_once: bool = False
    printer: Printer = Printer()

    _checkpoint: Optional[Checkpoint] = dataclasses.field(
        init=False, repr=False, default=None
    )

    @property
    def checkpoint(self) -> Checkpoint:
        if self._checkpoint is None:
            raise CheckpointNotInitialized("Checkpoint has not been initialized yet")
        return self._checkpoint

    def store_offset(self, topic: str, partition: int, offset: int):
        """
        Store the offset of the processed message to the checkpoint.

        :param topic: topic name
        :param partition: partition number
        :param offset: message offset
        """
        self.checkpoint.store_offset(topic=topic, partition=partition, offset=offset)

    def init_checkpoint(self):
        """
        Initialize a new checkpoint
        """
        self._checkpoint = Checkpoint(
            commit_interval=self.commit_interval,
            commit_every=self.commit_every,
            state_manager=self.state_manager,
            producer=self.producer,
            consumer=self.consumer,
            sink_manager=self.sink_manager,
            dataframe_registry=self.dataframe_registry,
            exactly_once=self.exactly_once,
        )

    def commit_checkpoint(self, force: bool = False):
        """
        Attempts finalizing the current Checkpoint only if the Checkpoint is "expired",
        or `force=True` is passed, otherwise do nothing.

        To finalize: the Checkpoint will be committed if it has any stored offsets,
        else just close it. A new Checkpoint is then created.

        :param force: if `True`, commit the Checkpoint before its expiration deadline.
        """
        if self.checkpoint.expired() or force:
            if self.checkpoint.empty():
                self.checkpoint.close()
            else:
                logger.debug(f"Committing a checkpoint; forced={force}")
                start = time.monotonic()
                self.checkpoint.commit()
                elapsed = round(time.monotonic() - start, 2)
                logger.debug(
                    f"Committed a checkpoint; forced={force}, time_elapsed={elapsed}s"
                )
            self.init_checkpoint()

    def __enter__(self):
        self.sink_manager.start_sinks()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.exactly_once:
            self.producer.abort_transaction(5)
        self.printer.clear()
