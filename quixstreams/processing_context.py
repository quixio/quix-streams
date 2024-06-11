import dataclasses
import logging
import time
from typing import Optional

from quixstreams.checkpointing import Checkpoint
from quixstreams.exceptions import QuixException
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer
from quixstreams.state import StateStoreManager

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
    producer: RowProducer
    consumer: RowConsumer
    state_manager: StateStoreManager
    exactly_once: bool = False
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
        self._checkpoint.store_offset(topic=topic, partition=partition, offset=offset)

    def init_checkpoint(self):
        """
        Initialize a new checkpoint
        """
        logger.debug(f"Starting a checkpoint...")
        self._checkpoint = Checkpoint(
            commit_interval=self.commit_interval,
            state_manager=self.state_manager,
            producer=self.producer,
            consumer=self.consumer,
            exactly_once=self.exactly_once,
        )

    def commit_checkpoint(self, force: bool = False):
        """
        Commit the current checkpoint.

        The actual commit will happen only when:

        1. The checkpoint has at least one stored offset
        2. The checkpoint is expired or `force=True` is passed

        :param force: if `True`, commit the checkpoint before its expiration deadline.
        """
        if self._checkpoint.expired() or force:
            logger.debug(f"Attempting checkpoint commit; forced={force}")
            start = time.monotonic()
            self._checkpoint.commit()
            elapsed = round(time.monotonic() - start, 2)
            logger.debug(
                f"Committed a checkpoint; forced={force}, time_elapsed={elapsed}s"
            )
            self.init_checkpoint()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.exactly_once:
            self.producer.abort_transaction(5)
