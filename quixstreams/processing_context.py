import dataclasses
import logging
import time
from typing import Optional, Union, Type, cast

from quixstreams.checkpointing import Checkpoint, EOSCheckpoint
from quixstreams.exceptions import QuixException
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer, TransactionalRowProducer
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
    producer: Union[RowProducer, TransactionalRowProducer]
    consumer: RowConsumer
    state_manager: StateStoreManager
    _checkpointer: Optional[Type[Checkpoint]] = dataclasses.field(init=False)
    _checkpoint: Optional[Checkpoint] = dataclasses.field(
        init=False, repr=False, default=None
    )

    def __post_init__(self):
        if isinstance(self.producer, TransactionalRowProducer):
            self._checkpointer = EOSCheckpoint
        else:
            self._checkpointer = Checkpoint

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
        self._checkpoint = self._checkpointer(
            commit_interval=self.commit_interval,
            state_manager=self.state_manager,
            producer=self.producer,
            consumer=self.consumer,
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
