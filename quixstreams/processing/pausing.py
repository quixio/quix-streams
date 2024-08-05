import logging
import sys
import time
from typing import Tuple, Dict

from confluent_kafka import TopicPartition

from quixstreams.kafka import Consumer

logger = logging.getLogger(__name__)

_MAX_FLOAT = sys.float_info.max


class PausingManager:
    """
    A class to temporarily pause topic partitions and resume them after
    the timeout is elapsed.
    """

    _paused_tps: Dict[Tuple[str, int], float]

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._paused_tps = {}
        self._next_resume_at = _MAX_FLOAT

    def pause(
        self,
        topic: str,
        partition: int,
        offset_to_seek: int,
        resume_after: float,
    ):
        """
        Pause the topic-partition for a certain period of time.

        This method is supposed to be called in case of backpressure from Sinks.
        """
        if self.is_paused(topic=topic, partition=partition):
            # Exit early if the TP is already paused
            return

        # Add a TP to the dict to avoid repetitive pausing
        resume_at = time.monotonic() + resume_after
        self._paused_tps[(topic, partition)] = resume_at
        # Remember when the next TP should be resumed to exit early
        # in the resume_if_ready() calls.
        # Partitions are rarely paused, but the resume checks can be done
        # thousands times a sec.
        self._next_resume_at = min(self._next_resume_at, resume_at)
        tp = TopicPartition(topic=topic, partition=partition, offset=offset_to_seek)
        position, *_ = self._consumer.position([tp])
        logger.debug(
            f'Pausing topic partition "{topic}[{partition}]" for {resume_after}s; '
            f"current_offset={position.offset}"
        )
        self._consumer.pause(partitions=[tp])
        # Seek the TP back to the "offset_to_seek" to start from it on resume.
        # The "offset_to_seek" is provided by the Checkpoint and is expected to be the
        # first offset processed in the checkpoint.
        logger.debug(
            f'Seek the paused partition "{topic}[{partition}]" back to '
            f"offset {tp.offset}"
        )
        self._consumer.seek(partition=tp)

    def is_paused(self, topic: str, partition: int) -> bool:
        """
        Check if the topic-partition is already paused
        """
        return (topic, partition) in self._paused_tps

    def resume_if_ready(self):
        """
        Resume consuming from topic-partitions after the wait period has elapsed.
        """
        now = time.monotonic()
        if self._next_resume_at > now:
            # Nothing to resume yet, exit early
            return

        tps_to_resume = [
            tp for tp, resume_at in self._paused_tps.items() if resume_at <= now
        ]
        for topic, partition in tps_to_resume:
            logger.debug(f'Resuming topic partition "{topic}[{partition}]"')
            self._consumer.resume(
                partitions=[TopicPartition(topic=topic, partition=partition)]
            )
            self._paused_tps.pop((topic, partition))
        self._reset_next_resume_at()

    def revoke(self, topic: str, partition: int):
        """
        Remove partition from the list of paused TPs if it's revoked
        """
        tp = (topic, partition)
        if tp not in self._paused_tps:
            return
        self._paused_tps.pop(tp)
        self._reset_next_resume_at()

    def _reset_next_resume_at(self):
        if self._paused_tps:
            self._next_resume_at = min(self._paused_tps.values())
        else:
            self._next_resume_at = _MAX_FLOAT
