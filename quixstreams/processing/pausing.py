import logging
import sys
import time

from confluent_kafka import TopicPartition

from quixstreams.kafka import BaseConsumer
from quixstreams.models import TopicManager

logger = logging.getLogger(__name__)

_MAX_FLOAT = sys.float_info.max


class PausingManager:
    """
    A class to temporarily pause topic partitions and resume them after
    the timeout is elapsed.
    """

    _resume_at: float

    def __init__(self, consumer: BaseConsumer, topic_manager: TopicManager):
        self._consumer = consumer
        self._topic_manager = topic_manager
        self.reset()

    def pause(
        self,
        offsets_to_seek: dict[tuple[str, int], int],
        resume_after: float,
    ):
        """
        Pause all partitions for the certain period of time and seek the partitions
        provided in the `offsets_to_seek` dict.

        This method is supposed to be called in case of backpressure from Sinks.
        """
        resume_at = time.monotonic() + resume_after
        self._resume_at = min(self._resume_at, resume_at)

        # Pause only data TPs excluding changelog TPs
        non_changelog_tps = self._get_non_changelog_assigned_tps()

        for tp in non_changelog_tps:
            position, *_ = self._consumer.position([tp])
            logger.debug(
                f'Pausing topic partition "{tp.topic}[{tp.partition}]" for {resume_after}s; '
                f"position={position.offset}"
            )
            self._consumer.pause(partitions=[tp])
            # Seek the TP back to the "offset_to_seek" to start from it on resume.
            # The "offset_to_seek" is provided by the Checkpoint and is expected to be the
            # first offset processed in the checkpoint.
            seek_offset = offsets_to_seek.get((tp.topic, tp.partition))
            if seek_offset is not None:
                logger.debug(
                    f'Seek the paused partition "{tp.topic}[{tp.partition}]" back to '
                    f"offset {seek_offset}"
                )
                self._consumer.seek(
                    partition=TopicPartition(
                        topic=tp.topic, partition=tp.partition, offset=seek_offset
                    )
                )

    def resume_if_ready(self):
        """
        Resume consuming from assigned data partitions after the wait period has elapsed.
        """
        if self._resume_at > time.monotonic():
            return

        # Resume only data TPs excluding changelog TPs
        non_changelog_tps = self._get_non_changelog_assigned_tps()

        for tp in non_changelog_tps:
            logger.debug(f'Resuming topic partition "{tp.topic}[{tp.partition}]"')
            self._consumer.resume(
                partitions=[TopicPartition(topic=tp.topic, partition=tp.partition)]
            )
        self.reset()

    def reset(self):
        # Reset the timeout back to its initial state
        self._resume_at = _MAX_FLOAT

    def _get_non_changelog_assigned_tps(self) -> list[TopicPartition]:
        """
        Get assigned topic partitions for non-changelog topics.
        """
        return [
            tp
            for tp in self._consumer.assignment()
            if tp.topic in self._topic_manager.non_changelog_topics
        ]
