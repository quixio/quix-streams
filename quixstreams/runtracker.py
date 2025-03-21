import logging
import time
from typing import Optional

from confluent_kafka import TopicPartition

from .processing import ProcessingContext

__all__ = ("RunTracker",)

logger = logging.getLogger(__name__)


class RunTracker:
    __slots__ = (
        "running",
        "_has_stop_condition",
        "_processing_context",
        "_stop_checker",
        "_timeout",
        "_timeout_start_time",
        "_primary_topics",
        "_repartition_topics",
        "_current_message_tp",
        "_max_count",
        "_current_count",
        "_repartition_stop_points",
    )

    # Here we establish all attrs that will be reset every `Application.run()` call
    # This avoids various IDE and mypy complaints around not setting them in init.

    running: bool
    _has_stop_condition: bool
    _current_message_tp: Optional[tuple[str, int]]

    # timeout-specific attrs
    _timeout: float
    _timeout_start_time: float

    # count-specific attrs
    _max_count: int
    _current_count: int
    _primary_topics: list[str]
    _repartition_topics: list[str]
    _repartition_stop_points: dict[tuple, int]

    def __init__(
        self,
        processing_context: ProcessingContext,
    ):
        """
        Tracks the runtime status of an Application, along with managing variables
          associated with stopping the app based on a timeout or count.

        Though intended for debugging, it is designed to minimize impact on
          normal Application operation.
        """
        self._processing_context = processing_context

        # Sets the resettable attributes, avoiding defining the same values twice.
        self.stop_and_reset()

    @property
    def _consumer(self):
        return self._processing_context.consumer

    def stop_and_reset(self):
        """
        Called when Application is stopped, or self._stop_checker condition is met.
        Resets all values required for re-running.
        """
        self.running = False
        self._has_stop_condition = False

        self._timeout = 0.0
        self._timeout_start_time = 0.0

        self._current_message_tp = None
        self._max_count = 0
        self._current_count = 0
        self._primary_topics = []
        self._repartition_topics = []
        self._repartition_stop_points = {}

    def update_status(self):
        """
        Trigger stop if any stop conditions are met.
        """
        if self._has_stop_condition:
            if self._at_timeout() or self._at_count():
                self.stop_and_reset()

    def set_topics(self, primary: list[str], repartition: list[str]):
        """
        Sets the primary and repartition topic lists for counting.
        """
        self._primary_topics = primary
        self._repartition_topics = repartition

    def set_as_running(self):
        """
        Called as part of Application.run() to initialize self.running.
        """
        self.running = True
        if self._timeout:
            # give an additional 60s buffer for initial assignment to trigger
            self._timeout_start_time = time.monotonic() + 60

    def set_current_message_tp(self, tp: Optional[tuple[str, int]]):
        """
        Sets the current message topic partition (if one was consumed)
        """
        self._current_message_tp = tp

    def timeout_refresh(self):
        """
        Timeout is refreshed when:
        - Rebalance completes
        - Recovery completes
        - Any message is consumed (reset during timeout check)
        """
        self._timeout_start_time = time.monotonic()

    def set_stop_condition(
        self,
        timeout: float = 0.0,
        count: int = 0,
    ):
        """
        Called as part of app.run(); this handles the users optional stop conditions.
        """
        if not ((timeout := max(timeout, 0.0)) or (count := max(count, 0))):
            return

        self._has_stop_condition = True
        self._timeout = timeout
        self._max_count = count

        time_stop_log = f"timeout={timeout} seconds" if timeout else ""
        count_stop_log = f"count={count} records" if count else ""
        logger.info(
            "APP STOP CONDITIONS SET: "
            f"{time_stop_log}{' OR ' if (timeout and count) else ''}{count_stop_log}"
        )

    def _at_count(self) -> bool:
        if not self._max_count:
            return False
        if self._max_count == self._current_count:
            return self._count_repartitions_finished()
        if (tp := self._current_message_tp) and tp[0] in self._primary_topics:
            self._current_count += 1
            if self._max_count == self._current_count:
                logger.info(f"Count of {self._max_count} records reached.")
                if not self._repartition_topics:
                    return True
                self._count_prepare_repartition_check()
        return False

    def _count_prepare_repartition_check(self):
        """
        Stores repartition watermarks for efficient validation.
        Commits checkpoint and pauses primary topics to ensure watermark accuracy.
        """
        logger.info("Finalizing any repartition topic processing...")
        self._processing_context.commit_checkpoint(force=True)
        self._consumer.pause(
            [t for t in self._consumer.assignment() if t.topic in self._primary_topics]
        )
        topic_partitions = [
            tp
            for tp in self._consumer.assignment()
            if tp.topic in self._repartition_topics
        ]
        self._repartition_stop_points = {
            (tp.topic, tp.partition): self._consumer.get_watermark_offsets(tp)[1]
            for tp in topic_partitions
        }

    def _count_repartitions_finished(self) -> bool:
        """
        Confirms repartitions are at their watermarks as those messages are consumed.
        Empty consumer polls will cause a check on all of them.
        """
        if self._current_message_tp:
            tps = [self._current_message_tp]
        else:
            tps = list(self._repartition_stop_points.keys())
        for tp in tps:
            current_offset = self._consumer.position([TopicPartition(*tp)])[0].offset
            if current_offset >= self._repartition_stop_points[tp]:
                self._repartition_stop_points.pop(tp)
        if not self._repartition_stop_points:
            logger.info("All downstream repartition topics processed with counting.")
            return True
        return False

    def _at_timeout(self) -> bool:
        if not self._timeout:
            return False
        if self._current_message_tp:
            # refresh when any message is consumed
            self.timeout_refresh()
        elif (time.monotonic() - self._timeout_start_time) >= self._timeout:
            logger.info(f"Timeout of {self._timeout}s reached.")
            return True
        return False
