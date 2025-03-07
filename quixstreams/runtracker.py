import logging
import time
from typing import (
    Callable,
    Iterator,
    Optional,
)

from confluent_kafka import TopicPartition

from .processing import ProcessingContext

__all__ = ("RunTracker",)

logger = logging.getLogger(__name__)


class RunTracker:
    __slots__ = (
        "running",
        "_processing_context",
        "_stop_checker",
        "_timeout",
        "_timeout_start_time",
        "_timeout_wait_buffer",
        "_timeout_needs_start_time_refresh",
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
    _stop_checker: Optional[Callable[[], bool]]

    # timeout-specific attrs
    _timeout: float
    _timeout_start_time: float
    _timeout_needs_start_time_refresh: bool
    _timeout_wait_buffer: float

    # count-specific attrs
    _current_message_tp: Optional[tuple[str, int]]
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
        self._stop_checker = None

        self._timeout = 0.0
        self._timeout_start_time = 0.0
        self._timeout_needs_start_time_refresh = False
        self._timeout_wait_buffer = 5.0

        self._current_message_tp = None
        self._max_count = 0
        self._current_count = 0
        self._primary_topics = []
        self._repartition_topics = []
        self._repartition_stop_points = {}

    def update_status(self):
        """
        Trigger stop if any stop conditions are met.
        This is optimized for maximum performance for when there is no stop_checker.
        """
        if self._stop_checker is None:
            return False
        if self._stop_checker():
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

    def set_current_message_tp(self, tp: Optional[tuple[str, int]]):
        """
        Sets the current message topic partition (if one was consumed)
        """
        self._current_message_tp = tp

    def set_timeout_start_time(self):
        """
        _timeout_start_time will be set from this once per app.run() call.

        It's set either when:

        1. the timeout_wait_buffer threshold is reached, or
        2. the first non-recovery message is consumed
        """
        if self._timeout_needs_start_time_refresh:
            logger.info(f"Starting time tracking with {self._timeout}s timeout")
            self._timeout_start_time = time.monotonic()
            self._timeout_needs_start_time_refresh = False

    def set_stop_condition(
        self,
        timeout: float = 0.0,
        count: int = 0,
        timeout_wait_buffer: float = 5.0,
    ):
        """
        Called as part of app.run(); this handles the users optional stop conditions.
        """
        if not ((timeout := max(timeout, 0.0)) or (count := max(count, 0))):
            return

        if timeout:
            self._timeout = timeout
            self._timeout_start_time = time.monotonic()
            self._timeout_needs_start_time_refresh = True
            self._timeout_wait_buffer = max(timeout_wait_buffer, 0.0)
            self._stop_checker = self._at_timeout_func()

        if count:
            self._max_count = count
            self._stop_checker = self._at_count_func()

        if timeout and count:
            self._stop_checker = self._at_count_or_timeout_func()

        time_stop_log = f"after running for {timeout} seconds" if timeout else ""
        count_stop_log = f"after processing {count} records" if count else ""
        logger.info(
            "APP STOP CONDITION DETECTED: Application will stop "
            f"{time_stop_log}{' OR ' if (timeout and count) else ''}{count_stop_log}"
        )

    def _at_count(self) -> bool:
        if self._current_message_tp:
            # add to count only if message is from a non-repartition topic
            if self._current_message_tp[0] in self._primary_topics:
                self._current_count += 1
                if (self._max_count - self._current_count) <= 0:
                    return True
        return False

    def _count_prepare_repartition_check(self):
        """
        Stores repartition watermarks for efficient validation.
        Commits checkpoint and pauses primary topics to ensure watermark accuracy.
        """
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
            if current_offset == self._repartition_stop_points[tp]:
                self._repartition_stop_points.pop(tp)
        return not bool(self._repartition_stop_points)

    def _at_count_func(self) -> Callable[[], bool]:
        """
        This is handled as a generator to minimize continuous superfluous conditional
        checks or having to adjust the underlying _stop_checker during runtime.
        """
        at_count = self._at_count
        finished_repartition_processing = self._count_repartitions_finished

        def at_count_gen() -> Iterator[bool]:
            while not at_count():
                yield False
            logger.info(f"Count of {self._max_count} records reached!")
            # Count was met for primary topics, now confirm downstream repartitions
            if self._repartition_topics:
                self._count_prepare_repartition_check()
                yield False  # poll for a new message before continuing
                while not finished_repartition_processing():
                    yield False
                logger.info("All downstream internal topics processed with counting!")
            yield True

        gen = at_count_gen()
        return lambda: next(gen)

    def _at_timeout(self, timeout) -> bool:
        if (time.monotonic() - self._timeout_start_time) >= timeout:
            return True
        return False

    def _at_timeout_buffer(self) -> bool:
        """
        Wait for a message for a given period of time before beginning to track timeout.
        """
        if not self._timeout_needs_start_time_refresh:
            # first message was received and start time was reset then
            return True
        if self._at_timeout(self._timeout_wait_buffer):
            logger.info(
                f"Timeout wait period of {self._timeout_wait_buffer} fully elapsed "
                f"before a message was consumed. Timeout tracking will begin now"
            )
            self.set_timeout_start_time()
            return True
        return False

    def _at_timeout_func(self) -> Callable[[], bool]:
        """
        This is handled as a generator to minimize continuous superfluous conditional
        checks or having to adjust the underlying _stop_checker during runtime.
        """
        at_timeout_buffer = self._at_timeout_buffer
        at_timeout = self._at_timeout
        timeout = self._timeout

        def at_timeout_gen() -> Iterator[bool]:
            while not at_timeout_buffer():
                yield False
            while not at_timeout(timeout):
                yield False
            logger.info(f"Timeout of {timeout}s reached!")
            yield True

        gen = at_timeout_gen()
        return lambda: next(gen)

    def _at_count_or_timeout_func(self) -> Callable[[], bool]:
        at_timeout = self._at_timeout_func()
        at_count = self._at_count_func()
        return lambda: at_timeout() or at_count()
