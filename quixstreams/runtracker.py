import logging
import time
from typing import (
    Callable,
    Iterator,
    Optional,
)

from confluent_kafka import TopicPartition

from .models import TopicManager
from .processing import ProcessingContext

__all__ = ("RunTracker",)

logger = logging.getLogger(__name__)


class RunTracker:
    __slots__ = (
        "_is_first_run",
        "_processing_context",
        "running",
        "_stop_checker",
        "_has_stop_checker",
        "_needs_assigning",
        "_is_recovering",
        "_timeout",
        "_start_time",
        "_topic_manager",
        "_primary_topics",
        "_repartition_topics",
        "last_consumed_tp",
        "_max_count",
        "_count",
        "_repartition_stop_points",
    )

    # Here we establish all attrs that will be reset every `Application.run()` call
    # This avoids various IDE and mypy complaints around not setting them in init.

    running: bool
    _stop_checker: Optional[Callable[[], bool]]
    _has_stop_checker: bool

    # timeout-specific attrs
    _start_time: float
    _timeout: float
    _needs_assigning: bool
    _is_recovering: bool

    # count-specific attrs
    last_consumed_tp: Optional[tuple]
    _max_count: int
    _count: int
    _repartition_stop_points: dict[tuple, int]

    def __init__(
        self,
        processing_context: ProcessingContext,
        topic_manager: TopicManager,
    ):
        """
        Tracks the runtime status of an Application, along with managing variables
          associated with stopping the app based on a timeout or count.

        Though intended for debugging, it is designed to minimize impact on
        normal Application operation.
        """
        self._is_first_run: bool = True
        self._processing_context = processing_context

        # count specific attrs; 1 time setup
        self._topic_manager = topic_manager
        self._primary_topics: list[str] = []
        self._repartition_topics: list[str] = []

        # Sets the resettable attributes, avoiding defining the same values twice.
        self.stop_and_reset()

    @property
    def _consumer(self):
        return self._processing_context.consumer

    @property
    def is_first_run(self) -> bool:
        return self._is_first_run

    def update_status(self):
        """
        Trigger stop if any conditions are met.
        This is optimized for maximum performance for when there is no stop_checker.
        """
        if self._has_stop_checker:
            if self._stop_checker():
                self.stop_and_reset()

    def start_runner(self):
        """
        Called as part of Application.run(), mostly to initialize self.running.
        """
        self.running = True
        if self._is_first_run:
            self._primary_topics = [t for t in self._topic_manager.topics]
            self._repartition_topics = [
                t for t in self._topic_manager.repartition_topics
            ]
            self._is_first_run = False

    def stop_and_reset(self):
        """
        Called when Application is stopped, or self._stop_checker condition is met.
        Resets all values required for re-running.
        """
        self.running = False
        self._stop_checker = None
        self._has_stop_checker = False

        self._timeout = 0.0
        self._start_time = 0.0
        self._needs_assigning = True
        self._is_recovering = False

        self.last_consumed_tp = None
        self._max_count = 0
        self._count = 0
        self._repartition_stop_points = {}

    def handle_rebalance(self, recovery_required: bool):
        """
        This is the most common way the timeout start time will be set.
        """
        if self._timeout and self._needs_assigning:
            self._needs_assigning = False
            self._is_recovering = recovery_required
            if not recovery_required:
                self._set_timeout_start_time()

    def handle_recovery(self):
        """
        Sets the timeout start time after a recovery occurs.
        """
        if self._timeout:
            self._is_recovering = False
            self._set_timeout_start_time()

    def set_stop_condition(
        self,
        timeout: float = 0.0,
        count: int = 0,
    ):
        """
        Called as part of app.run(); this handles the users optional stop conditions.
        """
        if not (timeout or count):
            logger.info("APP RUN: preparing to run until errors or manually stopped...")
            return

        logger.info("APP RUN: preparing to run with stop conditions...")
        if timeout:
            if timeout < 0.0:
                raise ValueError("run timeout must be >= 0.0")
            logger.info(
                f"APP RUN: timeout detected; "
                f"Application will run for up to {timeout}s "
                f"(after initial rebalance or recovery)"
            )
            self._stop_checker = self._at_timeout_func()
            self._timeout = timeout

        if count:
            if count < 0:
                raise ValueError("run count must be >= 0")
            logger.info(
                f"APP RUN: count limit detected; "
                f"Application will process up to {count} records"
            )
            self._stop_checker = self._at_count_func()
            self._max_count = count

        if self._timeout and self._max_count:
            logger.info("APP RUN: Application will stop at first satisfied limiter!")
            self._stop_checker = self._at_count_or_timeout_func()
        self._has_stop_checker = True

    def _set_timeout_start_time(self):
        """
        There are three options for calling this, but it will only be set once.

        1. If waiting for first message, it's set during poll loop (`_at_timeout_func`)
        2. If recovery is required, it's set after recovering (`handle_recovery`)
        3. Else, it's set immediately after rebalance (`handle_rebalance`)
        """
        logger.info(f"Starting time tracking with {self._timeout}s timeout")
        self._start_time = time.monotonic()

    def _at_timeout(self) -> bool:
        if (time.monotonic() - self._start_time) > self._timeout:
            logger.info(f"Timeout of {self._timeout}s reached!")
            return True
        return False

    def _at_count(self) -> bool:
        if self.last_consumed_tp:
            # add to count only if message is from a non-repartition topic
            if self.last_consumed_tp[0] in self._primary_topics:
                self._count += 1
                if (self._max_count - self._count) <= 0:
                    logger.info(f"Count of {self._max_count} records reached!")
                    return True
        return False

    def _prepare_repartition_check(self):
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

    def _repartitions_finished(self) -> bool:
        """
        Confirms repartitions are at their watermarks as those messages are consumed.
        Empty consumer polls will cause a check on all of them.
        """
        if self.last_consumed_tp:
            tps = [self.last_consumed_tp]
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
        finished_repartition_processing = self._repartitions_finished

        def at_count_gen() -> Iterator[bool]:
            while not at_count():
                yield False
            # Count was met for primary topics, now confirm downstream repartitions
            if self._repartition_topics:
                self._prepare_repartition_check()
                yield False  # poll for a new message before continuing
                while not finished_repartition_processing():
                    yield False
                logger.info("All downstream internal topics processed with counting!")
            yield True

        gen = at_count_gen()
        return lambda: next(gen)

    def _at_timeout_func(self) -> Callable[[], bool]:
        """
        This is handled as a generator to minimize continuous superfluous conditional
        checks or having to adjust the underlying _stop_checker during runtime.
        """
        at_timeout = self._at_timeout

        def at_timeout_gen() -> Iterator[bool]:
            while self._needs_assigning or self._is_recovering:
                yield False
            while not at_timeout():
                yield False
            yield True

        gen = at_timeout_gen()
        return lambda: next(gen)

    def _at_count_or_timeout_func(self) -> Callable[[], bool]:
        at_timeout = self._at_timeout_func()
        at_count = self._at_count_func()
        return lambda: at_timeout() or at_count()
