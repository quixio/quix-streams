import logging
import time
from collections.abc import Mapping
from typing import Any, Optional

from .context import message_context
from .core.stream import VoidExecutor
from .models import Headers

__all__ = ("RunTracker", "RunCollector")

logger = logging.getLogger(__name__)


class RunCollector:
    """
    A simple sink to accumulate the outputs during the application run.
    """

    def __init__(self):
        self._items: list[dict] = []
        self._count: int = 0

    def add_value_and_metadata(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        headers: Headers,
        topic: str,
        partition: int,
        offset: int,
    ):
        if not isinstance(value, Mapping):
            value = {"_value": value}
        self._items.append(
            {
                "_key": key,
                "_timestamp": timestamp_ms,
                "_headers": headers,
                "_topic": topic,
                "_partition": partition,
                "_offset": offset,
                **value,
            }
        )
        self._count += 1

    def add_value(self, value: Any):
        if not isinstance(value, Mapping):
            value = {"_value": value}
        self._items.append(value)
        self._count += 1

    def increment_count(self):
        self._count += 1

    @property
    def items(self) -> list[dict]:
        return self._items

    @property
    def count(self) -> int:
        return self._count


class RunTracker:
    running: bool
    _has_stop_condition: bool
    _message_consumed: bool
    _collector: RunCollector

    # timeout-specific attrs
    _timeout: float
    _timeout_start_time: float

    # count-specific attrs
    _max_count: int

    def __init__(self):
        """
        Tracks the runtime status of an Application, along with managing variables
          associated with stopping the app based on a timeout or count.

        Though intended for debugging, it is designed to minimize impact on
          normal Application operation.
        """
        # Sets the resettable attributes, avoiding defining the same values twice.
        self.reset()

    @property
    def collected(self) -> list[dict]:
        """
        Get a list of results accumulated during the run
        """
        return self._collector.items

    def collect_values_and_metadata(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: Any,
    ):
        ctx = message_context()
        self._collector.add_value_and_metadata(
            key=key,
            value=value,
            timestamp_ms=timestamp,
            headers=headers,
            offset=ctx.offset,
            partition=ctx.partition,
            topic=ctx.topic,
        )

    def collect_values(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: Any,
    ):
        self._collector.add_value(value=value)

    def increment_count(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: Any,
    ):
        self._collector.increment_count()

    def stop(self):
        """
        Called when Application is stopped, or self._stop_checker condition is met.
        """
        self.running = False

    def reset(self):
        """
        Resets all values required for re-running.
        """
        self.running = False
        self._collector = RunCollector()
        self._has_stop_condition = False
        self._timeout = 0.0
        self._timeout_start_time = 0.0
        self._message_consumed = False
        self._max_count = 0

    def update_status(self):
        """
        Trigger stop if any stop conditions are met.
        """
        if self._has_stop_condition and (self._at_timeout() or self._at_count()):
            self.stop()

    def set_as_running(self):
        """
        Called as part of Application.run() to initialize self.running.
        """
        self.running = True
        if self._timeout:
            # give an additional 60s buffer for initial assignment to trigger
            self._timeout_start_time = time.monotonic() + 60

    def set_message_consumed(self, consumed: bool):
        """
        Sets the current message topic partition (if one was consumed)
        """
        self._message_consumed = consumed

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

    def get_collector(self, collect: bool, metadata: bool) -> Optional[VoidExecutor]:
        if not self._has_stop_condition:
            # Skip collecting when the app doesn't have the "count" stop condition.
            return None
        elif not collect:
            # There's a "count" stop condition, but "collect" is False.
            return self.increment_count
        elif not metadata:
            # Collect values only
            return self.collect_values
        else:
            # Collect values and metadata
            return self.collect_values_and_metadata

    def _at_count(self) -> bool:
        if self._max_count and self._collector.count >= self._max_count:
            logger.info(f"Count of {self._max_count} records reached.")
            return True
        return False

    def _at_timeout(self) -> bool:
        if not self._timeout:
            return False
        if self._message_consumed:
            # refresh when any message is consumed
            self.timeout_refresh()
        elif (time.monotonic() - self._timeout_start_time) >= self._timeout:
            logger.info(f"Timeout of {self._timeout}s reached.")
            return True
        return False
