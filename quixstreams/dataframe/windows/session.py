import logging
import time
from typing import TYPE_CHECKING, Any, Iterable, Optional

from quixstreams.state import WindowedPartitionTransaction, WindowedState

from .base import (
    MultiAggregationWindowMixin,
    SingleAggregationWindowMixin,
    WindowKeyResult,
    WindowOnLateCallback,
)
from .time_based import ClosingStrategy, TimeWindow

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

logger = logging.getLogger(__name__)


class SessionWindow(TimeWindow):
    """
    Session window groups events that occur within a specified timeout period.

    A session starts with the first event and extends each time a new event arrives
    within the timeout period. The session closes after the timeout period with no
    new events.

    Each session window can have different start and end times based on the actual
    events, making sessions dynamic rather than fixed-time intervals.
    """

    def __init__(
        self,
        timeout_ms: int,
        grace_ms: int,
        name: str,
        dataframe: "StreamingDataFrame",
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(name=name, dataframe=dataframe, on_late=on_late)

        self._timeout_ms = timeout_ms
        self._grace_ms = grace_ms

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        transaction: WindowedPartitionTransaction,
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        state = transaction.as_state(prefix=key)
        timeout_ms = self._timeout_ms
        grace_ms = self._grace_ms

        collect = self.collect
        aggregate = self.aggregate

        # Determine the latest timestamp for expiration logic
        if self._closing_strategy == ClosingStrategy.PARTITION:
            latest_expired_timestamp = transaction.get_latest_expired(prefix=b"")
            latest_timestamp = max(timestamp_ms, latest_expired_timestamp)
        else:
            state_ts = state.get_latest_timestamp() or 0
            latest_timestamp = max(timestamp_ms, state_ts)

        # Calculate session expiry threshold
        expiry_threshold = latest_timestamp - grace_ms

        # Check if the event is too late
        if timestamp_ms < expiry_threshold:
            self._on_expired_window(
                value=value,
                key=key,
                start=timestamp_ms,
                end=timestamp_ms,
                timestamp_ms=timestamp_ms,
                late_by_ms=expiry_threshold - timestamp_ms,
            )
            return [], []

        # Search for active sessions that can accommodate the new event
        for (window_start, window_end), aggregated_value, _ in state.get_windows(
            start_from_ms=max(0, timestamp_ms - timeout_ms * 2),
            start_to_ms=timestamp_ms + timeout_ms + 1,
            backwards=True,
        ):
            # Calculate the time gap between the new event and the session's last activity
            # window_end now directly represents the timestamp of the last event
            time_gap = timestamp_ms - window_end

            # Check if this session can be extended
            if time_gap <= timeout_ms + grace_ms and timestamp_ms >= window_start:
                extend_session = True
                session_start = window_start
                # Only update end time if the new event is greater than the current end time
                session_end = max(window_end, timestamp_ms)
                existing_aggregated = aggregated_value
                # Delete the old window if extending an existing session
                state.delete_window(window_start, window_end)
                break
        else:
            # If no extendable session found, start a new one
            extend_session = False
            session_start = session_end = timestamp_ms

        # Process the event for this session
        updated_windows: list[WindowKeyResult] = []

        # Add to collection if needed
        if collect:
            state.add_to_collection(value=self._collect_value(value), id=timestamp_ms)

        # Update the session window aggregation
        if aggregate:
            current_value = (
                existing_aggregated if extend_session else self._initialize_value()
            )

            aggregated = self._aggregate_value(current_value, value, timestamp_ms)
            updated_windows.append(
                (
                    key,
                    self._results(aggregated, [], session_start, session_end),
                )
            )
        else:
            aggregated = None

        state.update_window(
            session_start, session_end, value=aggregated, timestamp_ms=timestamp_ms
        )

        # Expire old sessions
        if self._closing_strategy == ClosingStrategy.PARTITION:
            expired_windows = self.expire_by_partition(
                transaction, expiry_threshold, collect
            )
        else:
            expired_windows = self.expire_by_key(key, state, expiry_threshold, collect)

        return updated_windows, expired_windows

    def expire_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        expiry_threshold: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        start = time.monotonic()
        count = 0

        # Import the parsing function to extract message keys from window keys
        from quixstreams.state.rocksdb.windowed.serialization import parse_window_key

        expired_results = []

        # Collect all keys and extract unique prefixes to avoid iteration conflicts
        all_keys = list(transaction.keys())
        seen_prefixes = set()

        for key_bytes in all_keys:
            try:
                prefix, start_ms, end_ms = parse_window_key(key_bytes)
                if prefix not in seen_prefixes:
                    seen_prefixes.add(prefix)
            except (ValueError, IndexError):
                # Skip invalid window key formats
                continue

        # Expire sessions for each unique prefix
        for prefix in seen_prefixes:
            state = transaction.as_state(prefix=prefix)
            prefix_expired = list(
                self.expire_by_key(prefix, state, expiry_threshold, collect)
            )
            expired_results.extend(prefix_expired)
            count += len(prefix_expired)

        if count:
            logger.debug(
                "Expired %s session windows in %ss",
                count,
                round(time.monotonic() - start, 2),
            )

        return expired_results

    def expire_by_key(
        self,
        key: Any,
        state: WindowedState,
        expiry_threshold: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        start = time.monotonic()
        count = 0

        # Get all windows and check which ones have expired
        all_windows = list(
            state.get_windows(0, expiry_threshold + self._timeout_ms, backwards=False)
        )

        windows_to_delete = []
        for (window_start, window_end), aggregated, _ in all_windows:
            # Session expires when the session end time + timeout has passed the expiry threshold
            # window_end directly represents the timestamp of the last event
            if window_end + self._timeout_ms <= expiry_threshold:
                collected = []
                if collect:
                    # window_end is now the timestamp of the last event, so we need +1 to include it
                    collected = state.get_from_collection(window_start, window_end + 1)

                windows_to_delete.append((window_start, window_end))
                count += 1
                yield (
                    key,
                    self._results(aggregated, collected, window_start, window_end),
                )

        # Clean up expired windows
        for window_start, window_end in windows_to_delete:
            state.delete_window(window_start, window_end)
            if collect:
                state.delete_from_collection(window_end, start=window_start)

        if count:
            logger.debug(
                "Expired %s session windows in %ss",
                count,
                round(time.monotonic() - start, 2),
            )


class SessionWindowSingleAggregation(SingleAggregationWindowMixin, SessionWindow):
    pass


class SessionWindowMultiAggregation(MultiAggregationWindowMixin, SessionWindow):
    pass
