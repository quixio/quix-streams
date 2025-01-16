from typing import Any, Iterable

from quixstreams.state import WindowedState

from .base import WindowResult
from .time_based import FixedTimeWindow


class SlidingWindow(FixedTimeWindow):
    def process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        """
        The algorithm is based on the concept that each message
        is associated with a left and a right window.

        Left Window:
        - Begins at message timestamp - window size
        - Ends at message timestamp

        Right Window:
        - Begins at message timestamp + 1 ms
        - Ends at message timestamp + 1 ms + window size

        For example, for a window size of 10 and a message A arriving at timestamp 26:

            0        10        20        30        40        50        60
        ----|---------|---------|---------|---------|---------|---------|--->
                                    A
        left window ->    |---------||---------|    <- right window
                          16      26  27      37

        The algorithm scans backward through the window store:
        - Starting at: start_time = message timestamp + 1 ms (the right window's start time)
        - Ending at: start_time = message timestamp - 2 * window size

        During this traversal, the algorithm performs the following actions:

        1. Determine if the right window should be created.
           If yes, locate the existing aggregation to copy to the new window.
        2. Determine if the right window of the previous record should be created.
           If yes, locate the existing aggregation and combine it with the incoming message.
        3. Locate and update the left window if it exists.
        4. If the left window does not exist, create it. Locate the existing
           aggregation and combine it with the incoming message.
        5. Locate and update all existing windows to which the new message belongs.

        Note:
            For collection aggregations (created using .collect()), the behavior is special:
            Windows are persisted with empty values (None) only to preserve their start and
            end times. The actual values are collected separately and combined during
            the window expiration step.
        """

        duration = self._duration_ms
        grace = self._grace_ms
        aggregate = self._aggregate_func
        default = self._aggregate_default
        collect = self._aggregate_collection

        # Sliding windows are inclusive on both ends, so values with
        # timestamps equal to latest_timestamp - duration - grace
        # are still eligible for processing.
        state_ts = state.get_latest_timestamp() or 0
        latest_timestamp = max(timestamp_ms, state_ts)
        max_expired_window_end = latest_timestamp - grace - 1
        max_expired_window_start = max_expired_window_end - duration
        max_deleted_window_start = max_expired_window_start - duration

        left_start = max(0, timestamp_ms - duration)
        left_end = timestamp_ms

        right_start = timestamp_ms + 1
        right_end = right_start + duration
        right_exists = False

        starts = set([left_start])
        updated_windows: list[WindowResult] = []
        iterated_windows = state.get_windows(
            # start_from_ms is exclusive, hence -1
            start_from_ms=max(0, left_start - duration) - 1,
            start_to_ms=right_start,
            # Iterating backwards makes the algorithm more efficient because
            # it starts with the rightmost windows, where existing aggregations
            # are checked. Once the aggregation for the left window is found,
            # the iteration can be terminated early.
            backwards=True,
        )

        for (start, end), (max_timestamp, aggregation) in iterated_windows:
            starts.add(start)

            if start == right_start:
                # Right window already exists; no need to create it
                right_exists = True

            elif end > left_end:
                # Create the right window if it does not exist and will not be empty
                if not right_exists and max_timestamp > timestamp_ms:
                    self._update_window(
                        state=state,
                        start=right_start,
                        end=right_end,
                        value=aggregation,
                        timestamp=timestamp_ms,
                        max_timestamp=max_timestamp,
                    )
                    right_exists = True

                # Update existing window if it is not expired
                if start > max_expired_window_start:
                    max_timestamp = max(timestamp_ms, max_timestamp)
                    window = self._update_window(
                        state=state,
                        start=start,
                        end=end,
                        value=aggregate(aggregation, value),
                        timestamp=timestamp_ms,
                        max_timestamp=max_timestamp,
                    )
                    if end == max_timestamp:  # Emit only left windows
                        updated_windows.append(window)
                else:
                    self._log_expired_window(
                        window=[start, end],
                        timestamp_ms=timestamp_ms,
                        late_by_ms=max_expired_window_end + 1 - timestamp_ms,
                    )

            elif end == left_end:
                # Create the right window for previous messages if it does not exist
                if (
                    right_start := max_timestamp + 1
                ) not in starts and max_timestamp < timestamp_ms:
                    self._update_window(
                        state=state,
                        start=right_start,
                        end=right_start + duration,
                        value=aggregate(default, value),
                        timestamp=timestamp_ms,
                        max_timestamp=timestamp_ms,
                    )

                # The left window already exists; updating it is sufficient
                # if window is not expired
                if start > max_expired_window_start:
                    updated_windows.append(
                        self._update_window(
                            state=state,
                            start=start,
                            end=end,
                            value=aggregate(aggregation, value),
                            timestamp=timestamp_ms,
                            max_timestamp=timestamp_ms,
                        )
                    )
                else:
                    self._log_expired_window(
                        window=[start, end],
                        timestamp_ms=timestamp_ms,
                        late_by_ms=max_expired_window_end + 1 - timestamp_ms,
                    )
                break

            elif end < left_end:
                # Create the right window for previous messages if it does not exist
                if (right_start := max_timestamp + 1) not in starts and (
                    right_end := right_start + duration
                ) >= timestamp_ms:
                    self._update_window(
                        state=state,
                        start=right_start,
                        end=right_start + duration,
                        value=aggregate(default, value),
                        timestamp=timestamp_ms,
                        max_timestamp=timestamp_ms,
                    )

                # Create a left window with existing aggregation if it falls within the window
                if left_start > max_timestamp:
                    aggregation = default

                updated_windows.append(
                    self._update_window(
                        state=state,
                        start=left_start,
                        end=left_end,
                        value=aggregate(aggregation, value),
                        timestamp=timestamp_ms,
                        max_timestamp=timestamp_ms,
                    )
                )

                # At this point, this is the last window that will ever be considered
                # for existing aggregations. Windows lower than this and lower than
                # the expiration watermark may be deleted.
                max_deleted_window_start = min(start - 1, max_expired_window_start)
                break

        else:
            # As iteration completed without creating (or updating) left window,
            # create it if it is above expiration watermark.
            if left_start > max_expired_window_start:
                updated_windows.append(
                    self._update_window(
                        state=state,
                        start=left_start,
                        end=left_end,
                        value=aggregate(default, value),
                        timestamp=timestamp_ms,
                        max_timestamp=timestamp_ms,
                    )
                )

        if collect:
            state.add_to_collection(value=value, timestamp_ms=timestamp_ms)

        expired_windows: list[WindowResult] = [
            WindowResult(start=start, end=end, value=self._merge_func(aggregation))
            for (start, end), (max_timestamp, aggregation) in state.expire_windows(
                max_start_time=max_expired_window_start,
                delete=False,
                collect=collect,
                end_inclusive=True,
            )
            if end == max_timestamp  # Emit only left windows
        ]

        state.delete_windows(
            max_start_time=max_deleted_window_start,
            delete_values=collect,
        )

        if collect:
            return [], expired_windows
        else:
            return reversed(updated_windows), expired_windows

    def _update_window(
        self,
        state: WindowedState,
        start: int,
        end: int,
        value: Any,
        timestamp: int,
        max_timestamp: int,
    ) -> WindowResult:
        state.update_window(
            start_ms=start,
            end_ms=end,
            value=[max_timestamp, value],
            timestamp_ms=timestamp,
        )
        return WindowResult(start=start, end=end, value=self._merge_func(value))
