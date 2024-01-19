import pytest

from quixstreams.dataframe.windows.base import get_window_ranges


class TestGetWindowRanges:
    @pytest.mark.parametrize(
        "timestamp, duration, step, expected",
        [
            (100, 30, 10, [(80, 110), (90, 120), (100, 130)]),  # Standard case
            (50, 30, 10, [(30, 60), (40, 70), (50, 80)]),  # Standard case
            (75, 20, 20, [(60, 80)]),  # Step equals duration
            (85, 15, 0, [(75, 90)]),  # Zero step (defaults to duration)
            (100, 20, 20, [(100, 120)]),  # Timestamp on step boundary
            (1, 20, 20, [(0, 20)]),  # Timestamp shouldn't go below zero
        ],
    )
    def test_get_window_ranges_with_step(self, timestamp, duration, step, expected):
        assert (
            get_window_ranges(
                timestamp_ms=timestamp, duration_ms=duration, step_ms=step
            )
            == expected
        )

    @pytest.mark.parametrize(
        "timestamp, duration, expected",
        [
            (100, 10, [(100, 110)]),
            (100, 60, [(60, 120)]),
        ],
    )
    def test_get_window_ranges_no_step(self, timestamp, duration, expected):
        assert get_window_ranges(timestamp, duration) == expected
