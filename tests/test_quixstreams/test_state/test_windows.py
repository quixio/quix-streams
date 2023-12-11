import operator
from collections import namedtuple
from unittest.mock import MagicMock

import pytest

from quixstreams import MessageContext, State
from quixstreams.core.stream import Filtered
from quixstreams.models import MessageTimestamp
from quixstreams.models.topics import Topic
from quixstreams.state.rocksdb.windowed.store import WindowedTransactionState
from quixstreams.state.windows import (
    get_window_range,
    TumblingWindow,
    WindowResult,
    TumblingWindowDefinition,
)
from tests.utils import TopicPartitionStub


class TestTumblingWindow:
    @pytest.mark.parametrize(
        "timestamp, duration, expected",
        [
            (100, 10, (100, 109.9)),
            (100, 60, (60, 119.9)),
        ],
    )
    def test_get_window_range(self, timestamp, duration, expected):
        assert get_window_range(timestamp, duration) == expected

    @pytest.mark.parametrize(
        "duration, grace, provided_name, func_name, expected_name",
        [
            (10, 5, "custom_window", "sum", "custom_window"),
            (10, 5, None, "sum", "tumbling_window_10_sum"),
            (15, 5, None, "count", "tumbling_window_15_count"),
            # Add more test cases if needed
        ],
    )
    def test_tumbling_window_definition_get_name(
        self, duration, grace, provided_name, func_name, expected_name
    ):
        twd = TumblingWindowDefinition(
            duration=duration, grace=grace, dataframe=None, name=provided_name
        )
        name = twd.get_name(func_name)
        assert name == expected_name

    def test_tumbling_window_definition_sum(self):
        dataframe_mock = MagicMock()
        twd = TumblingWindowDefinition(10, 5, dataframe_mock)

        tumbling_window = twd.sum()

        assert isinstance(tumbling_window, TumblingWindow)

        # Mocking WindowedTransactionState and its methods
        state_mock = MagicMock(spec=WindowedTransactionState)
        state_mock.get_window.return_value = 10

        # Test the internal function of the sum method
        func = tumbling_window._func
        result = func(0, 10, 100, 5, state_mock)

        assert result == 15
        state_mock.update_window.assert_called_with(0, 10, timestamp=100, value=15)

    def test_tumbling_window_definition_count(self):
        dataframe_mock = MagicMock()
        twd = TumblingWindowDefinition(10, 5, dataframe_mock)

        tumbling_window = twd.count()

        assert isinstance(tumbling_window, TumblingWindow)

        # Mocking WindowedTransactionState and its methods
        state_mock = MagicMock(spec=WindowedTransactionState)
        state_mock.get_window.return_value = 3

        # Test the internal function of the count method
        func = tumbling_window._func
        result = func(0, 10, 100, None, state_mock)

        assert result == 4
        state_mock.update_window.assert_called_with(0, 10, value=4, timestamp=100)

    @pytest.mark.parametrize(
        "duration, grace, name",
        [
            (-10, 5, "test"),  # duration < 0
            (10, -5, "test"),  # grace < 0
            (10, 5, None),  # name is None
        ],
    )
    def test_tumbling_window_init_invalid(self, duration, grace, name):
        with pytest.raises(ValueError):
            TumblingWindow(duration, grace, name, lambda: None, None)

    @pytest.mark.parametrize(
        "timestamp, latest_timestamp, expired_windows, expected_output",
        [
            # Case: stale timestamp
            (100, 110, [{}], ([], [])),
            # Case: fresh timestamp
            (100, 90, [{}], ([{"value": "test_value", "start": 90, "end": 100}], [])),
        ],
    )
    def test_tumbling_window_process_window(
        self, timestamp, latest_timestamp, expired_windows, expected_output
    ):
        state_mock = MagicMock(spec=WindowedTransactionState)
        state_mock.get_latest_timestamp.return_value = latest_timestamp
        # state_mock.get_expired_windows.return_value = expired_windows
        expired_windows = []  # delete this line when get_expired_windows is implemented

        tw = TumblingWindow(10, 5, "test", lambda st, ed, ts, value, state: value, None)

        result = tw.process_window(value=5, state=state_mock, timestamp=timestamp)
        expected_windows = expected_output[0]

        if expected_windows:
            start, end = get_window_range(timestamp, tw._duration)
            expected_updated_window = WindowResult(value=5, start=start, end=end)
            assert result == ([expected_updated_window], expired_windows)
        else:
            assert result == expected_output

    def test_tumbling_window_latest(self, tumbling_window, mock_state):
        mock_state.get_latest_timestamp.return_value = 100
        tumbling_window._dataframe.apply_window.return_value = ["processed_value"]

        result = tumbling_window.latest()

        assert result == "processed_value"
        tumbling_window._dataframe.apply_window.assert_called_once()

    def test_tumbling_window_all(self, tumbling_window, mock_state):
        mock_state.get_latest_timestamp.return_value = 100
        tumbling_window._dataframe.apply_window.return_value = ["processed_values"]

        result = tumbling_window.all()

        assert result == ["processed_values"]
        tumbling_window._dataframe.apply_window.assert_called_once()

    def test_tumbling_window_final(self, tumbling_window, mock_state):
        mock_state.get_latest_timestamp.return_value = 100
        tumbling_window._dataframe.apply_window.return_value = ["final_values"]

        result = tumbling_window.final()

        assert result == ["final_values"]
        tumbling_window._dataframe.apply_window.assert_called_once()
