import pytest
from unittest.mock import MagicMock
from quixstreams.dataframe import StreamingDataFrame
from quixstreams.state.rocksdb.windowed.store import WindowedTransactionState
from quixstreams.windows import HoppingWindowDefinition, HoppingWindow
from quixstreams.windows.base import get_window_ranges, WindowResult


class TestHoppingWindow:
    @pytest.fixture
    def state_mock(self):
        return MagicMock(spec=WindowedTransactionState)

    @pytest.fixture
    def hopping_window_def(self):
        return HoppingWindowDefinition(
            duration=10, step=3, grace=5, dataframe=MagicMock(spec=StreamingDataFrame)
        )

    @pytest.mark.parametrize(
        "timestamp, duration, step, expected",
        [
            (100, 30, 10, [(80, 109.9), (90, 119.9), (100, 129.9)]),  # Standard case
            (50, 30, 10, [(30, 59.9), (40, 69.9), (50, 79.9)]),  # Standard case
            (75, 20, 20, [(60, 79.9)]),  # Step equals duration
            (85, 15, 0, [(75, 89.9)]),  # Zero step (defaults to duration)
            (123, 10.5, 5.5, [(115.5, 125.9), (121.0, 131.4)]),  # Fractional values
            (100, 20, 20, [(100, 119.9)]),  # Timestamp on step boundary
        ],
    )
    def test_get_window_ranges(self, timestamp, duration, step, expected):
        assert (
            get_window_ranges(timestamp=timestamp, window_duration=duration, step=step)
            == expected
        )

    @pytest.mark.parametrize(
        "duration, grace, provided_name, func_name, expected_name",
        [
            (10, 5, "custom_window", "sum", "custom_window"),
            (10, 5, None, "sum", "hopping_window_10_sum"),
            (15, 5, None, "count", "hopping_window_15_count"),
        ],
    )
    def test_hopping_window_definition_get_name(
        self, duration, grace, provided_name, func_name, expected_name
    ):
        twd = HoppingWindowDefinition(
            duration=duration, grace=grace, dataframe=None, name=provided_name
        )
        name = twd._get_name(func_name)
        assert name == expected_name

    def test_hopping_window_definition_count(self, hopping_window_def, state_mock):
        tw = hopping_window_def.count()
        assert isinstance(tw, HoppingWindow)
        assert "hopping_window_10_3_count" == tw._name

        state_mock.get_window.return_value = 3
        result = tw._func(0, 10, 100, None, state_mock)
        assert result == 4  # New count value
        state_mock.update_window.assert_called_with(0, 10, timestamp=100, value=4)

    def test_hopping_window_definition_sum(self, hopping_window_def, state_mock):
        tw = hopping_window_def.sum()
        assert isinstance(tw, HoppingWindow)
        assert "hopping_window_10_3_sum" == tw._name

        state_mock.get_window.return_value = 10
        result = tw._func(0, 10, 100, 5, state_mock)
        assert result == 15  # New sum value
        state_mock.update_window.assert_called_with(0, 10, timestamp=100, value=15)

    def test_mean_method(self, hopping_window_def, state_mock):
        tw = hopping_window_def.mean()
        assert isinstance(tw, HoppingWindow)
        assert "hopping_window_10_3_mean" == tw._name

        state_mock.get_window.return_value = (10.0, 2)
        result = tw._func(0, 10, 100, 5, state_mock)
        assert result == 15 / 3  # New mean value
        state_mock.update_window.assert_called_with(
            0, 10, timestamp=100, value=(15.0, 3)
        )

    def test_reduce_method(self, hopping_window_def, state_mock):
        tw = hopping_window_def.reduce(lambda a, b: a + b, initializer=lambda _: 0)
        assert isinstance(tw, HoppingWindow)
        assert "hopping_window_10_3_reduce" == tw._name

        state_mock.get_window.return_value = 10
        result = tw._func(0, 10, 100, 5, state_mock)
        assert result == 15  # Reduced value
        state_mock.update_window.assert_called_with(0, 10, timestamp=100, value=15)

    def test_max_method(self, hopping_window_def, state_mock):
        tw = hopping_window_def.max()
        assert isinstance(tw, HoppingWindow)
        assert "hopping_window_10_3_max" == tw._name

        state_mock.get_window.return_value = 10
        result = tw._func(0, 10, 100, 15, state_mock)
        assert result == 15  # Max value
        state_mock.update_window.assert_called_with(0, 10, timestamp=100, value=15)

    def test_min_method(self, hopping_window_def, state_mock):
        tw = hopping_window_def.min()
        assert isinstance(tw, HoppingWindow)
        assert "hopping_window_10_3_min" == tw._name

        state_mock.get_window.return_value = 10
        result = tw._func(0, 10, 100, 5, state_mock)
        assert result == 5  # Min value
        state_mock.update_window.assert_called_with(0, 10, timestamp=100, value=5)

    @pytest.mark.parametrize(
        "duration, grace, name",
        [
            (-10, 5, "test"),  # duration < 0
            (10, -5, "test"),  # grace < 0
        ],
    )
    def test_hopping_window_def_init_invalid(self, duration, grace, name):
        with pytest.raises(ValueError):
            HoppingWindowDefinition(
                duration=duration, grace=grace, name=name, dataframe=None
            )

    @pytest.mark.parametrize(
        "timestamp, latest_timestamp, expired_windows_input, expected_output",
        [
            # Case: stale timestamp
            (100, 200, [], ([], [])),
            # Case: fresh timestamp
            (
                100,
                90,
                [],
                (
                    [  # updated_output
                        WindowResult(start=93, end=102.9, value=5),
                        WindowResult(start=96, end=105.9, value=5),
                        WindowResult(start=99, end=108.9, value=5),
                    ],
                    [],  # expired_output
                ),
            ),
        ],
    )
    def test_hopping_window_process_window(
        self, timestamp, latest_timestamp, expired_windows_input, expected_output
    ):
        state_mock = MagicMock(spec=WindowedTransactionState)
        state_mock.get_latest_timestamp.return_value = latest_timestamp
        # state_mock.get_expired_windows.return_value = expired_windows
        expired_windows = expired_windows_input  # delete this line when get_expired_windows is implemented

        tw = HoppingWindow(
            duration=10,
            grace=5,
            step=3,
            name="test",
            func=lambda st, ed, ts, value, state: value,
            dataframe=None,
        )

        result = tw._process_window(value=5, state=state_mock, timestamp=timestamp)
        expected_windows = expected_output[0]

        assert result == expected_output
