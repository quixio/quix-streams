import pytest

from quixstreams.dataframe.windows.aggregations import (
    Collect,
    Count,
    Earliest,
    First,
    Last,
    Latest,
    Max,
    Mean,
    Min,
    Reduce,
    Sum,
)


class TestAggregators:
    @pytest.mark.parametrize(
        "aggregator, values, expected",
        [
            (Sum(), [1, 2], 3),
            (Sum(), [0], 0),
            (Sum(), [], 0),
            (Sum(), [1, None, 2], 3),
            (Sum(), [None], 0),
            (Sum(column="foo"), [{"foo": 1}, {"foo": 2}], 3),
            (Sum(column="foo"), [{"foo": 1}, {"foo": None}], 1),
            (Sum(column="foo"), [{"foo": 1}, {"bar": 2}], 1),
            (Sum(column="foo"), [{"foo": 1}, {}], 1),
            (Count(), [1, "2", None], 2),
            (Count(), [1, "2", object()], 3),
            (Count(column="foo"), [{"foo": 1}, {"foo": 2}, {"foo": 3}], 3),
            (Count(column="foo"), [{"foo": 1}, {"foo": None}, {"foo": 3}], 2),
            (Count(column="foo"), [{"bar": 1}, {"foo": 2}, {}], 1),
            (Mean(), [1, 2], 1.5),
            (Mean(), [0], 0),
            (Mean(), [1, None, 2], 1.5),
            (Mean(), [None], None),
            (Mean(column="foo"), [{"foo": 1}, {"foo": 2}], 1.5),
            (Mean(column="foo"), [{"foo": 1}, {"foo": None}], 1),
            (Mean(column="foo"), [{"foo": 1}, {"bar": 2}], 1),
            (Mean(column="foo"), [{"foo": 1}, {}], 1),
            (
                Reduce(
                    reducer=lambda old, new: old + new,
                    initializer=lambda x: x,
                ),
                ["A", "B", "C"],
                "ABC",
            ),
            (Max(), [3, 1, 2], 3),
            (Max(), [3, None, 2], 3),
            (Max(), [None, 3, 2], 3),
            (Max(), [None], None),
            (Max(column="foo"), [{"foo": 3}, {"foo": 1}], 3),
            (Max(column="foo"), [{"foo": 3}, {"foo": None}], 3),
            (Max(column="foo"), [{"foo": 3}, {"bar": 2}], 3),
            (Max(column="foo"), [{"foo": 3}, {}], 3),
            (Min(), [3, 1, 2], 1),
            (Min(), [3, None, 2], 2),
            (Min(), [None, 3, 2], 2),
            (Min(), [None], None),
            (Min(column="foo"), [{"foo": 3}, {"foo": 1}], 1),
            (Min(column="foo"), [{"foo": 3}, {"foo": None}], 3),
            (Min(column="foo"), [{"foo": 3}, {"bar": 2}], 3),
            (Min(column="foo"), [{"foo": 3}, {}], 3),
        ],
    )
    def test_number_aggregation(self, aggregator, values, expected):
        old = aggregator.initialize()
        for new in values:
            old = aggregator.agg(old, new, 0)

        assert aggregator.result(old) == expected

    @pytest.mark.parametrize(
        "aggregator, values, expected",
        [
            (First(), [[1, 1], [2, 2], [3, 3]], 1),
            (First(), [[1, 3], [2, 2], [3, 1]], 1),
            (
                First(column="foo"),
                [[{"foo": 1}, 1], [{"foo": 2}, 2], [{"foo": 3}, 3]],
                1,
            ),
            (
                First(column="foo"),
                [[{"foo": 1}, 3], [{"foo": 2}, 2], [{"foo": 3}, 1]],
                1,
            ),
            (
                First(column="foo"),
                [[{"bar": 1}, 3], [{"bar": 2}, 2], [{"foo": 3}, 3]],
                3,
            ),
            (First(column="foo"), [[{"bar": 1}, 3], [{}, 2], [{"foo": 3}, 1]], 3),
            (Last(), [[1, 1], [2, 2], [3, 3]], 3),
            (Last(), [[1, 3], [2, 2], [3, 1]], 3),
            (
                Last(column="foo"),
                [[{"foo": 1}, 1], [{"foo": 2}, 2], [{"foo": 3}, 3]],
                3,
            ),
            (
                Last(column="foo"),
                [[{"foo": 1}, 3], [{"foo": 2}, 2], [{"foo": 3}, 1]],
                3,
            ),
            (
                Last(column="foo"),
                [[{"foo": 1}, 3], [{"bar": 2}, 2], [{"bar": 3}, 3]],
                1,
            ),
            (Last(column="foo"), [[{"foo": 1}, 3], [{}, 2], [{"bar": 3}, 1]], 1),
            (Earliest(), [[1, 1], [2, 2], [3, 3]], 1),
            (Earliest(), [[1, 3], [2, 2], [3, 1]], 3),
            (
                Earliest(),
                [[1, 1], [2, 1], [3, 1]],
                1,
            ),  # if multiple equal timestamp use first seen
            (
                Earliest(column="foo"),
                [[{"foo": 1}, 1], [{"foo": 2}, 2], [{"foo": 3}, 3]],
                1,
            ),
            (
                Earliest(column="foo"),
                [[{"foo": 1}, 3], [{"foo": 2}, 2], [{"foo": 3}, 1]],
                3,
            ),
            (
                Earliest(column="foo"),
                [[{"bar": 1}, 3], [{"bar": 2}, 2], [{"foo": 3}, 1]],
                3,
            ),
            (Earliest(column="foo"), [[{"bar": 1}, 3], [{}, 2], [{"foo": 3}, 1]], 3),
            (Latest(), [[1, 1], [2, 2], [3, 3]], 3),
            (Latest(), [[1, 3], [2, 2], [3, 1]], 1),
            (
                Latest(),
                [[1, 1], [2, 1], [3, 1]],
                3,
            ),  # if multiple equal timestamp use last seen
            (
                Latest(column="foo"),
                [[{"foo": 1}, 1], [{"foo": 2}, 2], [{"foo": 3}, 3]],
                3,
            ),
            (
                Latest(column="foo"),
                [[{"foo": 1}, 3], [{"foo": 2}, 2], [{"foo": 3}, 1]],
                1,
            ),
            (
                Latest(column="foo"),
                [[{"bar": 1}, 3], [{"bar": 2}, 2], [{"foo": 3}, 1]],
                3,
            ),
            (Latest(column="foo"), [[{"bar": 1}, 3], [{"foo": 3}, 1], [{}, 2]], 3),
        ],
    )
    def test_other_aggregation(self, aggregator, values, expected):
        old = aggregator.initialize()
        for new, timestamp in values:
            old = aggregator.agg(old, new, timestamp)

        assert aggregator.result(old) == expected

    @pytest.mark.parametrize(
        "aggregation, result",
        [
            (Count(), "Count"),
            (Sum(), "Sum"),
            (Mean(), "Mean"),
            (Max(), "Max"),
            (Min(), "Min"),
            (Reduce(reducer=lambda old, new: old + new, initializer=lambda x: x), "Reduce"),
            (Count("value"), "Count/value"),
            (Sum("value"), "Sum/value"),
            (Mean("value"), "Mean/value"),
            (Min("value"), "Min/value"),
            (Max("value"), "Max/value"),
        ],
    )
    def test_state_suffix(self, aggregation, result):
        assert aggregation.state_suffix == result


class TestCollectors:
    @pytest.mark.parametrize(
        "collector, inputs, result",
        [
            (Collect(), [], []),
            (Collect(), [0, 1, 1, 0], [0, 1, 1, 0]),
            (Collect(), ["foo", "bar", "foo", "bar"], ["foo", "bar", "foo", "bar"]),
            (Collect(), list(range(4)), [0, 1, 2, 3]),
        ],
    )
    def test_collect(self, collector, inputs, result):
        assert collector.result(inputs) == result
