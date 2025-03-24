import pytest

from quixstreams.dataframe.windows.aggregations import (
    Count,
    Max,
    Mean,
    Min,
    Reduce,
    Sum,
)


@pytest.mark.parametrize(
    ["aggregator", "values", "expected"],
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
        (Count(), [1, "2", None], 3),
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
def test_aggregators(aggregator, values, expected):
    old = aggregator.initialize()
    for new in values:
        old = aggregator.agg(old, new)

    assert aggregator.result(old) == expected


# @pytest.mark.parametrize("aggregator", [Sum(), Mean()])
# def test_aggregators_exceptions(aggregator):
#     with pytest.raises(TypeError):
#         aggregator.agg(aggregator.initialize(), "1")
