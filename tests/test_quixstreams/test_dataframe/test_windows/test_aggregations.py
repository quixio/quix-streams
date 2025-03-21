import pytest

from quixstreams.dataframe.windows.aggregations import (
    Collect,
    Count,
    Max,
    Mean,
    Min,
    Reduce,
    Sum,
)


class TestAggregators:
    @pytest.mark.parametrize(
        "aggregation, inputs, result",
        [
            (Count, [1, 2, 3, 4], 4),
            (Sum, [1, 2, 3, 4], 10),
            (Sum, [1, -4, 3, 4], 4),
            (Mean, [1, 2, 3, 4], 2.5),
            (Mean, [1, -4, 3, 4], 1),
            (Max, [1, 2, 3, 4], 4),
            (Max, [1, -4, 3, 4], 4),
            (Min, [1, 2, 3, 4], 1),
            (Min, [1, -4, 3, 4], -4),
        ],
    )
    def test_aggregation(self, aggregation, inputs, result):
        agg = aggregation()

        value = agg.initialize()
        for i in inputs:
            value = agg.agg(value, i)

        assert agg.result(value) == result

    @pytest.mark.parametrize(
        "aggregation, inputs, result",
        [
            (Sum, [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}], 10),
            (Sum, [{"value": 1}, {"value": -4}, {"value": 3}, {"value": 4}], 4),
            (Mean, [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}], 2.5),
            (Mean, [{"value": 1}, {"value": -4}, {"value": 3}, {"value": 4}], 1),
            (Max, [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}], 4),
            (Max, [{"value": 1}, {"value": -4}, {"value": 3}, {"value": 4}], 4),
            (Min, [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}], 1),
            (Min, [{"value": 1}, {"value": -4}, {"value": 3}, {"value": 4}], -4),
        ],
    )
    def test_aggregation_column(self, aggregation, inputs, result):
        agg = aggregation("value")

        value = agg.initialize()
        for i in inputs:
            value = agg.agg(value, i)

        assert agg.result(value) == result

    @pytest.mark.parametrize(
        "aggregation, inputs",
        [
            (Sum, {"value": 1}),
            (Sum, 1),
            (Mean, {"value": 1}),
            (Mean, 1),
            (Max, {"value": 1}),
            (Max, 1),
            (Min, {"value": 1}),
            (Min, 1),
        ],
    )
    def test_aggregation_missing_column(self, aggregation, inputs):
        agg = aggregation("missing")

        value = agg.initialize()
        with pytest.raises((KeyError, TypeError)):
            agg.agg(value, inputs)

    @pytest.mark.parametrize(
        "inputs, result",
        [
            ([1, 2, 3, 4], 10),
            ([1, -4, 3, 4], 4),
        ],
    )
    def test_reduce(self, inputs, result):
        def initializer(new):
            return new

        def reducer(old, new):
            return old + new

        agg = Reduce(initializer=initializer, reducer=reducer)

        value = agg.initialize()
        for i in inputs:
            value = agg.agg(value, i)

        assert agg.result(value) == result

    @pytest.mark.parametrize(
        "aggregation, result",
        [
            (Count(), "Count"),
            (Sum(), "Sum"),
            (Mean(), "Mean"),
            (Max(), "Max"),
            (Min(), "Min"),
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
        "inputs, result",
        [
            ([], []),
            ([0, 1, 2, 3], [0, 1, 2, 3]),
            (range(4), [0, 1, 2, 3]),
        ],
    )
    def test_collect(self, inputs, result):
        col = Collect()
        assert col.result(inputs) == result
