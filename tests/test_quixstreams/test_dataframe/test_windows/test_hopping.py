from typing import Any

import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows import (
    HoppingTimeWindowDefinition,
)
from quixstreams.dataframe.windows.time_based import TimeWindow
from quixstreams.state import WindowedPartitionTransaction


@pytest.fixture()
def hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int, step_ms: int, grace_ms: int = 0
    ) -> HoppingTimeWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = HoppingTimeWindowDefinition(
            duration_ms=duration_ms, step_ms=step_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory


def process(
    window: TimeWindow,
    value: Any,
    key: Any,
    transaction: WindowedPartitionTransaction,
    timestamp_ms: int,
):
    updated = window.process_window(
        value=value, key=key, transaction=transaction, timestamp_ms=timestamp_ms
    )
    expired = window.expire_by_partition(
        transaction=transaction, timestamp_ms=timestamp_ms
    )
    return list(updated), list(expired)


class TestHoppingWindow:
    @pytest.mark.parametrize(
        "duration, grace, step, provided_name, func_name, expected_name",
        [
            (10, 5, 3, "custom_window", "sum", "custom_window_hopping_window_10_3_sum"),
            (10, 5, 3, None, "sum", "hopping_window_10_3_sum"),
            (15, 5, 3, None, "count", "hopping_window_15_3_count"),
        ],
    )
    def test_hopping_window_definition_get_name(
        self,
        duration,
        grace,
        step,
        provided_name,
        func_name,
        expected_name,
        dataframe_factory,
    ):
        twd = HoppingTimeWindowDefinition(
            duration_ms=duration,
            grace_ms=grace,
            step_ms=step,
            dataframe=dataframe_factory(),
            name=provided_name,
        )
        name = twd._get_name(func_name)
        assert name == expected_name

    def test_multiaggregation(
        self,
        hopping_window_definition_factory,
        state_manager,
    ):
        window = hopping_window_definition_factory(duration_ms=10, step_ms=5).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final()
        assert window.name == "hopping_window_10_5"

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=2
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 0,
                        "end": 10,
                        "count": 1,
                        "sum": 1,
                        "mean": 1.0,
                        "max": 1,
                        "min": 1,
                        "collect": [],
                    },
                ),
            ]

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=6
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 0,
                        "end": 10,
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [],
                    },
                ),
                (
                    key,
                    {
                        "start": 5,
                        "end": 15,
                        "count": 1,
                        "sum": 4,
                        "mean": 4,
                        "max": 4,
                        "min": 4,
                        "collect": [],
                    },
                ),
            ]

            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=12
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 0,
                        "end": 10,
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [1, 4],
                    },
                ),
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 5,
                        "end": 15,
                        "count": 2,
                        "sum": 6,
                        "mean": 3.0,
                        "max": 4,
                        "min": 2,
                        "collect": [],
                    },
                ),
                (
                    key,
                    {
                        "start": 10,
                        "end": 20,
                        "count": 1,
                        "sum": 2,
                        "mean": 2,
                        "max": 2,
                        "min": 2,
                        "collect": [],
                    },
                ),
            ]

        # Update window definition
        # * delete an aggregation (min)
        # * change aggregation but keep the name with new aggregation (mean -> max)
        # * add new aggregations (sum2, collect2)
        window = hopping_window_definition_factory(duration_ms=10, step_ms=5).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "hopping_window_10_5"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=16
            )
            assert (
                expired
                == [
                    (
                        key,
                        {
                            "start": 5,
                            "end": 15,
                            "count": 2,
                            "sum": 6,
                            "sum2": 0,  # sum2 only aggregates the values after the update. Use initial value.
                            "mean": None,  # mean was replace by max. The aggregation restarts with the new values. Use initial value.
                            "max": 4,
                            "collect": [4, 2],
                            "collect2": [
                                4,
                                2,
                            ],  # Collect2 has all the values as they were fully collected before the update
                        },
                    )
                ]
            )
            assert (
                updated
                == [
                    (
                        key,
                        {
                            "start": 10,
                            "end": 20,
                            "count": 2,
                            "sum": 3,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 2,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                    (
                        key,
                        {
                            "start": 15,
                            "end": 25,
                            "count": 1,
                            "sum": 1,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 1,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                ]
            )

    def test_hoppingwindow_count(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.count()
        assert window.name == "hopping_window_10_5_count"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 2
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 2
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    def test_hoppingwindow_sum(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.sum()
        assert window.name == "hopping_window_10_5_sum"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 3
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 3
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    def test_hoppingwindow_mean(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.mean()
        assert window.name == "hopping_window_10_5_mean"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 1.5
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 1.5
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    def test_hoppingwindow_reduce(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "hopping_window_10_5_reduce"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == [1]
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == [1]
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    def test_hoppingwindow_max(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.max()
        assert window.name == "hopping_window_10_5_max"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 1
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 1
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    def test_hoppingwindow_min(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.min()
        assert window.name == "hopping_window_10_5_min"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 1
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 1
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    def test_hoppingwindow_collect(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.collect()
        assert window.name == "hopping_window_10_5_collect"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key, transaction=tx, timestamp_ms=101)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=110
            )

        assert not updated
        assert expired == [
            (key, {"start": 95, "end": 105, "value": [1, 2, 3]}),
            (key, {"start": 100, "end": 110, "value": [1, 2, 3]}),
        ]

    @pytest.mark.parametrize(
        "duration, grace, step, name",
        [
            (-10, 5, 3, "test"),  # duration < 0
            (10, -5, 3, "test"),  # grace < 0
            (10, 5, -1, "test"),  # step < 0
            (10, 5, 0, "test"),  # step == 0
            (10, 5, 10, "test"),  # step == duration
            (10, 5, 11, "test"),  # step > duration
        ],
    )
    def test_hopping_window_def_init_invalid(
        self, duration, grace, step, name, dataframe_factory
    ):
        with pytest.raises(ValueError):
            HoppingTimeWindowDefinition(
                duration_ms=duration,
                grace_ms=grace,
                step_ms=step,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_hopping_window_process_window_expired(
        self,
        hopping_window_definition_factory,
        state_manager,
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=0, step_ms=5
        )
        window = window_def.sum()
        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            # Add item to the windows [95, 105) and [100, 110)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 1
            assert updated[0][1]["start"] == 95
            assert updated[0][1]["end"] == 105
            assert updated[1][1]["value"] == 1
            assert updated[1][1]["start"] == 100
            assert updated[1][1]["end"] == 110

            assert not expired

            # Now add item to the windows [105, 115) and [110, 120)
            # The windows [95, 105) and [100, 110) are now expired
            # and should be returned
            _, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )
            expired = expired
            assert len(expired) == 2
            assert expired[0][1]["value"] == 1
            assert expired[0][1]["start"] == 95
            assert expired[0][1]["end"] == 105
            assert expired[1][1]["value"] == 1
            assert expired[1][1]["start"] == 100
            assert expired[1][1]["end"] == 110

    def test_hopping_partition_expiration(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=2, step_ms=5
        )
        window = window_def.sum()
        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            # items for key1
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key1, transaction=tx, timestamp_ms=106)

            # items for key2
            process(window, value=5, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=7, key=key2, transaction=tx, timestamp_ms=108)

            # don't expire key1 due to grace period
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=110
            )

            assert updated == [
                (key1, {"start": 105, "end": 115, "value": 5}),
                (key1, {"start": 110, "end": 120, "value": 2}),
            ]
            assert expired == []

            # an older key 2 is accepted
            updated, expired = process(
                window, value=2, key=key2, transaction=tx, timestamp_ms=109
            )
            assert updated == [
                (key2, {"start": 100, "end": 110, "value": 14}),
                (key2, {"start": 105, "end": 115, "value": 9}),
            ]
            assert expired == []

            # expire key1
            updated, expired = process(
                window, value=3, key=key1, transaction=tx, timestamp_ms=112
            )
            assert updated == [
                (key1, {"start": 105, "end": 115, "value": 8}),
                (key1, {"start": 110, "end": 120, "value": 5}),
            ]
            assert expired == [
                (key1, {"start": 100, "end": 110, "value": 4}),
                (key2, {"start": 100, "end": 110, "value": 14}),
            ]
