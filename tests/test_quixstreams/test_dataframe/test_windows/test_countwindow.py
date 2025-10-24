from typing import Any

import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows import (
    HoppingCountWindowDefinition,
    TumblingCountWindowDefinition,
)
from quixstreams.dataframe.windows.count_based import CountWindow
from quixstreams.state import WindowedPartitionTransaction


def process(
    window: CountWindow,
    value: Any,
    key: Any,
    transaction: WindowedPartitionTransaction,
    timestamp_ms: int,
):
    updated, expired = window.process_window(
        value=value, key=key, transaction=transaction, timestamp_ms=timestamp_ms
    )

    return list(updated), list(expired)


@pytest.fixture()
def count_tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(count: int) -> TumblingCountWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = TumblingCountWindowDefinition(dataframe=sdf, count=count)
        return window_def

    return factory


class TestCountTumblingWindow:
    @pytest.mark.parametrize(
        "count, name",
        [
            (-10, "test"),
            (0, "test"),
            (1, "test"),
        ],
    )
    def test_init_invalid(self, count, name, dataframe_factory):
        with pytest.raises(ValueError):
            TumblingCountWindowDefinition(
                count=count,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_multiaggregation(
        self,
        count_tumbling_window_definition_factory,
        state_manager,
    ):
        window = count_tumbling_window_definition_factory(count=2).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final()
        assert window.name == "tumbling_count_window"

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
                        "start": 2,
                        "end": 2,
                        "count": 1,
                        "sum": 1,
                        "mean": 1.0,
                        "max": 1,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=4
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 4,
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [1, 4],
                    },
                )
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 4,
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=12
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 12,
                        "end": 12,
                        "count": 1,
                        "sum": 2,
                        "mean": 2.0,
                        "max": 2,
                        "min": 2,
                        "collect": [],
                    },
                )
            ]

        # Update window definition
        # * delete an aggregation (min)
        # * change aggregation but keep the name with new aggregation (mean -> max)
        # * add new aggregations (sum2, collect2)
        window = count_tumbling_window_definition_factory(count=2).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "tumbling_count_window"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=13
            )
            assert (
                expired
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 13,
                            "count": 2,
                            "sum": 3,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 2,
                            "collect": [2, 1],
                            "collect2": [
                                2,
                                1,
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
                            "start": 12,
                            "end": 13,
                            "count": 2,
                            "sum": 3,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 2,
                            "collect": [],
                            "collect2": [],
                        },
                    )
                ]
            )

            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=15
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 15,
                        "end": 15,
                        "count": 1,
                        "sum": 5,
                        "sum2": 5,
                        "mean": 5,
                        "max": 5,
                        "collect": [],
                        "collect2": [],
                    },
                )
            ]

    def test_count(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.count()
        assert window.name == "tumbling_count_window_count"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=0, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert not expired

    def test_sum(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.sum()
        assert window.name == "tumbling_count_window_sum"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 3
        assert not expired

    def test_mean(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.mean()
        assert window.name == "tumbling_count_window_mean"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1.5
        assert not expired

    def test_reduce(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "tumbling_count_window_reduce"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == [2, 1]
        assert not expired

    def test_max(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.max()
        assert window.name == "tumbling_count_window_max"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert not expired

    def test_min(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.min()
        assert window.name == "tumbling_count_window_min"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1
        assert not expired

    def test_collect(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=3)
        window = window_def.collect()
        assert window.name == "tumbling_count_window_collect"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=1, transaction=tx, timestamp_ms=100)
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=101
            )

        assert not updated
        assert expired == [("", {"start": 100, "end": 101, "value": [1, 2, 3]})]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == []

    def test_window_expired(
        self,
        count_tumbling_window_definition_factory,
        state_manager,
    ):
        window_def = count_tumbling_window_definition_factory(count=2)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            # Add first item to the window
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert updated[0][1]["start"] == 100
            assert updated[0][1]["end"] == 100
            assert not expired

            # Now add second item to the window
            # The window is now expired and should be returned
            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert updated[0][1]["start"] == 100
            assert updated[0][1]["end"] == 110

            assert len(expired) == 1
            assert expired[0][1]["value"] == 3
            assert expired[0][1]["start"] == 100
            assert expired[0][1]["end"] == 110

    def test_multiple_keys_sum(
        self, count_tumbling_window_definition_factory, state_manager
    ):
        window_def = count_tumbling_window_definition_factory(count=3)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="key1", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 1
            updated, expired = process(
                window, key="key2", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 5

            updated, expired = process(
                window, key="key1", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 3
            updated, expired = process(
                window, key="key2", value=4, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 9

            updated, expired = process(
                window, key="key1", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == 6
            assert updated[0][1]["value"] == 6

            updated, expired = process(
                window, key="key1", value=4, transaction=tx, timestamp_ms=130
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 4

            updated, expired = process(
                window, key="key2", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == 12
            assert updated[0][1]["value"] == 12

            updated, expired = process(
                window, key="key2", value=2, transaction=tx, timestamp_ms=130
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 2
            updated, expired = process(
                window, key="key1", value=5, transaction=tx, timestamp_ms=140
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 9

            updated, expired = process(
                window, key="key2", value=1, transaction=tx, timestamp_ms=140
            )
            assert len(expired) == 0
            assert updated[0][1]["value"] == 3

    def test_multiple_keys_collect(
        self, count_tumbling_window_definition_factory, state_manager
    ):
        window_def = count_tumbling_window_definition_factory(count=3)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="key1", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key2", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key2", value=4, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == [1, 2, 3]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=4, transaction=tx, timestamp_ms=130
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == [5, 4, 3]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=2, transaction=tx, timestamp_ms=130
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key1", value=5, transaction=tx, timestamp_ms=140
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=1, transaction=tx, timestamp_ms=140
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=0, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [2, 1, 0]
            assert len(updated) == 0
            updated, expired = process(
                window, key="key1", value=6, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [4, 5, 6]
            assert len(updated) == 0


@pytest.fixture()
def count_hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(count: int, step: int) -> HoppingCountWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = HoppingCountWindowDefinition(dataframe=sdf, count=count, step=step)
        return window_def

    return factory


class TestCountHoppingWindow:
    @pytest.mark.parametrize(
        "count, step, name",
        [
            (-10, 1, "test"),
            (0, 1, "test"),
            (1, 1, "test"),
            (2, 0, "test"),
            (2, -1, "test"),
        ],
    )
    def test_init_invalid(self, count, step, name, dataframe_factory):
        with pytest.raises(ValueError):
            HoppingCountWindowDefinition(
                count=count,
                step=step,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_multiaggregation(
        self,
        count_hopping_window_definition_factory,
        state_manager,
    ):
        window = count_hopping_window_definition_factory(count=3, step=2).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final()
        assert window.name == "hopping_count_window"

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
                        "start": 2,
                        "end": 2,
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
                window, value=5, key=key, transaction=tx, timestamp_ms=6
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 6,
                        "count": 2,
                        "sum": 6,
                        "mean": 3.0,
                        "max": 5,
                        "min": 1,
                        "collect": [],
                    },
                ),
            ]

            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=12
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 12,
                        "count": 3,
                        "sum": 9,
                        "mean": 3.0,
                        "max": 5,
                        "min": 1,
                        "collect": [1, 5, 3],
                    },
                ),
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 12,
                        "count": 3,
                        "sum": 9,
                        "mean": 3,
                        "max": 5,
                        "min": 1,
                        "collect": [],
                    },
                ),
                (
                    key,
                    {
                        "start": 12,
                        "end": 12,
                        "count": 1,
                        "sum": 3,
                        "mean": 3,
                        "max": 3,
                        "min": 3,
                        "collect": [],
                    },
                ),
            ]

        # Update window definition
        # * delete an aggregation (min)
        # * change aggregation but keep the name with new aggregation (mean -> max)
        # * add new aggregations (sum2, collect2)
        window = count_hopping_window_definition_factory(count=3, step=2).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "hopping_count_window"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=16
            )
            assert not expired
            assert (
                updated
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 16,
                            "count": 2,
                            "sum": 4,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 3,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                ]
            )

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=22
            )
            assert (
                expired
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 22,
                            "count": 3,
                            "sum": 8,
                            "sum2": 5,  # sum2 only aggregates the values after the update
                            "mean": 4,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 4,
                            "collect": [3, 1, 4],
                            "collect2": [3, 1, 4],
                        },
                    ),
                ]
            )
            assert (
                updated
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 22,
                            "count": 3,
                            "sum": 8,
                            "sum2": 5,  # sum2 only aggregates the values after the update
                            "mean": 4,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 4,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                    (
                        key,
                        {
                            "start": 22,
                            "end": 22,
                            "count": 1,
                            "sum": 4,
                            "sum2": 4,
                            "mean": 4,
                            "max": 4,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                ]
            )

    def test_count(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.count()
        assert window.name == "hopping_count_window_count"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert expired == []

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 2
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 1
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 2
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4

    def test_sum(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.sum()
        assert window.name == "hopping_count_window_sum"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3  # 1 + 2
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 6  # 1 + 2 + 3
            assert updated[1][1]["value"] == 3
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 10  # 1 + 2 + 3 + 4
            assert updated[1][1]["value"] == 7  # 3 + 4
            assert len(expired) == 1
            assert expired[0][1]["value"] == 10

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 12  # 3 + 4 + 5
            assert updated[1][1]["value"] == 5
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 18  # 3 + 4 + 5 + 6
            assert updated[1][1]["value"] == 11  # 5 + 6
            assert len(expired) == 1
            assert expired[0][1]["value"] == 18

    def test_mean(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.mean()
        assert window.name == "hopping_count_window_mean"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1.5  # (1 + 2) / 2
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2  # (1 + 2 + 3) / 3
            assert updated[1][1]["value"] == 3
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2.5  # (1 + 2 + 3 + 4) / 4
            assert updated[1][1]["value"] == 3.5  # 3 + 4
            assert len(expired) == 1
            assert expired[0][1]["value"] == 2.5

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4  # (3 + 4 + 5) / 3
            assert updated[1][1]["value"] == 5
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert (
                updated[0][1]["value"] == 4.5
            )  # (3 # sum2 only aggregates the values after the update + 6) / 2
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4.5

    def test_reduce(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "hopping_count_window_reduce"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == [1]
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == [1, 2]
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [1, 2, 3]
            assert updated[1][1]["value"] == [3]
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [1, 2, 3, 4]
            assert updated[1][1]["value"] == [3, 4]
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4]

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [3, 4, 5]
            assert updated[1][1]["value"] == [5]
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [3, 4, 5, 6]
            assert updated[1][1]["value"] == [5, 6]
            assert len(expired) == 1
            assert expired[0][1]["value"] == [3, 4, 5, 6]

    def test_max(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.max()
        assert window.name == "hopping_count_window_max"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 4
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 4
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 5
            assert updated[1][1]["value"] == 5
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 6
            assert updated[1][1]["value"] == 6
            assert len(expired) == 1
            assert expired[0][1]["value"] == 6

    def test_min(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.min()
        assert window.name == "hopping_count_window_min"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 4
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2
            assert updated[1][1]["value"] == 3
            assert expired == []

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2
            assert updated[1][1]["value"] == 3
            assert len(expired) == 1
            assert expired[0][1]["value"] == 2

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 6
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 5
            assert len(expired) == 1
            assert expired[0][1]["value"] == 3

    def test_collect(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.collect()
        assert window.name == "hopping_count_window_collect"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4]

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [3, 4, 5, 6]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix="")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [5, 6]

    def test_unaligned_steps(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=5, step=2)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4, 5]

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=7, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [3, 4, 5, 6, 7]

            updated, expired = process(
                window, key="", value=8, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=9, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [5, 6, 7, 8, 9]

            updated, expired = process(
                window, key="", value=10, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=11, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [7, 8, 9, 10, 11]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix="")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [9, 10, 11]

    def test_multiple_keys_sum(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=3, step=1)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="key1", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            updated, expired = process(
                window, key="key2", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 1
            assert updated[0][1]["value"] == 5

            updated, expired = process(
                window, key="key1", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 2

            updated, expired = process(
                window, key="key2", value=4, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 2
            assert updated[0][1]["value"] == 9
            assert updated[1][1]["value"] == 4

            updated, expired = process(
                window, key="key1", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == 6
            assert len(updated) == 3
            assert updated[0][1]["value"] == 6
            assert updated[1][1]["value"] == 5
            assert updated[2][1]["value"] == 3

            updated, expired = process(
                window, key="key1", value=4, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == 9
            assert len(updated) == 3
            assert updated[0][1]["value"] == 9
            assert updated[1][1]["value"] == 7
            assert updated[2][1]["value"] == 4

            updated, expired = process(
                window, key="key2", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == 12
            assert len(updated) == 3
            assert updated[0][1]["value"] == 12
            assert updated[1][1]["value"] == 7
            assert updated[2][1]["value"] == 3

            updated, expired = process(
                window, key="key2", value=2, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == 9
            assert len(updated) == 3
            assert updated[0][1]["value"] == 9
            assert updated[1][1]["value"] == 5
            assert updated[2][1]["value"] == 2

            updated, expired = process(
                window, key="key1", value=5, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == 12
            assert len(updated) == 3
            assert updated[0][1]["value"] == 12
            assert updated[1][1]["value"] == 9
            assert updated[2][1]["value"] == 5

            updated, expired = process(
                window, key="key2", value=1, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == 6
            assert len(updated) == 3
            assert updated[0][1]["value"] == 6
            assert updated[1][1]["value"] == 3
            assert updated[2][1]["value"] == 1

    def test_multiple_keys_collect(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=3, step=1)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="key1", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key2", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key2", value=4, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == [1, 2, 3]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=4, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [2, 3, 4]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == [5, 4, 3]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=2, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [4, 3, 2]
            assert len(updated) == 0
            updated, expired = process(
                window, key="key1", value=5, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [3, 4, 5]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=1, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [3, 2, 1]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=0, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [2, 1, 0]
            assert len(updated) == 0
            updated, expired = process(
                window, key="key1", value=6, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [4, 5, 6]
            assert len(updated) == 0
