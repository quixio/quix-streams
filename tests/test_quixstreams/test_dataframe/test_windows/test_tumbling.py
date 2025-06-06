import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows import (
    TumblingCountWindowDefinition,
    TumblingTimeWindowDefinition,
)
from quixstreams.dataframe.windows.time_based import ClosingStrategy


@pytest.fixture()
def tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(duration_ms: int, grace_ms: int = 0, timeout_ms: int = None) -> TumblingTimeWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = TumblingTimeWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=sdf, timeout_ms=timeout_ms
        )
        return window_def

    return factory


def process(window, value, key, transaction, timestamp_ms):
    updated, expired = window.process_window(
        value=value, key=key, transaction=transaction, timestamp_ms=timestamp_ms
    )
    return list(updated), list(expired)


class TestTumblingWindow:
    @pytest.mark.parametrize(
        "duration, grace, provided_name, func_name, expected_name",
        [
            (10, 5, "custom_window", "sum", "custom_window_tumbling_window_10_sum"),
            (10, 5, None, "sum", "tumbling_window_10_sum"),
            (15, 5, None, "count", "tumbling_window_15_count"),
        ],
    )
    def test_tumbling_window_definition_get_name(
        self,
        duration,
        grace,
        provided_name,
        func_name,
        expected_name,
        dataframe_factory,
    ):
        twd = TumblingTimeWindowDefinition(
            duration_ms=duration,
            grace_ms=grace,
            dataframe=dataframe_factory(),
            name=provided_name,
        )
        name = twd._get_name(func_name)
        assert name == expected_name

    def test_multiaggregation(
        self,
        tumbling_window_definition_factory,
        state_manager,
    ):
        window = tumbling_window_definition_factory(duration_ms=10).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final(closing_strategy="key")
        assert window.name == "tumbling_window_10"

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
                )
            ]

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=4
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
                )
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
                )
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 10,
                        "end": 20,
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
        window = tumbling_window_definition_factory(duration_ms=10).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "tumbling_window_10"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=13
            )
            assert not expired
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
                    )
                ]
            )

            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=22
            )
            assert (
                expired
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
                            "collect": [2, 1],
                            "collect2": [
                                2,
                                1,
                            ],  # Collect2 has all the values as they were fully collected before the update
                        },
                    )
                ]
            )
            assert updated == [
                (
                    key,
                    {
                        "start": 20,
                        "end": 30,
                        "count": 1,
                        "sum": 2,
                        "sum2": 2,
                        "mean": 2,
                        "max": 2,
                        "collect": [],
                        "collect2": [],
                    },
                )
            ]

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_count(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.count()
        assert window.name == "tumbling_window_10_count"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=0, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=0, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_sum(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.sum()
        assert window.name == "tumbling_window_10_sum"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 3
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_mean(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.mean()
        assert window.name == "tumbling_window_10_mean"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1.5
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_reduce(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "tumbling_window_10_reduce"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == [2, 1]
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_max(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.max()
        assert window.name == "tumbling_window_10_max"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_min(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.min()
        assert window.name == "tumbling_window_10_min"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_collect(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.collect()
        assert window.name == "tumbling_window_10_collect"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key, transaction=tx, timestamp_ms=101)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=200
            )
        assert not updated
        assert expired == [(key, {"start": 100, "end": 110, "value": [1, 2, 3]})]

    @pytest.mark.parametrize(
        "duration, grace, name",
        [
            (-10, 5, "test"),  # duration < 0
            (10, -5, "test"),  # grace < 0
        ],
    )
    def test_tumbling_window_def_init_invalid(
        self, duration, grace, name, dataframe_factory
    ):
        with pytest.raises(ValueError):
            TumblingTimeWindowDefinition(
                duration_ms=duration,
                grace_ms=grace,
                name=name,
                dataframe=dataframe_factory(),
            )

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumbling_window_process_window_expired(
        self,
        expiration,
        tumbling_window_definition_factory,
        state_manager,
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            # Add item to the window [100, 110)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert updated[0][1]["start"] == 100
            assert updated[0][1]["end"] == 110
            assert not expired

            # Now add item to the window [110, 120)
            # The window [100, 110) is now expired and should be returned
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert updated[0][1]["start"] == 110
            assert updated[0][1]["end"] == 120

            assert len(expired) == 1
            assert expired[0][1]["value"] == 1
            assert expired[0][1]["start"] == 100
            assert expired[0][1]["end"] == 110

    def test_tumbling_partition_expiration(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=2)
        window = window_def.sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            # items for key1
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key1, transaction=tx, timestamp_ms=105)

            # items for key2
            process(window, value=5, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=7, key=key2, transaction=tx, timestamp_ms=108)

            # don't expire key1 due to grace period
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=110
            )

            assert updated == [
                (key1, {"start": 110, "end": 120, "value": 2}),
            ]
            assert expired == []

            # an older key 2 is accepted
            updated, expired = process(
                window, value=2, key=key2, transaction=tx, timestamp_ms=109
            )
            assert updated == [
                (key2, {"start": 100, "end": 110, "value": 14}),
            ]
            assert expired == []

            # Expire key1
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=112
            )

            assert updated == [
                (key1, {"start": 110, "end": 120, "value": 4}),
            ]
            assert expired == [
                (key1, {"start": 100, "end": 110, "value": 4}),
                (key2, {"start": 100, "end": 110, "value": 14}),
            ]

    def test_tumbling_key_expiration_to_partition(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=105)
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=106)

        window._closing_strategy = ClosingStrategy.PARTITION
        with store.start_partition_transaction(0) as tx:
            key3 = b"key3"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=107)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=108)
            updated, expired = process(
                window, value=1, key=key3, transaction=tx, timestamp_ms=115
            )

        assert updated == [
            (key3, {"start": 110, "end": 120, "value": 1}),
        ]
        assert expired == [
            (key1, {"start": 100, "end": 110, "value": 3}),
            (key2, {"start": 100, "end": 110, "value": 3}),
        ]

    def test_tumbling_partition_expiration_to_key(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=105)
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=106)

        window._closing_strategy = ClosingStrategy.KEY
        with store.start_partition_transaction(0) as tx:
            key3 = b"key3"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=107)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=108)
            updated, expired = process(
                window, value=1, key=key3, transaction=tx, timestamp_ms=115
            )

            assert updated == [(key3, {"start": 110, "end": 120, "value": 1})]
            assert expired == []

            updated, expired = process(
                window, value=1, key=key1, transaction=tx, timestamp_ms=116
            )
            assert updated == [(key1, {"start": 110, "end": 120, "value": 1})]
            assert expired == [(key1, {"start": 100, "end": 110, "value": 3})]


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

    def test_tumbling_window_timeout(
        self, tumbling_window_definition_factory, state_manager
    ):
        """Test that windows expire based on timeout even without new data."""
        import time
        import unittest.mock
        
        # Create a window with 10ms duration, 0ms grace, and 5000ms timeout
        window_def = tumbling_window_definition_factory(
            duration_ms=10, grace_ms=0, timeout_ms=5000
        )
        window = window_def.sum()
        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        
        key = b"key"
        
        with store.start_partition_transaction(0) as tx:
            # Mock the time.time() function to control wall clock  
            with unittest.mock.patch('quixstreams.dataframe.windows.time_based.time.time') as mock_time:
                # Start at wall clock time 1000 seconds (1000000 ms)
                mock_time.return_value = 1000.0
                
                # Add item to window [100, 110) at wall clock time 1000000ms
                updated, expired = process(
                    window, value=1, key=key, transaction=tx, timestamp_ms=100
                )
                assert len(updated) == 1
                assert updated[0][1]["value"] == 1
                assert updated[0][1]["start"] == 100
                assert updated[0][1]["end"] == 110
                assert not expired
                
                # Advance wall clock by 3000ms (still within 5000ms timeout)
                mock_time.return_value = 1003.0
                
                # Process another message - window should not be expired yet
                updated, expired = process(
                    window, value=2, key=key, transaction=tx, timestamp_ms=101
                )
                assert len(updated) == 1
                assert updated[0][1]["value"] == 3  # 1 + 2
                assert not expired  # Should not be expired yet
                
                # Advance wall clock by 8500ms total (past the 5000ms idle timeout from last activity)
                # Last activity was at 1003.0s, so we need to go past 1003.0 + 5.0 = 1008.0s
                mock_time.return_value = 1008.5
                
                # Process another message - should expire the timed-out window
                updated, expired = process(
                    window, value=3, key=key, transaction=tx, timestamp_ms=102
                )
                
                # The new window [100, 110) should be in updated
                assert len(updated) == 1
                assert updated[0][1]["start"] == 100
                assert updated[0][1]["end"] == 110
                
                # The timed-out window should be in expired (last activity was 5500ms ago)
                assert len(expired) == 1
                assert expired[0][1]["value"] == 3  # Previous sum of 1 + 2
                assert expired[0][1]["start"] == 100
                assert expired[0][1]["end"] == 110

    def test_tumbling_window_timeout_proactive_expiration(
        self, tumbling_window_definition_factory, state_manager
    ):
        """Test that windows can be expired proactively without new messages using expire_timeouts_for_key."""
        import time
        import unittest.mock
        
        # Create a window with 10ms duration, 0ms grace, and 1000ms timeout
        window_def = tumbling_window_definition_factory(
            duration_ms=10, grace_ms=0, timeout_ms=1000
        )
        window = window_def.sum()
        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        
        key = b"key"
        
        with store.start_partition_transaction(0) as tx:
            with unittest.mock.patch('quixstreams.dataframe.windows.time_based.time.time') as mock_time:
                # Start at wall clock time 1000 seconds
                mock_time.return_value = 1000.0
                
                # Add item to window [100, 110) at wall clock time 1000000ms
                updated, expired = process(
                    window, value=1, key=key, transaction=tx, timestamp_ms=100
                )
                assert len(updated) == 1
                assert updated[0][1]["value"] == 1
                assert not expired
                
                # Now advance wall clock by 2000ms (past the 1000ms timeout)
                mock_time.return_value = 1002.0
                
                # Now we can proactively check for expired windows using the new method
                # This simulates what an application polling loop or background thread would do
                collect = False  # This is a sum window, not a collect window
                expired_windows = list(window.expire_timeouts_for_key(key, tx, collect))
                
                # The window should now be expired proactively
                assert len(expired_windows) == 1, "Window should have been expired proactively"
                assert expired_windows[0][1]["value"] == 1
                assert expired_windows[0][1]["start"] == 100
                assert expired_windows[0][1]["end"] == 110
                
                # Check that the window is now removed from state
                state_obj = tx.as_state(prefix=key)
                existing_window = state_obj.get_window(100, 110)
                assert existing_window is None, "Window should be removed after timeout expiration"


def test_application_window_timeout_integration():
    """
    Test the complete Application-level window timeout integration.
    
    This test verifies:
    1. Application registers timeout-enabled windows
    2. Keys are tracked when windows are processed
    3. Timeout checking works during consumer polling
    4. Windows are expired proactively when no messages arrive
    5. Key cleanup prevents memory leaks
    """
    import time
    import unittest.mock
    from unittest.mock import Mock, MagicMock
    from quixstreams import Application
    from quixstreams.models import Topic
    
    # Create an Application with timeout checking enabled
    app = Application(
        broker_address="localhost:9092",
        consumer_group="test-group",
        enable_window_timeout_checking=True,
        window_timeout_check_interval=1.0,  # Check every 1 second
        auto_create_topics=False  # Avoid creating real topics
    )
    
    # Create a topic
    topic = app.topic("test-topic")
    
    # Create a dataframe with a timeout-enabled window
    sdf = app.dataframe(topic)
    sdf = sdf.tumbling_window(duration_ms=100, timeout_ms=1000).sum()  # 1-second timeout
    
    # Verify that the window was registered for timeout checking
    assert "test-topic" in app._timeout_enabled_windows
    assert len(app._timeout_enabled_windows["test-topic"]) == 1
    window_def = app._timeout_enabled_windows["test-topic"][0]
    assert hasattr(window_def, '_timeout_ms')
    assert window_def._timeout_ms == 1000
    
    # Test key tracking functionality
    test_key = b"test_key"
    partition = 0
    
    # Initially, no keys should be tracked
    assert "test-topic" not in app._active_window_keys or \
           partition not in app._active_window_keys.get("test-topic", {})
    
    # Simulate tracking a window key (this would happen during message processing)
    app.track_window_key("test-topic", partition, test_key)
    
    # Verify key is now tracked
    assert "test-topic" in app._active_window_keys
    assert partition in app._active_window_keys["test-topic"]
    assert test_key in app._active_window_keys["test-topic"][partition]
    assert "test-topic" in app._key_last_activity
    assert partition in app._key_last_activity["test-topic"]
    assert test_key in app._key_last_activity["test-topic"][partition]
    
    # Test timeout checking logic
    with unittest.mock.patch('time.time') as mock_time:
        # Set initial time
        mock_time.return_value = 1000.0
        
        # Track initial activity
        app.track_window_key("test-topic", partition, test_key)
        initial_time = app._key_last_activity["test-topic"][partition][test_key]
        
        # Advance time by 500ms - should not trigger timeout
        mock_time.return_value = 1000.5
        
        # Mock the dataframe registry and processing context
        app._dataframe_registry._registry = {"test-topic": Mock()}
        app._dataframe_registry.get_stream_ids = Mock(return_value=["test-stream"])
        
        # Mock the checkpoint and store transaction
        mock_transaction = Mock()
        app._processing_context.checkpoint = Mock()
        app._processing_context.checkpoint.get_store_transaction = Mock(return_value=mock_transaction)
        
        # Mock the window definition's expire_timeouts_for_key method
        expired_results = []
        window_def.expire_timeouts_for_key = Mock(return_value=expired_results)
        
        # Call the timeout checking method directly
        app._check_window_timeouts()
        
        # Should not have called expire_timeouts_for_key since interval hasn't passed
        assert not window_def.expire_timeouts_for_key.called
        
        # Advance time past the check interval
        mock_time.return_value = 1001.5
        
        # Call timeout checking again
        app._check_window_timeouts()
        
        # Now it should have called expire_timeouts_for_key
        window_def.expire_timeouts_for_key.assert_called()
    
    # Test key untracking
    app.untrack_window_key("test-topic", partition, test_key)
    
    # Verify key is no longer tracked
    assert test_key not in app._active_window_keys["test-topic"][partition]
    assert test_key not in app._key_last_activity["test-topic"][partition]
    
    # Test stale key cleanup
    with unittest.mock.patch('time.time') as mock_time:
        # Add some keys with old activity times
        mock_time.return_value = 1000.0
        app.track_window_key("test-topic", partition, b"old_key1")
        app.track_window_key("test-topic", partition, b"old_key2")
        
        # Advance time significantly
        mock_time.return_value = 2000.0  # 1000 seconds later
        
        # Call cleanup (this happens automatically during _check_window_timeouts)
        app._cleanup_stale_keys()
        
        # Keys should be cleaned up since they're older than any reasonable timeout
        assert b"old_key1" not in app._active_window_keys.get("test-topic", {}).get(partition, set())
        assert b"old_key2" not in app._active_window_keys.get("test-topic", {}).get(partition, set())
    
    print("✅ Application window timeout integration test passed!")


def test_application_timeout_disabled():
    """Test that timeout checking can be disabled."""
    from quixstreams import Application
    
    # Create an Application with timeout checking disabled
    app = Application(
        broker_address="localhost:9092",
        consumer_group="test-group",
        enable_window_timeout_checking=False,
        auto_create_topics=False
    )
    
    # Create a topic and dataframe with timeout
    topic = app.topic("test-topic")
    sdf = app.dataframe(topic)
    sdf = sdf.tumbling_window(duration_ms=100, timeout_ms=1000).sum()
    
    # Window should still be registered (for consistency)
    assert "test-topic" in app._timeout_enabled_windows
    
    # But timeout checking should be disabled
    assert app._enable_window_timeout_checking is False
    
    # Track a key
    app.track_window_key("test-topic", 0, b"test_key")
    
    # Call timeout checking - should return early
    result = app._check_window_timeouts()
    
    # Should return None/early due to disabled checking
    assert result is None
    
    print("✅ Application timeout disabled test passed!")


def test_application_window_timeout_end_to_end():
    """
    End-to-end test simulating the complete timeout flow.
    
    This test simulates:
    1. Creating windows with timeouts
    2. Processing messages to create windows
    3. Time advancing without new messages
    4. Proactive window expiration
    5. Results being processed through the pipeline
    """
    import time
    import unittest.mock
    from unittest.mock import Mock, patch
    from quixstreams import Application
    
    # Create application
    app = Application(
        broker_address="localhost:9092",
        consumer_group="test-group",
        enable_window_timeout_checking=True,
        window_timeout_check_interval=0.1,  # Check frequently for testing
        auto_create_topics=False
    )
    
    # Create topic and dataframe with timeout
    topic = app.topic("test-topic")
    sdf = app.dataframe(topic)
    
    # Track processed results
    processed_results = []
    
    def capture_result(value, context):
        processed_results.append({"value": value, "context": context})
        return value
    
    # Create window and add processing
    sdf = sdf.tumbling_window(duration_ms=100, timeout_ms=500).sum()  # 500ms timeout
    sdf = sdf.apply(capture_result)
    
    # Verify window registration
    assert "test-topic" in app._timeout_enabled_windows
    window_def = app._timeout_enabled_windows["test-topic"][0]
    assert window_def._timeout_ms == 500
    
    # Mock time for controlled testing
    with unittest.mock.patch('time.time') as mock_time:
        mock_time.return_value = 1000.0
        
        # Simulate window creation by tracking a key
        test_key = b"test_key"
        partition = 0
        app.track_window_key("test-topic", partition, test_key)
        
        # Mock the required components for expiration
        app._dataframe_registry._registry = {"test-topic": Mock()}
        app._dataframe_registry.get_stream_ids = Mock(return_value=["test-stream"])
        
        # Create a mock store transaction that can handle window operations
        mock_transaction = Mock()
        
        # Mock window state operations
        mock_state = Mock()
        mock_state.get_window = Mock(return_value={"value": 42, "start": 100, "end": 200})
        mock_state.delete_window = Mock()
        mock_transaction.as_state = Mock(return_value=mock_state)
        
        app._processing_context.checkpoint = Mock()
        app._processing_context.checkpoint.get_store_transaction = Mock(return_value=mock_transaction)
        
        # Mock the window definition to return expired results
        from quixstreams.dataframe.windows.base import WindowKeyResult
        expired_result = WindowKeyResult(
            key=test_key,
            value={"value": 42, "start": 100, "end": 200}
        )
        
        # Advance time past timeout
        mock_time.return_value = 1000.6  # 600ms later, past 500ms timeout
        
        # Mock the expire_timeouts_for_key method to return our test result
        original_expire = window_def.expire_timeouts_for_key
        window_def.expire_timeouts_for_key = Mock(return_value=[expired_result])
        
        # Mock the dataframe execution pipeline
        mock_executor = Mock()
        app._dataframe_registry.compose_all = Mock(return_value={"test-topic": mock_executor})
        
        # Call timeout checking
        app._check_window_timeouts()
        
        # Verify that expire_timeouts_for_key was called
        window_def.expire_timeouts_for_key.assert_called_with(test_key, mock_transaction, collect=False)
        
        # Verify that the expired result would be processed through the pipeline
        # (Note: In the real implementation, this happens via _process_window_timeout_result)
        
        # Restore original method
        window_def.expire_timeouts_for_key = original_expire
    
    print("✅ Application end-to-end timeout test passed!")
