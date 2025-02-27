import pytest

from quixstreams.dataframe.windows import (
    TumblingCountWindowDefinition,
    TumblingTimeWindowDefinition,
)
from quixstreams.dataframe.windows.time_based import ClosingStrategy


@pytest.fixture()
def tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(duration_ms: int, grace_ms: int = 0) -> TumblingTimeWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = TumblingTimeWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=sdf
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

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_tumblingwindow_count(
        self, expiration, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.count()
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(topic="test", store_name=window.name)
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

            # Expire key1
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=111
            )

            assert updated == [
                (key1, {"start": 110, "end": 120, "value": 2}),
            ]
            assert expired == [
                (key1, {"start": 100, "end": 110, "value": 4}),
                (key2, {"start": 100, "end": 110, "value": 12}),
            ]

    def test_tumbling_key_expiration_to_partition(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        sdf = dataframe_factory(state_manager=state_manager)
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

    def test_count(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.count()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            process(window, key="", value=2, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1
        assert not expired

    def test_window_expired(
        self,
        count_tumbling_window_definition_factory,
        state_manager,
    ):
        window_def = count_tumbling_window_definition_factory(count=2)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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

    def test_collect(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=3)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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

    def test_multiple_keys_sum(
        self, count_tumbling_window_definition_factory, state_manager
    ):
        window_def = count_tumbling_window_definition_factory(count=3)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
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
        store = state_manager.get_store(topic="test", store_name=window.name)
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
