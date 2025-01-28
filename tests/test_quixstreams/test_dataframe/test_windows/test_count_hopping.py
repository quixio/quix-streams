import pytest

from quixstreams.dataframe.windows import CountHoppingWindowDefinition


@pytest.fixture()
def count_hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(count: int, step: int) -> CountHoppingWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = CountHoppingWindowDefinition(dataframe=sdf, count=count, step=step)
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
            CountHoppingWindowDefinition(
                count=count,
                step=step,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_count(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.count()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert expired == []

            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 2
            assert len(expired) == 1
            assert expired[0]["value"] == 4

            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 1
            assert len(expired) == 0

            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 2
            assert len(expired) == 1
            assert expired[0]["value"] == 4

    def test_sum(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 3  # 1 + 2
            assert expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 6  # 1 + 2 + 3
            assert updated[1]["value"] == 3
            assert expired == []

            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 10  # 1 + 2 + 3 + 4
            assert updated[1]["value"] == 7  # 3 + 4
            assert len(expired) == 1
            assert expired[0]["value"] == 10

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 12  # 3 + 4 + 5
            assert updated[1]["value"] == 5
            assert len(expired) == 0

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 18  # 3 + 4 + 5 + 6
            assert updated[1]["value"] == 11  # 5 + 6
            assert len(expired) == 1
            assert expired[0]["value"] == 18

    def test_mean(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.mean()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1.5  # (1 + 2) / 2
            assert expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2  # (1 + 2 + 3) / 3
            assert updated[1]["value"] == 3
            assert expired == []

            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2.5  # (1 + 2 + 3 + 4) / 4
            assert updated[1]["value"] == 3.5  # 3 + 4
            assert len(expired) == 1
            assert expired[0]["value"] == 2.5

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4  # (3 + 4 + 5) / 3
            assert updated[1]["value"] == 5
            assert len(expired) == 0

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4.5  # (3 + 4 + 5 + 6) / 4
            assert updated[1]["value"] == 5.5  # (5 + 6) / 2
            assert len(expired) == 1
            assert expired[0]["value"] == 4.5

    def test_reduce(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == [1]
            assert expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == [1, 2]
            assert expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [1, 2, 3]
            assert updated[1]["value"] == [3]
            assert expired == []

            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [1, 2, 3, 4]
            assert updated[1]["value"] == [3, 4]
            assert len(expired) == 1
            assert expired[0]["value"] == [1, 2, 3, 4]

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [3, 4, 5]
            assert updated[1]["value"] == [5]
            assert len(expired) == 0

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [3, 4, 5, 6]
            assert updated[1]["value"] == [5, 6]
            assert len(expired) == 1
            assert expired[0]["value"] == [3, 4, 5, 6]

    def test_max(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.max()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert expired == []

            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 4
            assert expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 4
            assert len(expired) == 1
            assert expired[0]["value"] == 4

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 5
            assert updated[1]["value"] == 5
            assert len(expired) == 0

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 6
            assert updated[1]["value"] == 6
            assert len(expired) == 1
            assert expired[0]["value"] == 6

    def test_min(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.min()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 4
            assert expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2
            assert updated[1]["value"] == 3
            assert expired == []

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2
            assert updated[1]["value"] == 3
            assert len(expired) == 1
            assert expired[0]["value"] == 2

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 6
            assert len(expired) == 0

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 5
            assert len(expired) == 1
            assert expired[0]["value"] == 3

    def test_collect(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [1, 2, 3, 4]

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [3, 4, 5, 6]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [5, 6]

    def test_unaligned_steps(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=5, step=2)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=5, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [1, 2, 3, 4, 5]

            updated, expired = window.process_window(
                value=6, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=7, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [3, 4, 5, 6, 7]

            updated, expired = window.process_window(
                value=8, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=9, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [5, 6, 7, 8, 9]

            updated, expired = window.process_window(
                value=10, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                value=11, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [7, 8, 9, 10, 11]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [9, 10, 11]
