import pytest

from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows import (
    HoppingTimeWindowDefinition,
    TumblingTimeWindowDefinition,
)


@pytest.fixture()
def tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(duration_ms: int, grace_ms: int = 0) -> TumblingTimeWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        return TumblingTimeWindowDefinition(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=sdf,
        )

    return factory


@pytest.fixture()
def hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int, step_ms: int, grace_ms: int = 0
    ) -> HoppingTimeWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        return HoppingTimeWindowDefinition(
            duration_ms=duration_ms,
            step_ms=step_ms,
            grace_ms=grace_ms,
            dataframe=sdf,
        )

    return factory


def process(window, value, key, transaction, timestamp_ms):
    updated, expired = window.process_window(
        value=value,
        key=key,
        timestamp_ms=timestamp_ms,
        headers=None,
        transaction=transaction,
    )
    return list(updated), list(expired)


class TestPartitionExpiryEmitsMessageKey:
    def test_tumbling_partition_mode_emits_the_message_key(
        self, tumbling_window_definition_factory, state_manager
    ):
        """A tumbling window closed with closing_strategy="partition" must
        emit the original str message key, not the serialized store prefix."""
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy="partition")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = "user-1"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            _, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )

        assert len(expired) == 1
        assert expired[0][0] == key

    def test_tumbling_key_mode_emits_the_message_key(
        self, tumbling_window_definition_factory, state_manager
    ):
        """A tumbling window closed with closing_strategy="key" already emits
        the original str message key, not the serialized store prefix."""
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy="key")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = "user-1"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            _, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )

        assert len(expired) == 1
        assert expired[0][0] == key

    def test_hopping_partition_mode_emits_the_message_key(
        self, hopping_window_definition_factory, state_manager
    ):
        """A hopping window closed with closing_strategy="partition" must
        emit the original str message key, not the serialized store prefix."""
        window_def = hopping_window_definition_factory(
            duration_ms=10, step_ms=5, grace_ms=0
        )
        window = window_def.sum()
        window.final(closing_strategy="partition")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = "user-1"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            _, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )

        assert expired
        assert all(emitted_key == key for emitted_key, _ in expired)

    @pytest.mark.parametrize("closing_strategy", ["key", "partition"])
    def test_bytes_message_key_round_trips_unchanged(
        self, closing_strategy, tumbling_window_definition_factory, state_manager
    ):
        """A bytes message key must be emitted unchanged under both closing
        strategies, since a bytes prefix is stored without serialization."""
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final(closing_strategy=closing_strategy)

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"user-1"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            _, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )

        assert len(expired) == 1
        assert expired[0][0] == key
