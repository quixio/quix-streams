import pytest


@pytest.fixture
def create_sdf(dataframe_factory, state_manager):
    def _create_sdf(topic):
        return dataframe_factory(topic=topic, state_manager=state_manager)

    return _create_sdf


@pytest.fixture
def assign_partition(state_manager):
    def _assign_partition(sdf):
        state_manager.on_partition_assign(stream_id=sdf.stream_id, partition=0)

    return _assign_partition


@pytest.fixture
def publish(message_context_factory):
    def _publish(sdf, topic, value, key, timestamp, headers=None):
        return sdf.test(
            value=value,
            key=key,
            timestamp=timestamp,
            topic=topic,
            headers=headers,
            ctx=message_context_factory(topic=topic.name),
        )

    return _publish
