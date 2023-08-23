import pytest

from src.quixstreams.dataframes.kafka import AutoOffsetReset, Consumer, Producer


@pytest.fixture()
def consumer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: str = "tests",
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
    ) -> Consumer:
        extra_config = extra_config or {}

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000

        return Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
        )

    return factory


@pytest.fixture()
def consumer(consumer_factory) -> Consumer:
    return consumer_factory()


@pytest.fixture()
def producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        extra_config: dict = None,
    ) -> Producer:
        extra_config = extra_config or {}

        return Producer(
            broker_address=broker_address,
            extra_config=extra_config,
        )

    return factory


@pytest.fixture()
def producer(producer_factory) -> Producer:
    return producer_factory()
