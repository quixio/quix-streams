import uuid
from typing import Optional

import pytest

from quixstreams.error_callbacks import ConsumerErrorCallback
from quixstreams.kafka import AutoOffsetReset, Consumer
from quixstreams.rowconsumer import RowConsumer


@pytest.fixture()
def random_consumer_group() -> str:
    return str(uuid.uuid4())


@pytest.fixture()
def consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
    ) -> Consumer:
        consumer_group = consumer_group or random_consumer_group
        extras = {
            # Make consumers to refresh cluster metadata often
            # to react on re-assignment changes faster
            "topic.metadata.refresh.interval.ms": 3000,
            # Keep rebalances as simple as possible for testing
            "partition.assignment.strategy": "range",
        }
        extras.update((extra_config or {}))

        return Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extras,
        )

    return factory


@pytest.fixture()
def consumer(consumer_factory) -> Consumer:
    return consumer_factory()


@pytest.fixture()
def row_consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
        on_error: Optional[ConsumerErrorCallback] = None,
    ) -> RowConsumer:
        extra_config = extra_config or {}
        consumer_group = consumer_group or random_consumer_group

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000
        return RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
            on_error=on_error,
        )

    return factory
