from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.error_callbacks import ProducerErrorCallback
from quixstreams.kafka import Producer
from quixstreams.rowproducer import RowProducer


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


@pytest.fixture
def row_producer_mock(request):
    producer = MagicMock(spec=RowProducer)
    producer.flush.return_value = getattr(request, "param", 0)
    return producer


@pytest.fixture()
def row_producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        extra_config: dict = None,
        on_error: Optional[ProducerErrorCallback] = None,
        transactional: bool = False,
    ) -> RowProducer:
        return RowProducer(
            broker_address=broker_address,
            extra_config=extra_config,
            on_error=on_error,
            transactional=transactional,
        )

    return factory


@pytest.fixture()
def row_producer(row_producer_factory):
    return row_producer_factory()


@pytest.fixture()
def transactional_row_producer(row_producer_factory):
    return row_producer_factory(transactional=True)
