from typing import Iterable, Optional
from unittest.mock import MagicMock

import pytest
from influxdb_client_3 import WritePrecision

from quixstreams.sinks.core.influxdb3 import InfluxDB3Sink


@pytest.fixture()
def influxdb3_sink_factory():
    def factory(
        client_mock: MagicMock,
        measurement: str,
        fields_keys: Iterable[str] = (),
        tags_keys: Iterable[str] = (),
        time_key: Optional[str] = None,
        batch_size: int = 1000,
        time_precision: WritePrecision = WritePrecision.MS,
        include_metadata_tags: bool = False,
    ) -> InfluxDB3Sink:
        sink = InfluxDB3Sink(
            token="",
            host="",
            organization_id="test",
            database="test",
            measurement=measurement,
            fields_keys=fields_keys,
            tags_keys=tags_keys,
            time_key=time_key,
            time_precision=time_precision,
            include_metadata_tags=include_metadata_tags,
            batch_size=batch_size,
        )
        sink._client = client_mock
        return sink

    return factory
