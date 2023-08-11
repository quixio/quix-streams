import pytest

from quixstreams.dataframes.models.rows import Row
from quixstreams.dataframes.models.timestamps import MessageTimestamp, TimestampType
from quixstreams.dataframes.sdf.pipeline import Pipeline


@pytest.fixture()
def message_value():
    return {
        'x': 5,
        'x2': 5,
        'y': 20,
        'z': 110
    }


@pytest.fixture()
def message_timestamp():
    return MessageTimestamp(1234567890, TimestampType(1))


@pytest.fixture()
def row(message_value, message_timestamp):
    return Row(
        value=message_value,
        topic='test_topic',
        partition=0,
        offset=0,
        size=0,
        timestamp=message_timestamp
    )


@pytest.fixture()
def pipeline():
    return Pipeline()