import pytest

from src.quixstreams.dataframes.models.rows import Row
from src.quixstreams.dataframes.models.topics import Topic
from src.quixstreams.dataframes.models.timestamps import MessageTimestamp, TimestampType
from src.quixstreams.dataframes.dataframe.pipeline import Pipeline, PipelineFunction
from src.quixstreams.dataframes.dataframe.dataframe import StreamingDataFrame
from copy import deepcopy
from functools import partial


@pytest.fixture()
def row_msg_value_factory():
    def _row_msg_value_factory(message_value):
        return Row(
            value=message_value,
            topic='test_topic',
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp(1234567890, TimestampType(1))
        )
    return _row_msg_value_factory


@pytest.fixture()
def pipeline_function():
    def test_func(data):
        return {k: v + 1 for k, v in data.items()}
    return PipelineFunction(func=test_func)


@pytest.fixture()
def pipeline(pipeline_function):
    return Pipeline(functions=[pipeline_function])


@pytest.fixture()
def dataframe():
    return StreamingDataFrame(topics=[Topic("test")])


@pytest.fixture()
def more_rows_func():
    def more_rows(row):
        rows_out = []
        for item in row['x_list']:
            row_out = deepcopy(row)
            row_out['x_list'] = item
            rows_out.append(row_out)
        return rows_out
    return more_rows


def row_values_plus_n(n, row):
    for k, v in row.items():
        row[k] = v + n
    return row


@pytest.fixture()
def row_plus_n_func():
    """
    This generally will be used alongside "row_plus_n"
    """
    def _row_values_plus_n(n=None):
        return partial(row_values_plus_n, n)
    return _row_values_plus_n
