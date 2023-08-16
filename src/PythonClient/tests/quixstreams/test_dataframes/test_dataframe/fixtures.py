import pytest

from quixstreams.dataframes.models.rows import Row
from quixstreams.dataframes.models.timestamps import MessageTimestamp, TimestampType
from quixstreams.dataframes.dataframe.pipeline import Pipeline, PipelineFunction
from quixstreams.dataframes.dataframe.dataframe import StreamingDataFrame
from quixstreams.dataframes.dataframe.column import Column
from copy import deepcopy


@pytest.fixture()
def message_value():
    return {
        'k': 0,
        'x': 5,
        'x2': 5,
        'y': 20,
        'z': 110
    }


@pytest.fixture()
def message_value_with_list():
    return {
        'x': 5,
        'x_list': [0, 1, 2],
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
def row_with_list(message_value_with_list, message_timestamp):
    return Row(
        value=message_value_with_list,
        topic='test_topic',
        partition=0,
        offset=0,
        size=0,
        timestamp=message_timestamp
    )


@pytest.fixture()
def pipeline_function():
    def test_func(data):
        return {k: v + 1 for k, v in data.items()}
    return PipelineFunction(func=test_func, pipeline_name='test_pipeline')


@pytest.fixture()
def pipeline(pipeline_function):
    return Pipeline(
        name='test_pipeline',
        _functions=[pipeline_function],
        _graph={'test_pipeline_parent': 'test_pipeline'},
        _parent='test_pipeline_parent'
    )


@pytest.fixture()
def column():
    return Column('x')


@pytest.fixture()
def dataframe():
    return StreamingDataFrame(name='test_dataframe')


@pytest.fixture()
def more_rows_func():
    def more_rows(row):
        rows_out = []
        for item in row['x_list']:
            rows_out.append({**row, 'x_list': item})
        return rows_out
    return more_rows


@pytest.fixture()
def row_values_plus_1_func():
    def row_values_plus_1(row):
        for k, v in row.items():
            row[k] = v + 1
        return row
    return row_values_plus_1


@pytest.fixture()
def row_with_values_plus_1(row, row_values_plus_1_func):
    row = deepcopy(row)
    return row_values_plus_1_func(row)