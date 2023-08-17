import pytest

from src.quixstreams.dataframes.models.rows import Row
from src.quixstreams.dataframes.models.timestamps import MessageTimestamp, TimestampType
from src.quixstreams.dataframes.dataframe.pipeline import Pipeline, PipelineFunction
from src.quixstreams.dataframes.dataframe.dataframe import StreamingDataFrame
from src.quixstreams.dataframes.dataframe.column import Column
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
def msg_value_with_ints():
    return {
        'k': 0,
        'x': 5,
        'x2': 5,
        'y': 20,
        'z': 110
    }


@pytest.fixture()
def msg_value_with_list():
    return {
        'x': 5,
        'x_list': [0, 1, 2],
    }


@pytest.fixture()
def msg_value_with_bools():
    return {
        'x': True,
        'y': True,
        'z': False
    }


@pytest.fixture()
def row_with_ints(row_msg_value_factory, msg_value_with_ints):
    return row_msg_value_factory(msg_value_with_ints)


@pytest.fixture()
def row_with_list(row_msg_value_factory, msg_value_with_list):
    return row_msg_value_factory(msg_value_with_list)


@pytest.fixture()
def row_with_bools(row_msg_value_factory, msg_value_with_bools):
    return row_msg_value_factory(msg_value_with_bools)


@pytest.fixture()
def pipeline_function():
    def test_func(data):
        return {k: v + 1 for k, v in data.items()}
    return PipelineFunction(func=test_func, pipeline_name='test_pipeline')


@pytest.fixture()
def pipeline(pipeline_function):
    return Pipeline(
        name='test_pipeline',
        functions=[pipeline_function],
        graph={'test_pipeline_parent': 'test_pipeline'},
        parent='test_pipeline_parent'
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
    def _row_values_plus_n(n):
        return partial(row_values_plus_n, n)
    return _row_values_plus_n


@pytest.fixture()
def row_plus_n():
    """
    This generally will be used alongside "row_plus_n_func"
    """
    def _row_plus_n(n, row):
        return row_values_plus_n(n, deepcopy(row))
    return _row_plus_n
