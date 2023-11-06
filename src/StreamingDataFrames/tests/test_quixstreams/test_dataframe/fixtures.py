from functools import partial
from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.dataframe.pipeline import Pipeline, PipelineFunction
from quixstreams.models.topics import Topic
from quixstreams.state import StateStoreManager


@pytest.fixture()
def pipeline_function():
    def test_func(data):
        return {k: v + 1 for k, v in data.items()}

    return PipelineFunction(func=test_func)


@pytest.fixture()
def pipeline(pipeline_function):
    return Pipeline(functions=[pipeline_function])


@pytest.fixture()
def dataframe_factory():
    def _dataframe_factory(
        topic: Optional[Topic] = None,
        state_manager: Optional[StateStoreManager] = None,
    ):
        return StreamingDataFrame(
            topic=topic or Topic(name="test_in"),
            state_manager=state_manager or MagicMock(spec=StateStoreManager),
        )

    return _dataframe_factory


def row_values_plus_n(n, row, ctx):
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
