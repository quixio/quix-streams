from copy import deepcopy
from functools import partial
from typing import Optional, List
from unittest.mock import MagicMock

import pytest

from streamingdataframes.dataframe.dataframe import StreamingDataFrame
from streamingdataframes.dataframe.pipeline import Pipeline, PipelineFunction
from streamingdataframes.models.topics import Topic
from streamingdataframes.state import StateStoreManager


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
        topics_in: Optional[List[Topic]] = None,
        state_manager: Optional[StateStoreManager] = None,
    ):
        return StreamingDataFrame(
            topics_in=topics_in or [Topic(name="test_in")],
            state_manager=state_manager or MagicMock(spec=StateStoreManager),
        )

    return _dataframe_factory


@pytest.fixture()
def more_rows_func():
    def more_rows(row):
        rows_out = []
        for item in row["x_list"]:
            row_out = deepcopy(row)
            row_out["x_list"] = item
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
