from datetime import timedelta
from typing import Union

import pytest

from quixstreams.dataframe.utils import ensure_milliseconds


@pytest.mark.parametrize(
    "delta, milliseconds",
    [
        (timedelta(seconds=10), 10_000),
        (timedelta(seconds=10, milliseconds=10), 10_010),
        (timedelta(seconds=10, milliseconds=10.1), 10_010),
        (timedelta(seconds=10, milliseconds=10.9), 10_011),
        (timedelta(seconds=10, microseconds=10), 10_000),
        (timedelta(seconds=10, microseconds=1000), 10_001),
        (timedelta(seconds=10, microseconds=10000), 10_010),
        (timedelta(seconds=10, microseconds=0.1), 10_000),
        (timedelta(seconds=10, microseconds=0.1), 10_000),
        (1000, 1000),
    ],
)
def test_ensure_milliseconds(delta: Union[timedelta, int], milliseconds: int):
    result = ensure_milliseconds(delta)
    assert isinstance(result, int)
    assert result == milliseconds


def test_ensure_milliseconds_invalid_type():
    with pytest.raises(TypeError):
        ensure_milliseconds(1.1)
