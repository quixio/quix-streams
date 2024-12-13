import pytest

from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.rocksdb.windowed.serialization import (
    append_integer,
    encode_integer_pair,
    parse_window_key,
)


@pytest.mark.parametrize(
    "start, end",
    [
        (0, 0),
        (1, 2),
        (-1, 2),
        (2, -2),
    ],
)
def test_encode_integer_pair(start, end):
    key = encode_integer_pair(start, end)
    assert isinstance(key, bytes)

    prefix, decoded_start, decoded_end = parse_window_key(key)
    assert decoded_start == start
    assert decoded_end == end


@pytest.mark.parametrize("base_bytes", [b"", b"base_bytes"])
def test_append_integer(
    base_bytes: bytes,
):
    start, end = 0, 10
    window_key = base_bytes + SEPARATOR + encode_integer_pair(start, end)
    window_base_bytes = append_integer(base_bytes=base_bytes, integer=start)
    assert window_key.startswith(window_base_bytes)
