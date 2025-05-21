import pytest

from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.rocksdb.windowed.serialization import append_integer
from quixstreams.state.serialization import encode_integer_pair


@pytest.mark.parametrize("base_bytes", [b"", b"base_bytes"])
def test_append_integer(
    base_bytes: bytes,
):
    start, end = 0, 10
    window_key = base_bytes + SEPARATOR + encode_integer_pair(start, end)
    window_base_bytes = append_integer(base_bytes=base_bytes, integer=start)
    assert window_key.startswith(window_base_bytes)
