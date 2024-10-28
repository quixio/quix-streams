import pytest

from quixstreams.state.metadata import PREFIX_SEPARATOR
from quixstreams.state.rocksdb.windowed.serialization import (
    encode_window_key,
    encode_window_prefix,
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
def test_encode_window_key(start, end):
    key = encode_window_key(start_ms=start, end_ms=end)
    assert isinstance(key, bytes)

    prefix, decoded_start, decoded_end = parse_window_key(key)
    assert decoded_start == start
    assert decoded_end == end


@pytest.mark.parametrize("prefix", [b"", b"prefix"])
def test_encode_window_prefix(
    prefix: bytes,
):
    start, end = 0, 10
    window_key = (
        prefix + PREFIX_SEPARATOR + encode_window_key(start_ms=start, end_ms=end)
    )
    window_prefix = encode_window_prefix(prefix=prefix, start_ms=start)
    assert window_key.startswith(window_prefix)
