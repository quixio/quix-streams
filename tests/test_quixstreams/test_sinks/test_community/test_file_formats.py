import gzip
import json

import pytest

from quixstreams.sinks.community.file.formats import JSONFormat, ParquetFormat
from quixstreams.sinks.community.file.local import LocalFileSink


@pytest.mark.parametrize(
    "key, expected_key",
    [
        ("customer-42", "customer-42"),
        ("", ""),
        ("München", "München"),
        (b"customer-42", "customer-42"),
        (42, "42"),
        (None, "None"),
    ],
)
@pytest.mark.parametrize("format_name", ["json", "json-gzip", "parquet"])
def test_local_file_sink_serializes_message_key(
    tmp_path, monkeypatch, key, expected_key, format_name
):
    if format_name == "parquet":
        pq = pytest.importorskip("pyarrow.parquet")
        format = ParquetFormat()
    else:
        format = JSONFormat(compress=format_name == "json-gzip")
    monkeypatch.chdir(tmp_path)
    sink = LocalFileSink(directory="data", format=format)
    value = {"amount": 25}
    sink.add(
        value=value,
        key=key,
        timestamp=1700000000000,
        headers=[],
        topic="orders",
        partition=0,
        offset=0,
    )
    sink.flush()

    output = (
        tmp_path
        / "data"
        / "orders"
        / "0"
        / f"0000000000000000000{format.file_extension}"
    )
    if format_name == "parquet":
        rows = pq.read_table(output).to_pylist()
        expected = {"amount": 25}
    else:
        data = output.read_bytes()
        if format_name == "json-gzip":
            data = gzip.decompress(data)
        rows = [json.loads(line) for line in data.splitlines()]
        expected = {"_value": value}
    expected.update(
        _key=expected_key,
        _timestamp=1700000000000,
    )
    assert rows == [expected]
