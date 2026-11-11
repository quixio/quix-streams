import csv
import json

import pytest

from quixstreams.sinks.core.csv import CSVSink


class TestCSVSink:
    @pytest.mark.parametrize("newline", ["\r\n", "\r", "\n"])
    @pytest.mark.parametrize("delimiter,dialect", [(",", "excel"), ("\t", "excel-tab")])
    def test_write_preserves_newlines(self, tmp_path, newline, delimiter, dialect):
        path = tmp_path / "sink.csv"
        sink = CSVSink(path=str(path), dialect=dialect, value_serializer=str)
        value = f"first{newline}second"

        for offset in (0, 1):
            sink.add(
                value=value,
                key=value,
                timestamp=1,
                headers=[],
                topic="topic",
                partition=0,
                offset=offset,
            )
            sink.flush()

        header = delimiter.join(
            ("key", "value", "timestamp", "topic", "partition", "offset")
        )
        rows = [
            delimiter.join((f'"{value}"', f'"{value}"', "1", "topic", "0", str(offset)))
            for offset in (0, 1)
        ]
        assert path.read_bytes() == ("\r\n".join([header, *rows]) + "\r\n").encode()
        with path.open(newline="") as stream:
            records = list(csv.DictReader(stream, dialect=dialect))
        assert len(records) == 2
        assert all(row["key"] == row["value"] == value for row in records)

    def test_write_success(self, tmp_path):
        path = str(tmp_path / "sink.csv")
        sink = CSVSink(path=path)
        topic = "test-topic"

        value, timestamp = "value", 1
        for partition in (0, 1):
            sink.add(
                value=value,
                key="key",
                timestamp=timestamp,
                headers=[],
                topic=topic,
                partition=partition,
                offset=1,
            )
        sink.flush()

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            lines = list(reader)
        assert len(lines) == 2
        assert lines == [
            {
                "key": "key",
                "value": json.dumps(value),
                "timestamp": str(timestamp),
                "topic": topic,
                "partition": "0",
                "offset": "1",
            },
            {
                "key": "key",
                "value": json.dumps(value),
                "timestamp": str(timestamp),
                "topic": topic,
                "partition": "1",
                "offset": "1",
            },
        ]
