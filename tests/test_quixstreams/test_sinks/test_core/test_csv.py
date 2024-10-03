import csv
import json

from quixstreams.sinks.core.csv import CSVSink


class TestCSVSink:
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
        sink.flush(topic=topic, partition=0)
        sink.flush(topic=topic, partition=1)

        with open(path) as f:
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
