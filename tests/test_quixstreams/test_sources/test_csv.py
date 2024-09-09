import csv
import json
import pytest

from unittest.mock import MagicMock

from quixstreams.sources import CSVSource
from quixstreams.rowproducer import RowProducer


class TestCSVSource:

    @pytest.fixture
    def producer(self):
        producer = MagicMock(spec=RowProducer)
        producer.flush.return_value = 0
        return producer

    def test_read(self, tmp_path, producer):
        path = tmp_path / "source.csv"
        with open(path, "w") as f:
            writer = csv.DictWriter(
                f, dialect="excel", fieldnames=("key", "value", "timestamp")
            )
            writer.writeheader()
            writer.writerows(
                [
                    {"key": "key1", "value": json.dumps({"value": "value1"})},
                    {"key": "key2", "value": json.dumps({"value": "value2"})},
                    {"key": "key3", "value": json.dumps({"value": "value3"})},
                    {"key": "key4", "value": json.dumps({"value": "value4"})},
                    {
                        "key": "key5",
                        "value": json.dumps({"value": "value5"}),
                        "timestamp": 10000,
                    },
                ]
            )

        source = CSVSource(path)
        source.configure(source.default_topic(), producer)
        source.start()

        assert producer.produce.called
        assert producer.produce.call_count == 5
        assert producer.produce.call_args.kwargs == {
            "buffer_error_max_tries": 3,
            "headers": None,
            "key": b"key5",
            "partition": None,
            "poll_timeout": 5.0,
            "timestamp": 10000,
            "topic": path,
            "value": b'{"value":"value5"}',
        }

    def test_read_no_timestamp(self, tmp_path, producer):
        path = tmp_path / "source.csv"
        with open(path, "w") as f:
            writer = csv.DictWriter(f, dialect="excel", fieldnames=("key", "value"))
            writer.writeheader()
            writer.writerows(
                [
                    {"key": "key1", "value": json.dumps({"value": "value1"})},
                    {"key": "key2", "value": json.dumps({"value": "value2"})},
                    {"key": "key3", "value": json.dumps({"value": "value3"})},
                    {"key": "key4", "value": json.dumps({"value": "value4"})},
                    {"key": "key5", "value": json.dumps({"value": "value5"})},
                ]
            )

        source = CSVSource(path)
        source.configure(source.default_topic(), producer)
        source.start()

        assert producer.produce.called
        assert producer.produce.call_count == 5
        assert producer.produce.call_args.kwargs == {
            "buffer_error_max_tries": 3,
            "headers": None,
            "key": b"key5",
            "partition": None,
            "poll_timeout": 5.0,
            "timestamp": None,
            "topic": path,
            "value": b'{"value":"value5"}',
        }
