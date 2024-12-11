import csv

from quixstreams.sources import CSVSource


class TestCSVSource:
    def test_read(self, tmp_path, row_producer_mock):
        path = tmp_path / "source.csv"
        with open(path, "w") as f:
            writer = csv.DictWriter(
                f, dialect="excel", fieldnames=("key", "field", "timestamp")
            )
            writer.writeheader()
            writer.writerows(
                [
                    {"key": "key1", "field": "value1", "timestamp": 1},
                    {"key": "key2", "field": "value2", "timestamp": 2},
                    {"key": "key3", "field": "value3", "timestamp": 3},
                    {"key": "key4", "field": "value4", "timestamp": 4},
                    {"key": "key5", "field": "value5", "timestamp": 5},
                ]
            )

        name = "csv"
        source = CSVSource(
            name=name,
            path=path,
            key_extractor=lambda r: r["key"],
            timestamp_extractor=lambda r: int(r["timestamp"]),
        )
        source.configure(source.default_topic(), row_producer_mock)
        source.start()

        assert row_producer_mock.produce.called
        assert row_producer_mock.produce.call_count == 5
        assert row_producer_mock.produce.call_args.kwargs == {
            "buffer_error_max_tries": 3,
            "headers": None,
            "key": b"key5",
            "partition": None,
            "poll_timeout": 5.0,
            "timestamp": 5,
            "topic": name,
            "value": b'{"key":"key5","field":"value5","timestamp":"5"}',
        }

    def test_read_no_extractors(self, tmp_path, row_producer_mock):
        path = tmp_path / "source.csv"
        with open(path, "w") as f:
            writer = csv.DictWriter(
                f, dialect="excel", fieldnames=("key", "field", "timestamp")
            )
            writer.writeheader()
            writer.writerows(
                [
                    {"key": "key1", "field": "value1", "timestamp": 1},
                    {"key": "key2", "field": "value2", "timestamp": 2},
                    {"key": "key3", "field": "value3", "timestamp": 3},
                    {"key": "key4", "field": "value4", "timestamp": 4},
                    {"key": "key5", "field": "value5", "timestamp": 5},
                ]
            )

        name = "csv"
        source = CSVSource(name="csv", path=path)
        source.configure(source.default_topic(), row_producer_mock)
        source.start()

        assert row_producer_mock.produce.called
        assert row_producer_mock.produce.call_count == 5
        assert row_producer_mock.produce.call_args.kwargs == {
            "buffer_error_max_tries": 3,
            "headers": None,
            "key": None,
            "partition": None,
            "poll_timeout": 5.0,
            "timestamp": None,
            "topic": name,
            "value": b'{"key":"key5","field":"value5","timestamp":"5"}',
        }
