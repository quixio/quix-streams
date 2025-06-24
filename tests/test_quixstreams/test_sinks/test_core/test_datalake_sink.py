import os
import shutil
import tempfile
from unittest import mock

import pyarrow.parquet as pq
import pytest
from fastavro import reader as avro_reader

from quixstreams.sinks.core.datalake.sink import (
    QuixDatalakeSink,
)


class DummyFS:
    """A dummy fsspec filesystem for local testing."""

    def _path(self, path):
        if path.startswith("dummy://"):
            return path[len("dummy://") :]
        return path

    def open(self, path, mode):
        full_path = self._path(path)
        parent = os.path.dirname(full_path)
        os.makedirs(parent, exist_ok=True)
        return open(full_path, mode)

    def size(self, path):
        return os.path.getsize(self._path(path))


@pytest.fixture
def tmp_dir():
    tmpdir = tempfile.mkdtemp()
    with mock.patch("fsspec.filesystem", return_value=DummyFS()):
        yield tmpdir
    shutil.rmtree(tmpdir)


@pytest.fixture
def sink(tmp_dir):
    """Fixture to create a QuixDatalakeSink instance."""
    return QuixDatalakeSink(storage_url=f"dummy://{tmp_dir}/ws1")


def walk(directory):
    """A simple generator to walk through files in a directory."""

    avro_files = []
    parquet_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".avro.snappy"):
                avro_files.append(os.path.join(root, file))
            elif file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))

    return avro_files, parquet_files


def test_quix_datalake_sink_basic(tmp_dir, sink):
    # Add two records with different keys
    sink.add(b"val1", b"key1", 1000, [], "testtopic", 0, 1)
    sink.add(b"val2", b"key2", 2000, [], "testtopic", 0, 2)
    sink.flush()

    avro_files, parquet_files = walk(os.path.join(tmp_dir, "ws1"))

    # Check Avro content
    assert len(avro_files) == 2
    for avro_path in avro_files:
        with open(avro_path, "rb") as f:
            records = list(avro_reader(f))
            assert records
            assert len(records) == 1

    # There should be one Parquet file per key
    assert len(parquet_files) == 2
    for parquet_path in parquet_files:
        table = pq.read_table(parquet_path)
        # Each Parquet file should have one row (one Avro file per key)
        assert table.num_rows == 1


def test_quix_datalake_sink_multiple_partitions(tmp_dir, sink):
    # Add records to two partitions, same key
    sink.add(b"val1", b"key1", 1000, [], "testtopic", 0, 1)
    sink.add(b"val2", b"key1", 2000, [], "testtopic", 1, 2)
    sink.flush()

    avro_files, parquet_files = walk(os.path.join(tmp_dir, "ws1"))

    assert len(avro_files) == 2
    for avro_path in avro_files:
        with open(avro_path, "rb") as f:
            records = list(avro_reader(f))
            assert records
            assert len(records) == 1

    # There should be one Parquet file per key/partition, each with one row (since only one Avro file per partition)
    assert len(parquet_files) == 2
    for parquet_path in parquet_files:
        table = pq.read_table(parquet_path)
        assert table.num_rows == 1


def test_quix_datalake_sink_empty_flush(tmp_dir, sink):
    # Flush without adding any records should not raise
    sink.flush()
    # No files should be created
    raw_dir = os.path.join(tmp_dir, "ws1/Raw/Topic=testtopic")
    assert not os.path.exists(raw_dir)


def test_quix_datalake_sink_headers(tmp_dir, sink):
    # Add a record with headers
    sink.add(b"val1", b"key1", 1000, [("h1", b"v1"), ("h2", "v2")], "testtopic", 0, 1)
    sink.flush()

    avro_files, _ = walk(os.path.join(tmp_dir, "ws1"))

    assert len(avro_files) == 1
    for avro_path in avro_files:
        with open(avro_path, "rb") as f:
            records = list(avro_reader(f))
            assert records
            assert len(records) == 1
            assert records[0]["headers"] == [
                {"key": "h1", "value": b"v1"},
                {"key": "h2", "value": "v2"},
            ]


def test_quix_datalake_sink_multiple_records(tmp_dir, sink):
    # Add multiple records with the same key and partition, flush after each
    for i in range(5):
        sink.add(f"val{i}".encode(), b"key1", 1000 + i, [], "testtopic", 0, i)
    sink.flush()

    avro_files, parquet_files = walk(os.path.join(tmp_dir, "ws1"))

    # Should be a single Avro file for the key/partition
    assert len(avro_files) == 1
    for avro_path in avro_files:
        with open(avro_path, "rb") as f:
            records = list(avro_reader(f))
            assert len(records) == 5
            for idx, rec in enumerate(records):
                assert rec["value"] == f"val{idx}".encode()

    # There should be one Parquet file for the key/partition, with one row (since only one Avro file)
    assert len(parquet_files) == 1
    for parquet_path in parquet_files:
        table = pq.read_table(parquet_path)
        assert table.num_rows == 1


def test_quix_datalake_sink_multiple_flush(tmp_dir, sink):
    # Add records, flush, add more, flush again (same key/partition)
    sink.add(b"val1", b"key1", 1000, [], "testtopic", 0, 1)
    sink.flush()
    sink.add(b"val2", b"key1", 2000, [], "testtopic", 0, 2)
    sink.flush()

    avro_files, parquet_files = walk(os.path.join(tmp_dir, "ws1"))

    # Should be two Avro files (one per flush)
    assert len(avro_files) == 2
    for avro_path in avro_files:
        with open(avro_path, "rb") as f:
            records = list(avro_reader(f))
            assert len(records) == 1

    # There should be one Parquet file for the key/partition, with two rows (one for each Avro file)
    assert len(parquet_files) == 1
    for parquet_path in parquet_files:
        table = pq.read_table(parquet_path)
        assert table.num_rows == 2


def test_notify_datacatalog_success(tmp_dir):
    """Test that _notify_datacatalog sends a POST and logs success."""
    sink = QuixDatalakeSink(
        storage_url=f"dummy://{tmp_dir}/ws1",
        datacatalog_api_url="http://mocked/api",
        datacatalog_timeout_sec=23,
    )

    class Resp:
        def raise_for_status(self):
            pass

    with mock.patch.object(
        sink._http_session, "post", return_value=Resp()
    ) as mock_post:
        sink.add(b"val1", b"key1", 1000, [], "mytopic", 0, 1)
        sink.add(b"val2", b"key2", 2000, [], "mytopic", 0, 2)
        sink.flush()

        mock_post.assert_called_with(
            "http://mocked/api/mytopic/refresh-cache",
            timeout=23,
        )


def test_notify_datacatalog_failure(tmp_dir, caplog):
    """Test that _notify_datacatalog logs a warning on failure."""
    sink = QuixDatalakeSink(
        storage_url=f"dummy://{tmp_dir}/ws1",
        datacatalog_api_url="http://mocked/api",
    )

    class Resp:
        def raise_for_status(self):
            raise Exception("fail!")

    with mock.patch.object(sink._http_session, "post", return_value=Resp()):
        sink.add(b"val1", b"key1", 1000, [], "mytopic", 0, 1)
        sink.add(b"val2", b"key2", 2000, [], "mytopic", 0, 2)

        with caplog.at_level("WARNING"):
            sink.flush()

    assert any(
        "Data Catalog notification failed" in r for r in caplog.text.splitlines()
    )


def test_notify_datacatalog_called_on_flush(tmp_dir):
    """Test that _notify_datacatalog is called once per topic on flush, after all partitions."""
    sink = QuixDatalakeSink(
        storage_url=f"dummy://{tmp_dir}/ws1",
        datacatalog_api_url="http://mocked/api",
    )

    class Resp:
        def raise_for_status(self):
            pass

    with mock.patch.object(
        sink._http_session, "post", return_value=Resp()
    ) as mock_post:
        # Add records for two topics, each with two partitions
        sink.add(b"val1", b"key1", 1000, [], "topic1", 0, 1)
        sink.add(b"val2", b"key2", 2000, [], "topic1", 1, 2)
        sink.add(b"val3", b"key3", 3000, [], "topic2", 0, 3)
        sink.add(b"val4", b"key4", 4000, [], "topic2", 1, 4)
        sink.flush()

        # Should be called once per topic
        assert mock_post.call_count == 2
        expected_calls = [
            mock.call("http://mocked/api/topic1/refresh-cache", timeout=5),
            mock.call("http://mocked/api/topic2/refresh-cache", timeout=5),
        ]
        for call in expected_calls:
            assert call in mock_post.call_args_list
