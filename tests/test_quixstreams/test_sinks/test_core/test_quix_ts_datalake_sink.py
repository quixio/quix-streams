"""
Tests for QuixTSDataLakeSink

Comprehensive unit and integration tests for the Quix Lake Blob Storage Sink,
covering initialization, timestamp mapping, partition handling, write operations,
catalog integration, and error handling.
"""

import io
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

# Mock quixportal before importing the sink modules
sys.modules["quixportal"] = MagicMock()
sys.modules["quixportal.storage"] = MagicMock()
sys.modules["quixportal.storage.config"] = MagicMock()

from quixstreams.sinks.base import SinkBatch
from quixstreams.sinks.core._blob_storage_client import BlobStorageClient
from quixstreams.sinks.core._quix_ts_datalake_catalog_client import (
    QuixTSDataLakeCatalogClient,
)
from quixstreams.sinks.core.quix_ts_datalake_sink import (
    QuixTSDataLakeSink,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_blob_client():
    """Mock BlobStorageClient for unit tests."""
    client = MagicMock(spec=BlobStorageClient)
    client.ensure_path_exists.return_value = True
    client.list_objects.return_value = []

    # Mock async upload
    future_mock = MagicMock()
    future_mock.result.return_value = None
    client.put_object_async.return_value = future_mock

    return client


@pytest.fixture
def mock_catalog_client():
    """Mock QuixTSDataLakeCatalogClient for unit tests."""
    client = MagicMock(spec=QuixTSDataLakeCatalogClient)

    # Health check response
    health_response = MagicMock()
    health_response.status_code = 200
    health_response.raise_for_status = MagicMock()

    # Table check response (404 = table doesn't exist)
    table_check_response = MagicMock()
    table_check_response.status_code = 404

    # Table create response. The body matters: the sink reads it to notice that
    # another writer won the race and the catalog kept ITS location.
    table_create_response = MagicMock()
    table_create_response.status_code = 201
    table_create_response.json.return_value = {
        "name": "test_table",
        "location": "s3://test-bucket/test-prefix/test_table",
        "properties": {},
    }

    # Manifest add response
    manifest_response = MagicMock()
    manifest_response.status_code = 200

    client.get.side_effect = lambda path, **kwargs: (
        health_response if "/health" in path else table_check_response
    )
    client.put.return_value = table_create_response
    client.post.return_value = manifest_response

    return client


@pytest.fixture
def mock_query_api_client():
    client = MagicMock(spec=QuixTSDataLakeCatalogClient)
    accepted = MagicMock()
    accepted.status_code = 202
    accepted.text = '{"subscribers": 1}'
    client.post.return_value = accepted
    return client


@pytest.fixture
def sink_factory(mock_blob_client):
    """Factory to create QuixTSDataLakeSink with mocked blob client."""

    def create(
        s3_prefix: str = "test-prefix",
        table_name: str = "test_table",
        workspace_id: str = "",
        hive_columns: List[str] = None,
        timestamp_column: str = "ts_ms",
        catalog_url: str = None,
        catalog_auth_token: str = None,
        auto_discover: bool = True,
        namespace: str = "default",
        **kwargs,
    ) -> QuixTSDataLakeSink:
        with patch(
            "quixstreams.sinks.core.quix_ts_datalake_sink.get_bucket_name",
            return_value="test-bucket",
        ):
            sink = QuixTSDataLakeSink(
                s3_prefix=s3_prefix,
                table_name=table_name,
                workspace_id=workspace_id,
                hive_columns=hive_columns,
                timestamp_column=timestamp_column,
                catalog_url=catalog_url,
                catalog_auth_token=catalog_auth_token,
                auto_discover=auto_discover,
                namespace=namespace,
                **kwargs,
            )
            # Inject mocked blob client
            sink._blob_client = mock_blob_client
            sink._s3_bucket = "test-bucket"
            return sink

    return create


@pytest.fixture
def sample_batch():
    """Create a sample SinkBatch for testing."""

    def create(
        topic: str = "test-topic",
        partition: int = 0,
        records: List[Dict[str, Any]] = None,
    ) -> SinkBatch:
        if records is None:
            records = [
                {
                    "value": {
                        "field1": "value1",
                        "field2": 100,
                        "ts_ms": 1704067200000,
                    },
                    "key": "key1",
                    "timestamp": 1704067200000,
                    "offset": 0,
                },
                {
                    "value": {
                        "field1": "value2",
                        "field2": 200,
                        "ts_ms": 1704067260000,
                    },
                    "key": "key2",
                    "timestamp": 1704067260000,
                    "offset": 1,
                },
            ]

        batch = SinkBatch(topic=topic, partition=partition)
        for record in records:
            batch.append(
                value=record["value"],
                key=record["key"],
                timestamp=record["timestamp"],
                headers=[],
                offset=record["offset"],
            )
        return batch

    return create


# =============================================================================
# Parquet row-group sizing
# =============================================================================


class TestRowGroupSizing:
    """Row groups are capped at a fixed, configurable row count (default
    1,000,000 — the lakehouse compaction's default). A reader pays about one
    range request per row group, so this bounds the request count of every
    query over sink-written files; small flushes stay a single group."""

    @staticmethod
    def _data_file_bytes(mock_blob_client) -> bytes:
        """Bytes of the last DATA file upload (skips any .vidx sidecar)."""
        calls = [
            c
            for c in mock_blob_client.put_object_async.call_args_list
            if "/.vidx/" not in c[0][0]
        ]
        return calls[-1][0][1]

    def test_default_matches_lakehouse_compaction(self, sink_factory):
        assert sink_factory()._row_group_rows == 1_000_000
        assert QuixTSDataLakeSink.ROW_GROUP_ROWS_DEFAULT == 1_000_000
        assert sink_factory(row_group_rows=250_000)._row_group_rows == 250_000

    def test_rejects_non_positive(self, sink_factory):
        with pytest.raises(ValueError):
            sink_factory(row_group_rows=0)

    def test_small_files_are_one_row_group(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        sink = sink_factory()
        sink.write(sample_batch())
        parquet_bytes = self._data_file_bytes(mock_blob_client)
        assert pq.ParquetFile(io.BytesIO(parquet_bytes)).num_row_groups == 1

    def test_large_flush_is_split_at_the_configured_rows(
        self, sink_factory, mock_blob_client
    ):
        sink = sink_factory(row_group_rows=100_000)
        n = 250_000
        table = pa.table({"ts_ms": list(range(n)), "v": [1.5] * n, "__key": ["k"] * n})
        sink._write_parquet_to_storage(table, "p/data.parquet", [], ())
        meta = pq.ParquetFile(
            io.BytesIO(self._data_file_bytes(mock_blob_client))
        ).metadata
        assert meta.num_row_groups == 3
        assert [meta.row_group(i).num_rows for i in range(3)] == [
            100_000,
            100_000,
            50_000,
        ]
        pending = sink._pending_futures[-1]
        # The catalog entry is per FILE, unaffected by row-group splitting.
        assert pending["row_count"] == n

    def test_default_keeps_a_million_row_flush_in_one_group(
        self, sink_factory, mock_blob_client
    ):
        sink = sink_factory()
        n = 1_000_000
        table = pa.table({"ts_ms": list(range(n)), "__key": ["k"] * n})
        sink._write_parquet_to_storage(table, "p/data.parquet", [], ())
        parquet_bytes = self._data_file_bytes(mock_blob_client)
        assert pq.ParquetFile(io.BytesIO(parquet_bytes)).num_row_groups == 1


# =============================================================================
# 1. Initialization Tests
# =============================================================================


class TestQuixTSDataLakeSinkInit:
    """Tests for sink initialization and configuration."""

    def test_init_minimal_params(self):
        """Test initialization with only required parameters."""
        sink = QuixTSDataLakeSink(
            s3_prefix="test-prefix",
            table_name="test_table",
        )
        assert sink.s3_prefix == "test-prefix"
        assert sink.table_name == "test_table"
        assert sink.workspace_id == ""
        assert sink.hive_columns == []
        assert sink.timestamp_column == "ts_ms"
        assert sink._catalog is None
        assert sink.auto_discover is True
        assert sink.namespace == "default"

    def test_init_all_params(self):
        """Test initialization with all parameters provided."""
        sink = QuixTSDataLakeSink(
            s3_prefix="data/prefix",
            table_name="events",
            workspace_id="ws-123",
            hive_columns=["year", "month", "day"],
            timestamp_column="event_time",
            sort_column="seq",
            catalog_url="http://catalog:8080",
            catalog_auth_token="token123",
            auto_discover=False,
            namespace="production",
            auto_create_bucket=False,
            max_workers=20,
        )
        assert sink.s3_prefix == "data/prefix"
        assert sink.table_name == "events"
        assert sink.workspace_id == "ws-123"
        assert sink.hive_columns == ["year", "month", "day"]
        assert sink.timestamp_column == "event_time"
        assert sink.sort_column == "seq"
        assert sink._catalog is not None
        assert sink.auto_discover is False
        assert sink.namespace == "production"
        assert sink._auto_create_bucket is False
        assert sink._max_workers == 20

    def test_init_without_query_api_url_has_no_query_api_client(self):
        sink = QuixTSDataLakeSink(s3_prefix="test-prefix", table_name="test_table")

        assert sink._query_api is None

    def test_init_with_query_api_url_builds_a_bearer_client(self):
        sink = QuixTSDataLakeSink(
            s3_prefix="test-prefix",
            table_name="test_table",
            query_api_url="http://lake-api:80/",
            query_api_auth_token="secret-token",
        )

        assert isinstance(sink._query_api, QuixTSDataLakeCatalogClient)
        assert sink._query_api.base_url == "http://lake-api:80"
        assert (
            sink._query_api._session.headers["Authorization"] == "Bearer secret-token"
        )

    def test_init_with_query_api_url_and_no_token_sends_no_authorization(self):
        sink = QuixTSDataLakeSink(
            s3_prefix="test-prefix",
            table_name="test_table",
            query_api_url="http://lake-api:80",
        )

        assert sink._query_api is not None
        assert "Authorization" not in sink._query_api._session.headers

    def test_hive_columns_defaults_to_empty_list(self):
        """Test that hive_columns=None becomes empty list."""
        sink = QuixTSDataLakeSink(
            s3_prefix="prefix",
            table_name="table",
            hive_columns=None,
        )
        assert sink.hive_columns == []
        assert isinstance(sink.hive_columns, list)

    def test_ts_hive_columns_extraction(self):
        """Test that only time-based hive columns are tracked in _ts_hive_columns."""
        sink = QuixTSDataLakeSink(
            s3_prefix="prefix",
            table_name="table",
            hive_columns=["year", "month", "custom_col", "hour"],
        )
        # Should only include year, month, day, hour - not custom_col
        assert sink._ts_hive_columns == {"year", "month", "hour"}

    def test_s3_bucket_property_raises_before_setup(self):
        """Test that accessing s3_bucket before setup raises RuntimeError."""
        sink = QuixTSDataLakeSink(
            s3_prefix="prefix",
            table_name="table",
        )
        with pytest.raises(RuntimeError, match="s3_bucket not initialized"):
            _ = sink.s3_bucket

    def test_silence_azure_http_logs_defaults_to_true(self):
        """The chatty-log mute should be on by default — Azure SDK + adlfs
        log one INFO record per HTTP round-trip, which is pure noise for a
        sink that probes a deep partition tree."""
        sink = QuixTSDataLakeSink(s3_prefix="p", table_name="t")
        assert sink._silence_azure_http_logs is True

    def test_silence_azure_http_logs_can_be_disabled(self):
        sink = QuixTSDataLakeSink(
            s3_prefix="p", table_name="t", silence_azure_http_logs=False
        )
        assert sink._silence_azure_http_logs is False


# =============================================================================
# 1b. Chatty Logger Silencing
# =============================================================================


class TestSilenceChattyLoggers:
    """Tests for the silence_chatty_loggers() helper and its integration
    with sink.setup()."""

    # Names that the helper is responsible for muting. Kept in sync with
    # _CHATTY_HTTP_LOGGERS in quix_ts_datalake_sink.py.
    _SILENCED = (
        "azure",
        "azure.core",
        "azure.core.pipeline.policies.http_logging_policy",
        "azure.storage",
        "adlfs",
        "botocore",
        "boto3",
        "s3transfer",
    )

    @pytest.fixture(autouse=True)
    def _reset_logger_levels(self):
        """Reset levels on the silenced loggers before and after each test
        so we don't leak state into the rest of the suite."""
        import logging as _logging

        previous = {name: _logging.getLogger(name).level for name in self._SILENCED}
        for name in self._SILENCED:
            _logging.getLogger(name).setLevel(_logging.NOTSET)
        try:
            yield
        finally:
            for name, level in previous.items():
                _logging.getLogger(name).setLevel(level)

    def test_helper_raises_levels_to_warning(self):
        import logging as _logging

        from quixstreams.sinks.core.quix_ts_datalake_sink import (
            silence_chatty_loggers,
        )

        # Start permissive — everything would otherwise emit INFO.
        for name in self._SILENCED:
            _logging.getLogger(name).setLevel(_logging.INFO)

        silence_chatty_loggers()

        for name in self._SILENCED:
            assert _logging.getLogger(name).level == _logging.WARNING, (
                f"{name} not raised to WARNING"
            )

    def test_setup_silences_when_flag_is_true(self, sink_factory, mock_blob_client):
        import logging as _logging

        sink = sink_factory(silence_azure_http_logs=True)
        for name in self._SILENCED:
            _logging.getLogger(name).setLevel(_logging.INFO)

        with patch(
            "quixstreams.sinks.core.quix_ts_datalake_sink.get_bucket_name",
            return_value="test-bucket",
        ):
            sink.setup()

        for name in self._SILENCED:
            assert _logging.getLogger(name).level == _logging.WARNING

    def test_setup_leaves_logger_levels_alone_when_flag_is_false(
        self, sink_factory, mock_blob_client
    ):
        import logging as _logging

        sink = sink_factory(silence_azure_http_logs=False)
        for name in self._SILENCED:
            _logging.getLogger(name).setLevel(_logging.INFO)

        with patch(
            "quixstreams.sinks.core.quix_ts_datalake_sink.get_bucket_name",
            return_value="test-bucket",
        ):
            sink.setup()

        for name in self._SILENCED:
            assert _logging.getLogger(name).level == _logging.INFO


# =============================================================================
# 2. Timestamp Column Mapping Tests
# =============================================================================


class TestTimestampColumnMapping:
    """Tests for timestamp detection and column extraction."""

    @staticmethod
    def _table(values, column="ts_ms"):
        return pa.table({column: values, "value": list(range(len(values)))})

    @pytest.mark.parametrize(
        "timestamp_value,expected_unit",
        [
            (1704067200, "s"),  # Seconds
            (1704067200000, "ms"),  # Milliseconds
            (1704067200000000, "us"),  # Microseconds
            (1704067200000000000, "ns"),  # Nanoseconds
        ],
    )
    def test_add_timestamp_columns_unit_detection(
        self, sink_factory, timestamp_value, expected_unit
    ):
        """Test automatic detection of timestamp units."""
        sink = sink_factory(hive_columns=["year", "month", "day", "hour"])

        result = sink._add_timestamp_columns(self._table([timestamp_value]))

        # All timestamps resolve to 2024-01-01 00:00:00 UTC
        assert result.column("year")[0].as_py() == "2024"
        assert result.column("month")[0].as_py() == "01"
        assert result.column("day")[0].as_py() == "01"
        assert result.column("hour")[0].as_py() == "00"

    def test_add_timestamp_columns_already_datetime(self, sink_factory):
        """Test that timestamp columns pass through without conversion."""
        sink = sink_factory(hive_columns=["year", "month"])

        dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        result = sink._add_timestamp_columns(self._table([dt]))

        assert result.column("year")[0].as_py() == "2024"
        assert result.column("month")[0].as_py() == "06"

    def test_add_timestamp_columns_parses_iso_strings(self, sink_factory):
        """An ISO-8601 STRING timestamp column is parsed, not rejected. The
        pandas path crashed on it (``float("2024-06-15T14:30:00")``), so a topic
        carrying string timestamps could not be partitioned by time at all."""
        sink = sink_factory(hive_columns=["year", "month", "day", "hour"])

        result = sink._add_timestamp_columns(self._table(["2024-06-15T14:30:00"]))

        assert result.column("year")[0].as_py() == "2024"
        assert result.column("month")[0].as_py() == "06"
        assert result.column("day")[0].as_py() == "15"
        assert result.column("hour")[0].as_py() == "14"

    def test_add_timestamp_columns_tolerates_leading_nulls(self, sink_factory):
        """The unit is detected from the first NON-NULL value; a leading null
        used to make the detection raise (``float(None)``) and fail the flush."""
        sink = sink_factory(hive_columns=["year"])

        result = sink._add_timestamp_columns(self._table([None, 1704067200000]))

        assert result.column("year").to_pylist() == [None, "2024"]

    def test_all_null_timestamp_yields_null_parts(self, sink_factory):
        """An all-null timestamp column has no unit to detect: every part is
        NULL, which the partition key turns into the Hive-NULL bucket."""
        sink = sink_factory(hive_columns=["year"])

        result = sink._add_timestamp_columns(self._table([None, None]))

        assert result.column("year").to_pylist() == [None, None]

    def test_timestamp_column_year_extraction(self, sink_factory):
        """Test year column extraction format."""
        sink = sink_factory(hive_columns=["year"])

        result = sink._add_timestamp_columns(self._table([1704067200000]))

        assert result.column("year")[0].as_py() == "2024"
        assert pa.types.is_string(result.schema.field("year").type)

    @pytest.mark.parametrize(
        "part,expected",
        [("month", "01"), ("day", "01"), ("hour", "00")],
    )
    def test_timestamp_parts_are_zero_padded(self, sink_factory, part, expected):
        """month/day/hour are zero-padded two-digit strings (``month=01``);
        the readers' partition values depend on it."""
        sink = sink_factory(hive_columns=[part])

        result = sink._add_timestamp_columns(self._table([1704067200000]))

        assert result.column(part)[0].as_py() == expected
        assert len(result.column(part)[0].as_py()) == 2

    def test_only_specified_columns_are_added(self, sink_factory):
        """Test that only specified hive columns are added."""
        sink = sink_factory(hive_columns=["year", "day"])  # No month, no hour

        result = sink._add_timestamp_columns(self._table([1704067200000]))

        assert "year" in result.column_names
        assert "day" in result.column_names
        assert "month" not in result.column_names
        assert "hour" not in result.column_names

    def test_parts_are_added_in_configured_order(self, sink_factory):
        """The derived columns are appended in hive_columns order, not in the
        iteration order of a Python set — which varies between processes
        (PYTHONHASHSEED), so two sink pods would otherwise build tables whose
        column order differs."""
        sink = sink_factory(hive_columns=["hour", "year", "month", "day"])

        result = sink._add_timestamp_columns(self._table([1704067200000]))

        assert result.column_names[-4:] == ["hour", "year", "month", "day"]

    def test_add_timestamp_columns_does_not_mutate_timestamp_column_type(
        self, sink_factory
    ):
        """
        Regression: extracting year/month/day/hour for time-based hive
        partitioning must not change the TYPE of the source timestamp
        column. ``ts_ms`` is a system column the sink injects from the
        Kafka ``item.timestamp`` (always int64 ms); its type is part of
        the contract with readers — files written under different
        ``HIVE_COLUMNS`` configurations must store ``ts_ms`` with the same
        type, otherwise downstream readers see the same column as BIGINT
        in some files and TIMESTAMP in others.
        """
        sink = sink_factory(hive_columns=["year", "month", "day", "hour"])

        table = self._table([1704067200000, 1704067260000])
        result = sink._add_timestamp_columns(table)

        # Derived columns still correct.
        assert result.column("year")[0].as_py() == "2024"
        assert result.column("month")[0].as_py() == "01"
        assert result.column("day")[0].as_py() == "01"
        assert result.column("hour")[0].as_py() == "00"

        # Source ts_ms column is untouched — same type, same values.
        assert result.schema.field("ts_ms").type == table.schema.field("ts_ms").type
        assert result.column("ts_ms").to_pylist() == [1704067200000, 1704067260000]

    def test_derived_part_replaces_a_column_of_the_same_name(self, sink_factory):
        """A record that already carries a ``year`` field does not end up with
        two ``year`` columns: the derived partition value replaces it (as the
        DataFrame assignment did), so the folder and the data agree."""
        sink = sink_factory(hive_columns=["year"])

        table = pa.table({"ts_ms": [1704067200000], "year": ["wrong"]})
        result = sink._add_timestamp_columns(table)

        assert result.column_names.count("year") == 1
        assert result.column("year")[0].as_py() == "2024"


# =============================================================================
# 3. Empty Dict Handling Tests
# =============================================================================


class TestEmptyDictHandling:
    """An empty dict cannot be stored in parquet (an empty struct/map has no
    type), so the batch conversion turns one into NULL. This used to be a
    separate DataFrame scan (``_null_empty_dicts``); it now happens while the
    column is being built."""

    @staticmethod
    def _column(sink, sample_batch, values, name="col"):
        batch = sample_batch(
            records=[
                {
                    "value": {"ts_ms": 1704067200000 + i, name: v},
                    "key": f"k{i}",
                    "timestamp": 1704067200000 + i,
                    "offset": i,
                }
                for i, v in enumerate(values)
            ]
        )
        return sink._batch_to_arrow(batch).column(name).to_pylist()

    def test_empty_dicts_become_null(self, sink_factory, sample_batch):
        assert self._column(sink_factory(), sample_batch, [{}, {}, {}]) == [
            None,
            None,
            None,
        ]

    def test_non_empty_dicts_are_preserved(self, sink_factory, sample_batch):
        assert self._column(sink_factory(), sample_batch, [{"a": 1}, {"a": 2}]) == [
            {"a": 1},
            {"a": 2},
        ]

    def test_mixed_empty_and_non_empty(self, sink_factory, sample_batch):
        assert self._column(
            sink_factory(), sample_batch, [{"a": 1}, {}, {"a": 3}, {}]
        ) == [{"a": 1}, None, {"a": 3}, None]

    def test_non_dict_column_unchanged(self, sink_factory, sample_batch):
        assert self._column(sink_factory(), sample_batch, [1, 2, 3]) == [1, 2, 3]

    def test_empty_dict_column_is_writable(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """End of the story: the file writes. An empty struct would raise
        during serialisation."""
        sink = sink_factory()
        sink.write(
            sample_batch(
                records=[
                    {
                        "value": {"ts_ms": 1704067200000, "meta": {}},
                        "key": "k",
                        "timestamp": 1704067200000,
                        "offset": 0,
                    }
                ]
            )
        )
        body = mock_blob_client.put_object_async.call_args_list[-1][0][1]
        assert pq.read_table(io.BytesIO(body)).column("meta").to_pylist() == [None]


# =============================================================================
# 3b. Arrow batch conversion / partitioning
# =============================================================================


class TestArrowBatchConversion:
    """The batch becomes an Arrow table directly — no DataFrame in between.
    These pin the behaviour that changed with it."""

    @staticmethod
    def _records(values):
        return [
            {
                "value": v,
                "key": f"k{i}",
                "timestamp": 1704067200000 + i,
                "offset": i,
            }
            for i, v in enumerate(values)
        ]

    @staticmethod
    def _files(mock_blob_client):
        """{storage key: pyarrow Table} for every DATA file written."""
        return {
            c[0][0]: pq.read_table(io.BytesIO(c[0][1]))
            for c in mock_blob_client.put_object_async.call_args_list
            if "/.vidx/" not in c[0][0]
        }

    def test_partitioned_file_has_no_index_column(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Regression: ``Table.from_pandas`` serialised a partition group's
        (non-range) index as an ``__index_level_0__`` COLUMN, so every file of
        every partitioned table carried a meaningless int64 column — in the
        data, in the footer statistics and in the catalog's zone maps."""
        sink = sink_factory(hive_columns=["machine"])
        sink.write(
            sample_batch(
                records=self._records(
                    [
                        {"ts_ms": 1704067200000, "machine": "A", "v": 1},
                        {"ts_ms": 1704067200001, "machine": "B", "v": 2},
                        {"ts_ms": 1704067200002, "machine": "A", "v": 3},
                    ]
                )
            )
        )
        files = self._files(mock_blob_client)
        assert len(files) == 2
        for key, table in files.items():
            assert "__index_level_0__" not in table.column_names, key
            assert table.column_names == ["ts_ms", "v", "__key"], key

    def test_missing_key_keeps_the_column_integer(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """A record that omits a field pads with NULL, not with pandas' NaN —
        so an integer column stays an integer column instead of being widened
        to double the moment one record leaves the field out (which made the
        same column INT in some files and DOUBLE in others)."""
        sink = sink_factory()
        sink.write(
            sample_batch(
                records=self._records(
                    [{"ts_ms": 1, "v": 1}, {"ts_ms": 2}, {"ts_ms": 3, "v": 3}]
                )
            )
        )
        table = next(iter(self._files(mock_blob_client).values()))
        assert pa.types.is_integer(table.schema.field("v").type)
        assert table.column("v").to_pylist() == [1, None, 3]

    def test_row_order_within_a_partition_is_arrival_order(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Partitioning sorts by the partition key only, stably: a time-ordered
        topic stays time-ordered inside each file, which is what keeps the
        file's zone maps tight and ORDER BY streaming able to skip it."""
        sink = sink_factory(hive_columns=["machine"])
        sink.write(
            sample_batch(
                records=self._records(
                    [
                        {"ts_ms": 10, "machine": "A", "v": 1},
                        {"ts_ms": 11, "machine": "B", "v": 2},
                        {"ts_ms": 12, "machine": "A", "v": 3},
                        {"ts_ms": 13, "machine": "B", "v": 4},
                        {"ts_ms": 14, "machine": "A", "v": 5},
                    ]
                )
            )
        )
        by_folder = {
            key.split("/")[-2]: table.column("ts_ms").to_pylist()
            for key, table in self._files(mock_blob_client).items()
        }
        assert by_folder == {"machine=A": [10, 12, 14], "machine=B": [11, 13]}

    def test_partition_column_missing_from_every_record(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """A hive column no record carries goes to the Hive-NULL bucket. The
        pandas path raised KeyError inside groupby, so the flush failed three
        times and then crashed the sink."""
        sink = sink_factory(hive_columns=["machine"])
        sink.write(
            sample_batch(records=self._records([{"ts_ms": 1, "v": 1}, {"ts_ms": 2}]))
        )
        keys = list(self._files(mock_blob_client))
        assert len(keys) == 1
        assert "/machine=__None__/" in keys[0]

    @pytest.mark.parametrize(
        "value,folder",
        [
            ("monza", "circuit=monza"),
            (7, "circuit=7"),
            (5.0, "circuit=5.0"),
            (1.25, "circuit=1.25"),
            (True, "circuit=True"),
            (None, "circuit=__None__"),
        ],
    )
    def test_partition_folder_names_match_the_pandas_path(
        self, sink_factory, sample_batch, mock_blob_client, value, folder
    ):
        """A sink upgrade must not rename a partition folder: Arrow's own cast
        writes ``5`` for 5.0 and ``true`` for True, which would scatter one
        logical value across two folders."""
        sink = sink_factory(hive_columns=["circuit"])
        sink.write(
            sample_batch(records=self._records([{"ts_ms": 1, "circuit": value}]))
        )
        assert f"/{folder}/" in next(iter(self._files(mock_blob_client)))

    def test_message_key_wins_over_a_field_named_key(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """``__key`` is the Kafka message key, as it was when the sink stamped
        it into the record's dict."""
        sink = sink_factory()
        sink.write(
            sample_batch(records=self._records([{"ts_ms": 1, "__key": "from-record"}]))
        )
        table = next(iter(self._files(mock_blob_client).values()))
        assert table.column("__key").to_pylist() == ["k0"]
        assert table.column_names.count("__key") == 1

    def test_column_order_is_first_seen_order(self, sink_factory, sample_batch):
        """Union of every record's keys, in the order they first appear, then
        the sink's own columns — the shape the DataFrame produced."""
        sink = sink_factory()
        table = sink._batch_to_arrow(
            sample_batch(
                records=self._records(
                    [{"b": 1, "a": 2}, {"c": 3, "a": 4}, {"ts_ms": 9, "a": 5}]
                )
            )
        )
        assert table.column_names == ["b", "a", "ts_ms", "__key", "c"]
        assert table.column("b").to_pylist() == [1, None, None]
        assert table.column("c").to_pylist() == [None, 3, None]
        # The record's own timestamp wins; the other rows get Kafka's.
        assert table.column("ts_ms").to_pylist() == [
            1704067200000,
            1704067200001,
            9,
        ]

    def test_every_row_is_written_exactly_once(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Across many partitions: no row is dropped, duplicated or moved."""
        sink = sink_factory(hive_columns=["year", "machine"])
        records = self._records(
            [
                {"ts_ms": 1704067200000 + i, "machine": f"m{i % 7}", "v": i}
                for i in range(500)
            ]
        )
        assert sink._write_batch(sample_batch(records=records)) == 500
        files = self._files(mock_blob_client)
        assert len(files) == 7
        seen = sorted(v for t in files.values() for v in t.column("v").to_pylist())
        assert seen == list(range(500))


# =============================================================================
# 4. Write Tests
# =============================================================================


class TestWriteOperations:
    """Tests for write operations."""

    def test_write_single_batch(self, sink_factory, sample_batch, mock_blob_client):
        """Test writing a single batch successfully."""
        sink = sink_factory()
        batch = sample_batch()

        sink.write(batch)

        # Verify blob client was called
        mock_blob_client.put_object_async.assert_called()
        call_args = mock_blob_client.put_object_async.call_args
        storage_key = call_args[0][0]

        # Verify storage key format
        assert storage_key.startswith("test-prefix/test_table/")
        assert storage_key.endswith(".parquet")

    def test_write_adds_key_column(self, sink_factory, sample_batch, mock_blob_client):
        """Test that __key column is added to written data."""
        sink = sink_factory()
        batch = sample_batch()

        sink.write(batch)

        # Get the parquet bytes that were uploaded
        call_args = mock_blob_client.put_object_async.call_args
        parquet_bytes = call_args[0][1]

        # Read back the parquet
        df = pq.read_table(io.BytesIO(parquet_bytes)).to_pandas()

        assert "__key" in df.columns
        assert list(df["__key"]) == ["key1", "key2"]

    def test_write_empty_batch_writes_nothing(self, sink_factory, mock_blob_client):
        """An empty batch uploads NOTHING. The pandas path wrote a 0-row parquet
        file for it, which every later query then had to open (and the catalog
        had to carry) for no rows at all."""
        sink = sink_factory()
        batch = SinkBatch(topic="test", partition=0)

        sink.write(batch)  # must not raise

        mock_blob_client.put_object_async.assert_not_called()

    def test_write_with_partitions(self, sink_factory, sample_batch, mock_blob_client):
        """Test writing with partition columns."""
        sink = sink_factory(hive_columns=["year", "month", "day"])

        records = [
            {
                "value": {"field1": "a", "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
        ]
        batch = sample_batch(records=records)

        sink.write(batch)

        # Verify the storage key includes partition path
        call_args = mock_blob_client.put_object_async.call_args
        storage_key = call_args[0][0]

        assert "year=2024" in storage_key
        assert "month=01" in storage_key
        assert "day=01" in storage_key

    def test_write_partitions_excluded_from_data(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test that partition columns are excluded from parquet data (Hive style)."""
        sink = sink_factory(hive_columns=["year", "month"])

        records = [
            {
                "value": {"field1": "a", "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
        ]
        batch = sample_batch(records=records)

        sink.write(batch)

        # Get the parquet bytes
        call_args = mock_blob_client.put_object_async.call_args
        parquet_bytes = call_args[0][1]

        # Read back the parquet
        df = pq.read_table(io.BytesIO(parquet_bytes)).to_pandas()

        # Year and month should NOT be in the parquet data (they're in the path)
        assert "year" not in df.columns
        assert "month" not in df.columns

    def test_write_creates_multiple_files_for_different_partitions(
        self, sink_factory, mock_blob_client
    ):
        """Test that different partition values create different files."""
        sink = sink_factory(hive_columns=["year", "month"])

        # Records from different months
        records = [
            {
                "value": {"field1": "jan", "ts_ms": 1704067200000},  # Jan 2024
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
            {
                "value": {"field1": "feb", "ts_ms": 1706745600000},  # Feb 2024
                "key": "k2",
                "timestamp": 1706745600000,
                "offset": 1,
            },
        ]

        batch = SinkBatch(topic="test", partition=0)
        for r in records:
            batch.append(
                value=r["value"],
                key=r["key"],
                timestamp=r["timestamp"],
                headers=[],
                offset=r["offset"],
            )

        sink.write(batch)

        # Should have 2 uploads - one for each partition
        assert mock_blob_client.put_object_async.call_count == 2

    def test_write_does_not_drop_rows_with_none_partition_value(
        self, sink_factory, mock_blob_client
    ):
        """
        Regression: rows whose partition column is None / missing must NOT
        be silently dropped.

        pandas' DataFrame.groupby defaults to dropna=True, so any row whose
        partition key contains NaN is silently excluded from the iteration.
        The sink's for-loop never produced a group for those rows, no PUT
        was issued — yet write() still logged "Wrote N rows" (using
        batch.size, not the actually-written count), hiding the data loss.
        """
        sink = sink_factory(hive_columns=["machine"])

        records = [
            {
                "value": {"machine": "M1", "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
            {
                "value": {"machine": None, "ts_ms": 1704067260000},
                "key": "k2",
                "timestamp": 1704067260000,
                "offset": 1,
            },
            {
                # Missing 'machine' entirely → NaN in the DataFrame, same path.
                "value": {"ts_ms": 1704067320000},
                "key": "k3",
                "timestamp": 1704067320000,
                "offset": 2,
            },
        ]
        batch = SinkBatch(topic="test", partition=0)
        for r in records:
            batch.append(
                value=r["value"],
                key=r["key"],
                timestamp=r["timestamp"],
                headers=[],
                offset=r["offset"],
            )

        sink.write(batch)

        # Two distinct partition groups → two PUTs: one for M1, one for the
        # null bucket containing both the explicit-None and missing-key rows.
        assert mock_blob_client.put_object_async.call_count == 2

        storage_keys = [
            call.args[0] for call in mock_blob_client.put_object_async.call_args_list
        ]
        assert any("machine=M1" in k for k in storage_keys)
        # Null partition values land in a single ``machine=__None__`` bucket.
        # The catalog still gets SQL NULL for these rows (see ``_write_batch``)
        # — the on-disk segment is the unified ``__None__`` sentinel used
        # across the sink, catalog, API, and UI.
        assert any("machine=__None__" in k for k in storage_keys)

    def test_write_all_null_partition_values_still_writes(
        self, sink_factory, mock_blob_client
    ):
        """
        Regression: a batch in which every row has a NaN partition value
        must still write a file. Previously this case produced zero PUTs
        while the sink reported success.
        """
        sink = sink_factory(hive_columns=["machine"])

        records = [
            {
                "value": {"ts_ms": 1704067200000},  # no 'machine'
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
            {
                "value": {"machine": None, "ts_ms": 1704067260000},
                "key": "k2",
                "timestamp": 1704067260000,
                "offset": 1,
            },
        ]
        batch = SinkBatch(topic="test", partition=0)
        for r in records:
            batch.append(
                value=r["value"],
                key=r["key"],
                timestamp=r["timestamp"],
                headers=[],
                offset=r["offset"],
            )

        sink.write(batch)

        assert mock_blob_client.put_object_async.call_count == 1
        storage_key = mock_blob_client.put_object_async.call_args.args[0]
        assert "machine=__None__" in storage_key

    def test_write_adds_timestamp_from_item_if_missing(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test that timestamp is added from SinkItem if not in value."""
        sink = sink_factory()

        # Record without ts_ms in value
        records = [
            {
                "value": {"field1": "a"},  # No ts_ms
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
        ]
        batch = sample_batch(records=records)

        sink.write(batch)

        # Get the parquet bytes
        call_args = mock_blob_client.put_object_async.call_args
        parquet_bytes = call_args[0][1]
        df = pq.read_table(io.BytesIO(parquet_bytes)).to_pandas()

        # ts_ms should be added from item.timestamp
        assert "ts_ms" in df.columns
        assert df["ts_ms"].iloc[0] == 1704067200000


# =============================================================================
# 5. Partition Validation Tests
# =============================================================================


class TestPartitionValidation:
    """Tests for partition strategy validation."""

    def test_validate_partition_strategy_matches(self, sink_factory, mock_blob_client):
        """Test validation passes when existing partitions match config."""
        sink = sink_factory(hive_columns=["year", "month"])

        # Mock existing files with matching partitions
        mock_blob_client.list_objects.return_value = [
            {
                "Key": "test-prefix/test_table/year=2024/month=01/data.parquet",
                "Size": 100,
            }
        ]

        # Should not raise
        sink._validate_existing_table_structure()

    def test_validate_partition_strategy_mismatch_raises(
        self, sink_factory, mock_blob_client
    ):
        """Test validation raises ValueError on partition mismatch."""
        sink = sink_factory(hive_columns=["year", "month"])

        # Mock existing files with different partitions
        mock_blob_client.list_objects.return_value = [
            {"Key": "test-prefix/test_table/year=2024/day=01/data.parquet", "Size": 100}
        ]

        with pytest.raises(ValueError, match="Partition strategy mismatch"):
            sink._validate_existing_table_structure()

    def test_validate_partition_strategy_empty_existing_ok(
        self, sink_factory, mock_blob_client
    ):
        """Test validation passes when no existing data."""
        sink = sink_factory(hive_columns=["year", "month"])

        mock_blob_client.list_objects.return_value = []

        # Should not raise
        sink._validate_existing_table_structure()

    def test_validate_catalog_partition_matches(
        self, sink_factory, mock_catalog_client
    ):
        """Test catalog partition validation passes when matching."""
        sink = sink_factory(
            hive_columns=["year", "month"],
            catalog_url="http://catalog:8080",
        )
        sink._catalog = mock_catalog_client

        table_metadata = {"partition_spec": ["year", "month"]}
        # Should not raise
        sink._validate_partition_strategy(table_metadata)

    def test_validate_catalog_partition_mismatch_raises(
        self, sink_factory, mock_catalog_client
    ):
        """Test catalog partition validation raises on mismatch."""
        sink = sink_factory(
            hive_columns=["year", "month"],
            catalog_url="http://catalog:8080",
        )
        sink._catalog = mock_catalog_client

        table_metadata = {"partition_spec": ["year", "day"]}  # Different!
        with pytest.raises(ValueError, match="Partition strategy mismatch"):
            sink._validate_partition_strategy(table_metadata)

    def test_validate_catalog_partition_matches_with_virtual_on_restart(
        self, sink_factory, mock_catalog_client
    ):
        """RESTART to an existing virtual-partitioned table must NOT raise.

        The catalog stores the full spec (physical + virtual) as the sink
        registers it; validation must compare against the full tree order, not
        the physical-only hive_columns (which previously caused a spurious
        'Partition strategy mismatch' on every restart of a ~-virtual sink)."""
        sink = sink_factory(
            hive_columns=["year", "month", "~driver"],
            catalog_url="http://catalog:8080",
        )
        sink._catalog = mock_catalog_client

        table_metadata = {
            "partition_spec": ["year", "month", "driver"]
        }  # phys + virtual
        # Should NOT raise.
        sink._validate_partition_strategy(table_metadata)


# =============================================================================
# 6. Catalog Integration Tests
# =============================================================================


class TestCatalogIntegration:
    """Tests for REST Catalog integration."""

    def test_auto_register_table_on_first_write(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that table is auto-registered on first write."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client

        batch = sample_batch()
        sink.write(batch)

        # Verify table registration was attempted
        mock_catalog_client.get.assert_called()  # Check if table exists
        mock_catalog_client.put.assert_called()  # Create table

        assert sink.table_registered is True

    def test_skip_register_if_table_exists(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that existing table is not recreated."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client

        # Mock table already exists
        existing_response = MagicMock()
        existing_response.status_code = 200
        existing_response.json.return_value = {"partition_spec": []}
        mock_catalog_client.get.side_effect = lambda path, **kwargs: (
            existing_response
            if "/tables/" in path and "/health" not in path
            else MagicMock(status_code=200)
        )

        batch = sample_batch()
        sink.write(batch)

        # put should NOT be called since table exists
        mock_catalog_client.put.assert_not_called()
        assert sink.table_registered is True

    def test_register_with_workspace_id_location(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that table location includes workspace_id."""
        sink = sink_factory(
            workspace_id="ws-123",
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client

        batch = sample_batch()
        sink.write(batch)

        # Check the location in the put call
        put_call = mock_catalog_client.put.call_args
        json_data = put_call.kwargs.get("json") or put_call[1].get("json")
        location = json_data["location"]

        assert "ws-123" in location
        assert location == "s3://test-bucket/ws-123/test-prefix/test_table"

    def test_setup_crashes_on_catalog_health_failure(
        self, sink_factory, mock_blob_client
    ):
        """Test that setup raises when catalog health check fails."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )

        # Mock failed health check
        failing_catalog = MagicMock(spec=QuixTSDataLakeCatalogClient)
        failing_catalog.get.side_effect = Exception("Connection refused")
        sink._catalog = failing_catalog

        with patch(
            "quixstreams.sinks.core.quix_ts_datalake_sink.get_bucket_name",
            return_value="test-bucket",
        ):
            with pytest.raises(Exception, match="Connection refused"):
                sink.setup()

    def test_manifest_registration_on_write(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that files are registered in manifest after write."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True  # Skip table registration

        batch = sample_batch()
        sink.write(batch)

        # Check manifest registration was called
        manifest_calls = [
            call
            for call in mock_catalog_client.post.call_args_list
            if "manifest" in str(call)
        ]
        assert len(manifest_calls) == 1

    def test_manifest_failure_propagates(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that manifest registration failure propagates from write."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        # Make manifest call fail
        mock_catalog_client.post.side_effect = Exception("Manifest error")

        batch = sample_batch()

        with pytest.raises(Exception, match="Manifest error"):
            sink.write(batch)

    def test_manifest_registers_null_partition_as_literal_sentinel(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        """
        A row whose partition column is None / missing lands in a
        ``col=__None__`` bucket on disk, and the catalog payload must
        record the same literal ``__None__`` string — not SQL NULL —
        so the manifest, the on-disk path, and what readers see at query
        time (DuckDB's ``hive_partitioning=true`` exposes the literal
        path segment as the column value) all agree. The lake treats
        partition values as opaque strings end-to-end; equality filters
        on ``__None__`` then resolve without any sentinel translation.
        """
        sink = sink_factory(
            hive_columns=["machine"],
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        records = [
            {
                "value": {"machine": "M1", "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
            {
                "value": {"machine": None, "ts_ms": 1704067260000},
                "key": "k2",
                "timestamp": 1704067260000,
                "offset": 1,
            },
        ]
        batch = SinkBatch(topic="test", partition=0)
        for r in records:
            batch.append(
                value=r["value"],
                key=r["key"],
                timestamp=r["timestamp"],
                headers=[],
                offset=r["offset"],
            )

        sink.write(batch)

        manifest_calls = [
            call
            for call in mock_catalog_client.post.call_args_list
            if "manifest" in str(call)
        ]
        assert len(manifest_calls) == 1
        files = manifest_calls[0].kwargs["json"]["files"]
        # One file per partition group — M1 and the null bucket.
        by_partition = {f["partition_values"]["machine"]: f for f in files}
        assert by_partition["M1"]["partition_values"]["machine"] == "M1"
        # Literal passthrough: catalog row carries ``__None__`` exactly,
        # matching the on-disk path segment.
        assert by_partition["__None__"]["partition_values"]["machine"] == "__None__"
        null_bucket_path = by_partition["__None__"]["file_path"]
        assert "machine=__None__" in null_bucket_path


class TestQueryApiNotify:
    """The sink tells the query API which files it registered; the notify never fails a flush."""

    def _sink(self, sink_factory, mock_catalog_client, mock_query_api_client):
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            query_api_url="http://lake-api:80",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink._query_api = mock_query_api_client
        sink.table_registered = True
        return sink

    def test_notify_follows_a_successful_manifest_registration(
        self,
        sink_factory,
        sample_batch,
        mock_blob_client,
        mock_catalog_client,
        mock_query_api_client,
    ):
        sink = self._sink(sink_factory, mock_catalog_client, mock_query_api_client)

        sink.write(sample_batch())

        mock_query_api_client.post.assert_called_once()
        path = mock_query_api_client.post.call_args.args[0]
        kwargs = mock_query_api_client.post.call_args.kwargs
        assert path == "/tables/test_table/files-added"
        assert kwargs["timeout"] == 5
        assert kwargs["json"]["namespace"] == "default"
        manifest_files = mock_catalog_client.post.call_args.kwargs["json"]["files"]
        assert [f["file_path"] for f in kwargs["json"]["files"]] == [
            f["file_path"] for f in manifest_files
        ]
        assert all("partition_values" in f for f in kwargs["json"]["files"])

    def test_no_notify_when_the_catalog_rejects_the_registration(
        self,
        sink_factory,
        sample_batch,
        mock_blob_client,
        mock_catalog_client,
        mock_query_api_client,
    ):
        sink = self._sink(sink_factory, mock_catalog_client, mock_query_api_client)
        rejected = MagicMock()
        rejected.status_code = 500
        rejected.text = "boom"
        mock_catalog_client.post.return_value = rejected

        with pytest.raises(RuntimeError, match="Failed to register files"):
            sink.write(sample_batch())

        mock_query_api_client.post.assert_not_called()

    def test_no_notify_without_a_query_api(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client, caplog
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        with caplog.at_level(
            logging.WARNING, logger="quixstreams.sinks.core.quix_ts_datalake_sink"
        ):
            sink.write(sample_batch())

        assert sink._query_api is None
        manifest_posts = [
            c
            for c in mock_catalog_client.post.call_args_list
            if "manifest" in c.args[0]
        ]
        assert len(manifest_posts) == 1
        assert not [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and "notify" in r.getMessage()
        ]

    def test_notify_connection_error_is_a_warning_not_a_failure(
        self,
        sink_factory,
        sample_batch,
        mock_blob_client,
        mock_catalog_client,
        mock_query_api_client,
        caplog,
    ):
        sink = self._sink(sink_factory, mock_catalog_client, mock_query_api_client)
        mock_query_api_client.post.side_effect = ConnectionError("api down")

        with caplog.at_level(
            logging.WARNING, logger="quixstreams.sinks.core.quix_ts_datalake_sink"
        ):
            sink.write(sample_batch())

        warning = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and "notify" in r.getMessage()
        ]
        assert len(warning) == 1
        assert "test_table" in warning[0].getMessage()
        assert "1 file(s)" in warning[0].getMessage()
        assert "api down" in warning[0].getMessage()

    def test_notify_rejected_status_is_a_warning_not_a_failure(
        self,
        sink_factory,
        sample_batch,
        mock_blob_client,
        mock_catalog_client,
        mock_query_api_client,
        caplog,
    ):
        sink = self._sink(sink_factory, mock_catalog_client, mock_query_api_client)
        rejected = MagicMock()
        rejected.status_code = 503
        rejected.text = "unavailable"
        mock_query_api_client.post.return_value = rejected

        with caplog.at_level(
            logging.WARNING, logger="quixstreams.sinks.core.quix_ts_datalake_sink"
        ):
            sink.write(sample_batch())

        messages = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("503" in m and "test_table" in m for m in messages)

    def test_notify_strips_column_stats(self, mock_query_api_client):
        sink = QuixTSDataLakeSink(
            s3_prefix="test-prefix",
            table_name="test_table",
            query_api_url="http://lake-api:80",
        )
        sink._query_api = mock_query_api_client
        entries = [
            {
                "file_path": "s3://b/p/x=1/a.parquet",
                "file_size": 10,
                "last_modified": "2026-09-29T00:00:00+00:00",
                "partition_values": {"x": "1"},
                "row_count": 3,
                "column_stats": {"temp": {"min": 1, "max": 2}},
            }
        ]

        sink._notify_query_api(entries)

        sent = mock_query_api_client.post.call_args.kwargs["json"]["files"]
        assert sent == [
            {
                "file_path": "s3://b/p/x=1/a.parquet",
                "file_size": 10,
                "last_modified": "2026-09-29T00:00:00+00:00",
                "partition_values": {"x": "1"},
                "row_count": 3,
            }
        ]
        assert entries[0]["column_stats"] == {"temp": {"min": 1, "max": 2}}


# =============================================================================
# 7. Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling scenarios."""

    def test_write_raises_after_max_retries(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test that write raises after exhausting retries."""
        sink = sink_factory()

        # Make uploads always fail
        future_mock = MagicMock()
        future_mock.result.side_effect = Exception("Upload failed")
        mock_blob_client.put_object_async.return_value = future_mock

        batch = sample_batch()

        with pytest.raises(Exception, match="Upload failed"):
            sink.write(batch)

    def test_blob_client_none_raises_in_write(self, sample_batch):
        """Test that write raises if blob client not initialized."""
        sink = QuixTSDataLakeSink(
            s3_prefix="prefix",
            table_name="table",
        )
        # Don't call setup() - blob client will be None
        sink._s3_bucket = "bucket"  # Set bucket to pass other checks

        batch = sample_batch()

        with pytest.raises(RuntimeError, match="BlobStorageClient not initialized"):
            sink._write_batch(batch)

    def test_cleanup_shuts_down_executor(self, sink_factory, mock_blob_client):
        """Test that cleanup shuts down the blob client executor."""
        sink = sink_factory()

        sink.cleanup()

        mock_blob_client.shutdown.assert_called_once()

    def test_cleanup_handles_none_blob_client(self):
        """Test that cleanup handles None blob client gracefully."""
        sink = QuixTSDataLakeSink(
            s3_prefix="prefix",
            table_name="table",
        )
        # blob_client is None

        # Should not raise
        sink.cleanup()

    def test_register_table_failure_propagates(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that table registration failure propagates from write."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        failing_catalog = MagicMock(spec=QuixTSDataLakeCatalogClient)
        failing_catalog.get.side_effect = Exception("Catalog unavailable")
        sink._catalog = failing_catalog

        batch = sample_batch()

        with pytest.raises(Exception, match="Catalog unavailable"):
            sink.write(batch)

    def test_register_table_bad_status_raises(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Test that non-200/201 table creation response raises RuntimeError."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )

        # Table check returns 404 (doesn't exist), create returns 500
        check_response = MagicMock()
        check_response.status_code = 404
        create_response = MagicMock()
        create_response.status_code = 500
        create_response.text = "Internal Server Error"

        failing_catalog = MagicMock(spec=QuixTSDataLakeCatalogClient)
        failing_catalog.get.return_value = check_response
        failing_catalog.put.return_value = create_response
        sink._catalog = failing_catalog

        batch = sample_batch()

        with pytest.raises(RuntimeError, match="Failed to create table"):
            sink.write(batch)

    def test_finalize_writes_clears_futures_on_failure(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test that _pending_futures is cleared even when uploads fail."""
        sink = sink_factory()

        # Make uploads fail
        future_mock = MagicMock()
        future_mock.result.side_effect = Exception("Upload failed")
        mock_blob_client.put_object_async.return_value = future_mock

        batch = sample_batch()

        with pytest.raises(Exception, match="Upload failed"):
            sink.write(batch)

        # Futures should be cleared even after failure
        assert len(sink._pending_futures) == 0

    def test_validate_structure_error_propagates(self, sink_factory, mock_blob_client):
        """Test that storage errors during structure validation propagate."""
        sink = sink_factory()

        # Make list_objects raise a storage error
        mock_blob_client.list_objects.side_effect = OSError("Storage unavailable")

        with pytest.raises(OSError, match="Storage unavailable"):
            sink._validate_existing_table_structure()


# =============================================================================
# 8. Storage Key Generation Tests
# =============================================================================


class TestStorageKeyGeneration:
    """Tests for storage key/path generation."""

    def test_storage_key_no_partitions(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test storage key without partitions."""
        sink = sink_factory(hive_columns=[])
        batch = sample_batch()

        sink.write(batch)

        call_args = mock_blob_client.put_object_async.call_args
        storage_key = call_args[0][0]

        # Should be flat: prefix/table/data_uuid.parquet
        assert storage_key.startswith("test-prefix/test_table/data_")
        assert storage_key.endswith(".parquet")
        # No partition directories
        assert "=" not in storage_key

    def test_storage_key_single_partition(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test storage key with single partition column."""
        sink = sink_factory(hive_columns=["year"])

        records = [
            {
                "value": {"field1": "a", "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
        ]
        batch = sample_batch(records=records)

        sink.write(batch)

        call_args = mock_blob_client.put_object_async.call_args
        storage_key = call_args[0][0]

        assert "year=2024" in storage_key
        assert storage_key.startswith("test-prefix/test_table/year=2024/data_")

    def test_storage_key_multiple_partitions(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test storage key with multiple partition columns."""
        sink = sink_factory(hive_columns=["year", "month", "day"])

        records = [
            {
                "value": {"field1": "a", "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
        ]
        batch = sample_batch(records=records)

        sink.write(batch)

        call_args = mock_blob_client.put_object_async.call_args
        storage_key = call_args[0][0]

        # Order should match hive_columns
        assert "year=2024/month=01/day=01" in storage_key

    def test_storage_key_custom_partition_column(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Test storage key with custom (non-time) partition column."""
        sink = sink_factory(hive_columns=["region"])

        records = [
            {
                "value": {"field1": "a", "region": "us-west"},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            },
        ]
        batch = sample_batch(records=records)

        sink.write(batch)

        call_args = mock_blob_client.put_object_async.call_args
        storage_key = call_args[0][0]

        assert "region=us-west" in storage_key


# =============================================================================
# 9. Stream-Timeout Wiring (MagicMock-based; behaviour lives in
#    test_stream_timeout_tracker.py)
# =============================================================================


class TestStreamTimeoutWiring:
    """Regression-pin that the sink calls the tracker's methods at the
    right lifecycle points. Behaviour of the tracker itself is covered
    in ``test_stream_timeout_tracker.py``; these tests replace
    ``sink._timeout`` with a ``MagicMock`` and assert call counts and
    argument shapes only. No real timing, no real threads.
    """

    def test_add_calls_tracker_touch_with_log_context(
        self, sink_factory, mock_blob_client
    ):
        """The sink forwards the key to tracker.touch(...) and passes
        topic/partition/offset as kwargs (opaque context to the tracker).
        """
        sink = sink_factory()
        sink._timeout = MagicMock()

        sink.add(
            value={"v": 1},
            key="sensor-a",
            timestamp=1000,
            headers=[],
            topic="my-topic",
            partition=3,
            offset=42,
        )

        sink._timeout.touch.assert_called_once_with(
            "sensor-a", topic="my-topic", partition=3, offset=42
        )

    def test_flush_calls_tracker_check_now(self, sink_factory, mock_blob_client):
        sink = sink_factory()
        sink._timeout = MagicMock()

        sink.flush()

        sink._timeout.check_now.assert_called_once_with()

    def test_setup_calls_tracker_start(self, sink_factory, mock_blob_client):
        """setup() calls tracker.start() AFTER the blob client is healthy."""
        sink = sink_factory()
        sink._timeout = MagicMock()

        with (
            patch(
                "quixstreams.sinks.core.quix_ts_datalake_sink.get_bucket_name",
                return_value="test-bucket",
            ),
            patch(
                "quixstreams.sinks.core.quix_ts_datalake_sink.BlobStorageClient",
                return_value=mock_blob_client,
            ),
        ):
            sink.setup()

        sink._timeout.start.assert_called_once_with()

    def test_cleanup_calls_tracker_stop(self, sink_factory, mock_blob_client):
        sink = sink_factory()
        sink._timeout = MagicMock()

        sink.cleanup()

        sink._timeout.stop.assert_called_once_with()

    def test_on_paused_does_not_touch_tracker(self, sink_factory, mock_blob_client):
        """Regression pin: on_paused must NOT invoke any tracker method
        (backpressure is not a silence event).
        """
        sink = sink_factory()
        sink._timeout = MagicMock()

        sink.on_paused()

        sink._timeout.touch.assert_not_called()
        sink._timeout.check_now.assert_not_called()
        sink._timeout.start.assert_not_called()
        sink._timeout.stop.assert_not_called()

    def test_constructor_builds_tracker_with_expected_args(self):
        """The sink forwards stream_timeout_ms / on_stream_timeout /
        _check_interval_ms to the tracker constructor. Public sink
        signature unchanged.
        """
        callback = MagicMock()
        sink = QuixTSDataLakeSink(
            s3_prefix="p",
            table_name="t",
            stream_timeout_ms=6000,
            on_stream_timeout=callback,
            _check_interval_ms=250,
        )
        assert sink._timeout.enabled is True
        assert sink._timeout._stream_timeout_ms == 6000
        assert sink._timeout._on_stream_timeout is callback
        assert sink._timeout._check_interval_ms == 250

    def test_constructor_disabled_pair_leaves_tracker_disabled(self):
        sink = QuixTSDataLakeSink(s3_prefix="p", table_name="t")
        assert sink._timeout.enabled is False
        # Disabled path: touch/check_now/start/stop are all no-ops.
        sink._timeout.touch("s1")
        sink._timeout.check_now()
        sink._timeout.start()
        sink._timeout.stop()


# =============================================================================
# Column statistics (per-file min/max zone maps for query-time pruning)
# =============================================================================


class TestQuixTSDataLakeSinkColumnStats:
    """Tests for per-file min/max stats computed in the sink."""

    def test_compute_column_stats_numeric_and_timestamp(self, sink_factory):
        sink = sink_factory()
        table = pa.table(
            {
                "speed": pa.array([100, 50, 300], type=pa.int64()),
                "temp": pa.array([1.5, 2.5, None], type=pa.float64()),
                "ts": pa.array(
                    [
                        datetime(2026, 1, 1, tzinfo=timezone.utc),
                        datetime(2026, 1, 3, tzinfo=timezone.utc),
                        datetime(2026, 1, 2, tzinfo=timezone.utc),
                    ]
                ),
                "name": pa.array(["a", "b", "c"]),  # string -> skipped
                "__key": pa.array(["k1", "k2", "k3"]),  # internal -> skipped
            }
        )

        stats = sink._compute_column_stats(table)

        # String and internal columns are not tracked.
        assert set(stats.keys()) == {"speed", "temp", "ts"}

        assert stats["speed"] == {
            "type": "numeric",
            "min": 50.0,
            "max": 300.0,
            "null_count": 0,
            "value_count": 3,
        }
        # One null in temp: min/max ignore it, counts reflect it.
        assert stats["temp"]["type"] == "numeric"
        assert stats["temp"]["min"] == 1.5
        assert stats["temp"]["max"] == 2.5
        assert stats["temp"]["null_count"] == 1
        assert stats["temp"]["value_count"] == 2

        assert stats["ts"]["type"] == "timestamp"
        # ISO-8601 bounds, min/max by time (not input order).
        assert stats["ts"]["min"].startswith("2026-01-01")
        assert stats["ts"]["max"].startswith("2026-01-03")

    def test_all_null_numeric_column_is_skipped(self, sink_factory):
        sink = sink_factory()
        table = pa.table({"x": pa.array([None, None], type=pa.float64())})
        assert sink._compute_column_stats(table) == {}

    def test_stats_columns_restricts_the_tracked_set(self, sink_factory):
        sink = sink_factory(stats_columns=["speed"])
        table = pa.table(
            {
                "speed": pa.array([1, 2], type=pa.int64()),
                "temp": pa.array([1.0, 2.0], type=pa.float64()),
            }
        )
        stats = sink._compute_column_stats(table)
        assert set(stats.keys()) == {"speed"}

    @pytest.mark.parametrize("value", [2**53 + 1, 2**53 + 3])
    def test_safe_float_bounds_widen_for_large_ints(self, sink_factory, value):
        # Neither value is exactly representable as float64, and they round in
        # OPPOSITE directions: 2**53+1 rounds DOWN (only _safe_float_max has to
        # widen) and 2**53+3 rounds UP (only _safe_float_min has to). Covering
        # both is what exercises both widening loops.
        sink = sink_factory()
        assert float(value) != value  # precondition: this value really is lossy

        lo = sink._safe_float_min(value)
        hi = sink._safe_float_max(value)

        # Stored bounds must ENCLOSE the true value so pruning never wrongly
        # skips a file holding matching rows.
        assert lo <= value <= hi
        # ...and STRICTLY bracket it: an unrepresentable int can equal neither
        # bound, so a naive float() would have been wrong on one side. Without
        # this the test would still pass if a widening loop were deleted.
        assert lo < value < hi

    def test_write_attaches_column_stats_to_manifest_payload(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        sink.write(sample_batch())

        manifest_calls = [
            call
            for call in mock_catalog_client.post.call_args_list
            if "manifest" in str(call)
        ]
        assert len(manifest_calls) == 1
        files = manifest_calls[0].kwargs["json"]["files"]
        assert len(files) == 1
        cs = files[0]["column_stats"]

        # Numeric data columns get zone maps; the string column and the
        # internal __key column do not.
        assert "field2" in cs and cs["field2"]["type"] == "numeric"
        assert cs["field2"]["min"] == 100.0
        assert cs["field2"]["max"] == 200.0
        assert "ts_ms" in cs and cs["ts_ms"]["type"] == "numeric"
        assert "field1" not in cs
        assert "__key" not in cs


# =============================================================================
# Virtual partition columns (~ prefix): navigate/filter without foldering/splitting
# =============================================================================


class TestQuixTSDataLakeSinkVirtualPartitions:
    """Tests for ~-prefixed virtual partition columns."""

    def _batch(self, records):
        batch = SinkBatch(topic="test", partition=0)
        for r in records:
            batch.append(
                value=r["value"],
                key=r["key"],
                timestamp=r["timestamp"],
                headers=[],
                offset=r["offset"],
            )
        return batch

    def _drivers_batch(self):
        # Two drivers, same day -> would be one physical partition group.
        return self._batch(
            [
                {
                    "value": {"driver": "HAM", "speed": 100, "ts_ms": 1704067200000},
                    "key": "k1",
                    "timestamp": 1704067200000,
                    "offset": 0,
                },
                {
                    "value": {"driver": "VER", "speed": 200, "ts_ms": 1704067200000},
                    "key": "k2",
                    "timestamp": 1704067200000,
                    "offset": 1,
                },
            ]
        )

    def test_tilde_prefix_parsing(self, sink_factory):
        sink = sink_factory(hive_columns=["year", "month", "~driver"])
        assert sink.hive_columns == ["year", "month"]  # physical only
        assert sink._virtual_columns == ["driver"]
        assert sink._partition_spec_order == ["year", "month", "driver"]

    @staticmethod
    def _uploads(mock_blob_client):
        """Split put_object_async calls into (data files, .vidx sidecars).

        Every data file is accompanied by a virtual-index sidecar upload, so a
        raw ``call_count`` conflates the two. Assertions about file *splitting*
        must look at data files only.
        """
        calls = mock_blob_client.put_object_async.call_args_list
        data = [c for c in calls if "/.vidx/" not in c[0][0]]
        sidecars = [c for c in calls if "/.vidx/" in c[0][0]]
        return data, sidecars

    def test_virtual_column_does_not_split_files(self, sink_factory, mock_blob_client):
        # physical=year, virtual=driver: two drivers in one year -> ONE data file,
        # plus its .vidx sidecar (metadata, not a data split).
        sink = sink_factory(hive_columns=["year", "~driver"])
        sink.write(self._drivers_batch())
        data, sidecars = self._uploads(mock_blob_client)
        assert len(data) == 1
        assert len(sidecars) == 1

    def test_virtual_column_kept_in_data_physical_dropped(
        self, sink_factory, mock_blob_client
    ):
        sink = sink_factory(hive_columns=["year", "~driver"])
        sink.write(self._drivers_batch())
        data, _ = self._uploads(mock_blob_client)
        df = pq.read_table(io.BytesIO(data[0][0][1])).to_pandas()
        assert "driver" in df.columns  # virtual column stays in the data
        assert "year" not in df.columns  # physical partition column is foldered away
        assert set(df["driver"]) == {"HAM", "VER"}

    def test_sidecar_carries_full_partition_tuple(self, sink_factory, mock_blob_client):
        # The sidecar holds one row per distinct VIRTUAL tuple, with the file's
        # PHYSICAL partition values added as constant columns, so a reader gets
        # the full tuple without hive_partitioning.
        sink = sink_factory(hive_columns=["year", "~driver"])
        sink.write(self._drivers_batch())
        _, sidecars = self._uploads(mock_blob_client)
        key, payload = sidecars[0][0]
        assert "/.vidx/" in key
        vdf = pq.read_table(io.BytesIO(payload)).to_pandas()
        assert set(vdf["driver"]) == {"HAM", "VER"}
        assert set(vdf["year"]) == {"2024"}

    # -- Durability of the sidecar lane -------------------------------------
    # Both handlers below back an explicit promise in the sink: the virtual
    # index is a HINT, not the data, so a sidecar failure is logged and never
    # raised. write() retries a failed batch 3x, so "exactly one data upload"
    # is also the assertion that no retry was triggered.

    @staticmethod
    def _split_futures(mock_blob_client, sidecar_future):
        """Route .vidx uploads to `sidecar_future`, data uploads to a good one."""
        ok = MagicMock()
        ok.result.return_value = None

        def route(key, _payload):
            if "/.vidx/" in key:
                if isinstance(sidecar_future, Exception):
                    raise sidecar_future
                return sidecar_future
            return ok

        mock_blob_client.put_object_async.side_effect = route

    def _assert_data_write_unaffected(self, mock_blob_client, mock_catalog_client):
        data, sidecars = self._uploads(mock_blob_client)
        # Exactly one -> the data file landed AND write() did not retry.
        assert len(data) == 1
        manifest_calls = [
            c for c in mock_catalog_client.post.call_args_list if "manifest" in str(c)
        ]
        assert len(manifest_calls) == 1
        assert len(manifest_calls[0].kwargs["json"]["files"]) == 1
        return data, sidecars

    def test_sidecar_submit_failure_does_not_fail_the_write(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        # The sidecar upload CALL itself raises (_write_virtual_sidecar's except).
        self._split_futures(mock_blob_client, RuntimeError("sidecar submit failed"))
        sink = sink_factory(
            hive_columns=["year", "~driver"], catalog_url="http://catalog:8080"
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        sink.write(self._drivers_batch())  # must not raise

        self._assert_data_write_unaffected(mock_blob_client, mock_catalog_client)
        # Nothing was queued to await, so the tracking list is clean for next batch.
        assert sink._pending_sidecar_futures == []

    def test_sidecar_upload_failure_does_not_fail_the_write(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        # Submit succeeds; the future raises when awaited (_await_sidecar_uploads).
        bad = MagicMock()
        bad.result.side_effect = RuntimeError("sidecar upload failed")
        self._split_futures(mock_blob_client, bad)
        sink = sink_factory(
            hive_columns=["year", "~driver"], catalog_url="http://catalog:8080"
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        sink.write(self._drivers_batch())  # must not raise

        _, sidecars = self._assert_data_write_unaffected(
            mock_blob_client, mock_catalog_client
        )
        assert len(sidecars) == 1  # it was submitted; only the await failed
        bad.result.assert_called_once()
        # Drained even though it failed — a stale future must not be re-awaited.
        assert sink._pending_sidecar_futures == []

    def test_register_table_declares_virtual_partitions(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(
            hive_columns=["year", "month", "~driver"],
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink._register_table()

        body = mock_catalog_client.put.call_args.kwargs["json"]
        # Full tree order sent up front (virtual can't be discovered from paths).
        assert body["partition_spec"] == ["year", "month", "driver"]
        assert body["properties"]["virtual_partitions"] == ["driver"]

    def test_register_table_records_sort_and_timestamp_columns(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(
            timestamp_column="ts_ms",
            sort_column="seq",
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink._register_table()

        props = mock_catalog_client.put.call_args.kwargs["json"]["properties"]
        assert props["sort_column"] == "seq"
        assert props["timestamp_column"] == "ts_ms"

    def test_register_table_omits_sort_column_when_unset(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(
            timestamp_column="ts_ms",
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink._register_table()

        props = mock_catalog_client.put.call_args.kwargs["json"]["properties"]
        # No explicit sort_column -> omitted so the lakehouse falls back to the
        # timestamp column (which is still recorded).
        assert "sort_column" not in props
        assert props["timestamp_column"] == "ts_ms"


# =============================================================================
# 9. The table's registered LOCATION wins
# =============================================================================


class TestTableLocationIsHonoured:
    """A table's location is where every file in its manifest lives. A second
    writer joining the table follows it instead of starting a second folder —
    and the catalog, for its part, refuses to be re-pointed by such a writer."""

    @staticmethod
    def _catalog(location=None, properties=None, status=200):
        """A catalog client whose table GET returns *location* (404 when None)."""
        client = MagicMock(spec=QuixTSDataLakeCatalogClient)
        health = MagicMock(status_code=200)
        health.raise_for_status = MagicMock()
        table = MagicMock(status_code=status if location else 404)
        table.json.return_value = {
            "name": "test_table",
            "location": location,
            "properties": properties or {},
        }
        client.get.side_effect = lambda path, **kw: (
            health if "/health" in path else table
        )
        created = MagicMock(status_code=201)
        created.json.return_value = {"location": location, "properties": {}}
        client.put.return_value = created
        client.post.return_value = MagicMock(status_code=200)
        client.patch.return_value = MagicMock(status_code=200)
        return client

    def _setup(self, sink, mock_blob_client):
        with (
            patch(
                "quixstreams.sinks.core.quix_ts_datalake_sink.get_bucket_name",
                return_value="test-bucket",
            ),
            patch(
                "quixstreams.sinks.core.quix_ts_datalake_sink.BlobStorageClient",
                return_value=mock_blob_client,
            ),
        ):
            sink.setup()

    def test_existing_table_elsewhere_is_written_to_where_it_lives(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        sink = sink_factory(
            s3_prefix="data-lake/time-series",
            workspace_id="ws-a",
            catalog_url="http://catalog:8080",
        )
        sink._catalog = self._catalog(
            "s3://test-bucket/ws-a/legacy-prefix/renamed_table"
        )
        self._setup(sink, mock_blob_client)

        assert sink._table_root == "legacy-prefix/renamed_table"
        assert (
            sink.table_location == "s3://test-bucket/ws-a/legacy-prefix/renamed_table"
        )

        sink.write(sample_batch())
        key = mock_blob_client.put_object_async.call_args_list[0].args[0]
        assert key.startswith("legacy-prefix/renamed_table/data_")
        # And the manifest entry agrees with the folder the file went to.
        manifest = [
            c for c in sink._catalog.post.call_args_list if "manifest" in c.args[0]
        ][0].kwargs["json"]
        assert manifest["files"][0]["file_path"] == (f"s3://test-bucket/ws-a/{key}")
        # Nothing was re-registered: the sink did not try to move the table.
        sink._catalog.put.assert_not_called()

    def test_own_location_changes_nothing(self, sink_factory, mock_blob_client, caplog):
        sink = sink_factory(
            s3_prefix="data-lake/time-series",
            workspace_id="ws-a",
            catalog_url="http://catalog:8080",
        )
        sink._catalog = self._catalog(
            "s3://test-bucket/ws-a/data-lake/time-series/test_table"
        )
        with caplog.at_level(logging.WARNING):
            self._setup(sink, mock_blob_client)

        assert sink._table_root == "data-lake/time-series/test_table"
        assert not [r for r in caplog.records if "registered at" in r.getMessage()]

    def test_a_location_in_another_bucket_is_refused(
        self, sink_factory, mock_blob_client
    ):
        sink = sink_factory(workspace_id="ws-a", catalog_url="http://catalog:8080")
        sink._catalog = self._catalog("s3://other-bucket/ws-a/prefix/test_table")

        with pytest.raises(ValueError, match="bucket"):
            self._setup(sink, mock_blob_client)

    def test_a_location_in_another_workspace_is_refused(
        self, sink_factory, mock_blob_client
    ):
        sink = sink_factory(workspace_id="ws-a", catalog_url="http://catalog:8080")
        sink._catalog = self._catalog("s3://test-bucket/ws-b/prefix/test_table")

        with pytest.raises(ValueError, match="workspace"):
            self._setup(sink, mock_blob_client)

    def test_a_catalog_hiccup_leaves_the_configured_path(
        self, sink_factory, mock_blob_client, caplog
    ):
        sink = sink_factory(
            s3_prefix="data-lake/time-series", catalog_url="http://catalog:8080"
        )
        client = self._catalog()
        health = MagicMock(status_code=200)
        health.raise_for_status = MagicMock()

        def _get(path, **kw):
            if "/health" in path:
                return health
            raise requests.exceptions.ConnectionError("catalog down")

        client.get.side_effect = _get
        sink._catalog = client

        with caplog.at_level(logging.WARNING):
            self._setup(sink, mock_blob_client)

        assert sink._table_root == "data-lake/time-series/test_table"
        assert any(
            "Could not read the registered location" in r.getMessage()
            for r in caplog.records
        )

    def test_losing_the_create_race_follows_the_winner(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Two sinks can both see "no such table" and both create it. The
        catalog keeps the winner's location and reports the conflict; the loser
        writes where the table actually is."""
        sink = sink_factory(
            s3_prefix="data-lake/time-series", catalog_url="http://catalog:8080"
        )
        client = self._catalog()  # GET -> 404, so the sink creates the table
        created = MagicMock(status_code=201)
        created.json.return_value = {
            "location": "s3://test-bucket/winner-prefix/test_table",
            "location_conflict": {
                "requested": "s3://test-bucket/data-lake/time-series/test_table",
                "kept": "s3://test-bucket/winner-prefix/test_table",
            },
        }
        client.put.return_value = created
        sink._catalog = client
        self._setup(sink, mock_blob_client)

        sink.write(sample_batch())

        assert sink._table_root == "winner-prefix/test_table"
        key = mock_blob_client.put_object_async.call_args_list[0].args[0]
        assert key.startswith("winner-prefix/test_table/data_")

    def test_a_new_table_is_created_at_the_configured_location(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        sink = sink_factory(
            s3_prefix="data-lake/time-series",
            workspace_id="ws-a",
            catalog_url="http://catalog:8080",
        )
        client = self._catalog()  # 404
        sink._catalog = client
        self._setup(sink, mock_blob_client)
        sink.write(sample_batch())

        body = client.put.call_args.kwargs["json"]
        assert body["location"] == (
            "s3://test-bucket/ws-a/data-lake/time-series/test_table"
        )


# =============================================================================
# 10. Source registry: which topic / workspace feeds the table
# =============================================================================


class TestTableSources:
    """The catalog records WHERE a table's data comes from. One table is
    routinely fed by several topics, so each writer registers its own source
    and never touches another's."""

    @staticmethod
    def _sink(sink_factory, mock_catalog_client):
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        return sink

    @staticmethod
    def _source_posts(client):
        return [c for c in client.post.call_args_list if c.args[0].endswith("/sources")]

    def test_first_write_registers_the_topic_once(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = self._sink(sink_factory, mock_catalog_client)
        sink.workspace_id = "ws-a"

        sink.write(sample_batch(topic="telemetry"))
        sink.write(sample_batch(topic="telemetry"))

        posts = self._source_posts(mock_catalog_client)
        assert len(posts) == 1, "one request per topic per process, not per flush"
        assert posts[0].args[0] == "/namespaces/default/tables/test_table/sources"
        source = posts[0].kwargs["json"]["source"]
        assert source["topic"] == "telemetry"
        assert source["workspace_id"] == "ws-a"
        assert source["source_type"] == "kafka"
        assert source["properties"]["sink"] == "quixstreams-quix-ts-datalake-sink"

    def test_a_second_topic_is_registered_too(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = self._sink(sink_factory, mock_catalog_client)

        sink.write(sample_batch(topic="car-1"))
        sink.write(sample_batch(topic="car-2"))

        topics = [
            c.kwargs["json"]["source"]["topic"]
            for c in self._source_posts(mock_catalog_client)
        ]
        assert topics == ["car-1", "car-2"]

    def test_add_files_carries_the_source(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """Provenance rides along with the files, in the same request, so the
        catalog records it in the transaction that registers them."""
        sink = self._sink(sink_factory, mock_catalog_client)

        sink.write(sample_batch(topic="telemetry"))

        manifest = [
            c
            for c in mock_catalog_client.post.call_args_list
            if "manifest" in c.args[0]
        ][0].kwargs["json"]
        assert manifest["source"]["topic"] == "telemetry"
        assert manifest["source"]["source_type"] == "kafka"

    def test_a_failed_registration_never_fails_the_write(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client, caplog
    ):
        def _post(path, **kwargs):
            if path.endswith("/sources"):
                raise requests.exceptions.ConnectionError("catalog down")
            return MagicMock(status_code=200)

        mock_catalog_client.post.side_effect = _post
        sink = self._sink(sink_factory, mock_catalog_client)

        with caplog.at_level(logging.WARNING):
            sink.write(sample_batch(topic="telemetry"))

        assert mock_blob_client.put_object_async.called
        assert any(
            "Could not register source" in r.getMessage() for r in caplog.records
        )

    def test_an_older_catalog_is_asked_only_once(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """A catalog without the registry answers 501; the sink stops asking
        instead of posting on every flush for ever."""

        def _post(path, **kwargs):
            return MagicMock(status_code=501 if path.endswith("/sources") else 200)

        mock_catalog_client.post.side_effect = _post
        sink = self._sink(sink_factory, mock_catalog_client)

        sink.write(sample_batch(topic="telemetry"))
        sink.write(sample_batch(topic="telemetry"))

        assert len(self._source_posts(mock_catalog_client)) == 1


# =============================================================================
# 11. The zone-map column set is declared on the table
# =============================================================================


class TestStatsColumnsDeclaration:
    """``stats_columns`` restricts which columns get a zone map. The lakehouse's
    own rewrites read parquet footers and would otherwise record one for EVERY
    numeric/timestamp column, so the restriction has to live on the TABLE, not
    only in this process's configuration."""

    @staticmethod
    def _existing_catalog(properties):
        client = MagicMock(spec=QuixTSDataLakeCatalogClient)
        health = MagicMock(status_code=200)
        health.raise_for_status = MagicMock()
        table = MagicMock(status_code=200)
        table.json.return_value = {
            "name": "test_table",
            "location": "s3://test-bucket/test-prefix/test_table",
            "partition_spec": [],
            "properties": properties,
        }
        client.get.side_effect = lambda path, **kw: (
            health if "/health" in path else table
        )
        client.patch.return_value = MagicMock(status_code=200)
        client.post.return_value = MagicMock(status_code=200)
        return client

    @staticmethod
    def _patches(client):
        return [c.kwargs["json"]["properties"] for c in client.patch.call_args_list]

    def test_declared_on_a_new_table(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", stats_columns=["ts_ms"])
        sink._catalog = mock_catalog_client
        sink.write(sample_batch())

        properties = mock_catalog_client.put.call_args.kwargs["json"]["properties"]
        assert properties["stats_columns"] == ["ts_ms"]

    def test_absent_when_unrestricted(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080")
        sink._catalog = mock_catalog_client
        sink.write(sample_batch())

        properties = mock_catalog_client.put.call_args.kwargs["json"]["properties"]
        assert "stats_columns" not in properties

    def test_pushed_onto_an_existing_table(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """The table the operator is complaining about already exists: the sink
        must bring the declaration to it, not only to tables it creates."""
        sink = sink_factory(
            catalog_url="http://catalog:8080", stats_columns=["ts_ms", "speed"]
        )
        sink._catalog = self._existing_catalog({"timestamp_column": "ts_ms"})
        sink.write(sample_batch())

        assert self._patches(sink._catalog) == [{"stats_columns": ["speed", "ts_ms"]}]

    def test_no_patch_when_the_table_already_agrees(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", stats_columns=["ts_ms"])
        sink._catalog = self._existing_catalog(
            {"stats_columns": ["ts_ms"], "timestamp_column": "ts_ms"}
        )
        sink.write(sample_batch())

        sink._catalog.patch.assert_not_called()

    def test_clearing_the_restriction_clears_the_property(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080")  # unrestricted
        sink._catalog = self._existing_catalog(
            {"stats_columns": ["ts_ms"], "timestamp_column": "ts_ms"}
        )
        sink.write(sample_batch())

        assert self._patches(sink._catalog) == [{"stats_columns": None}]

    def test_ordering_columns_are_filled_in_but_never_overwritten(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """An operator can set the sort column in the lakehouse console; a sink
        restart must not stamp its own value back over that choice. A table with
        NO ordering column does learn the sink's."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            timestamp_column="ts_ms",
            sort_column="speed",
        )
        sink._catalog = self._existing_catalog({"sort_column": "operator_choice"})
        sink.write(sample_batch())

        assert self._patches(sink._catalog) == [{"timestamp_column": "ts_ms"}]

    def test_a_failed_patch_never_fails_the_write(
        self, sink_factory, sample_batch, mock_blob_client, caplog
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", stats_columns=["ts_ms"])
        client = self._existing_catalog({})
        client.patch.side_effect = requests.exceptions.ConnectionError("catalog down")
        sink._catalog = client

        with caplog.at_level(logging.WARNING):
            sink.write(sample_batch())

        assert mock_blob_client.put_object_async.called
        assert any(
            "Could not update properties" in r.getMessage() for r in caplog.records
        )
