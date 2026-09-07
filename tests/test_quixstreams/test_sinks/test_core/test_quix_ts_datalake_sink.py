"""
Tests for QuixTSDataLakeSink

Comprehensive unit and integration tests for the Quix Lake Blob Storage Sink,
covering initialization, timestamp mapping, partition handling, write operations,
catalog integration, and error handling.
"""

import io
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


@pytest.fixture(autouse=True)
def no_retry_sleep():
    """The sink sleeps 3s between retry attempts; the tests exercising those
    paths only care about the attempt count."""
    with patch("quixstreams.sinks.core.quix_ts_datalake_sink.time.sleep"):
        yield


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

    # Table create response
    table_create_response = MagicMock()
    table_create_response.status_code = 201

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
            assert (
                _logging.getLogger(name).level == _logging.WARNING
            ), f"{name} not raised to WARNING"

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


class TestPartitionConfigValidation:
    """Configs that used to be accepted silently and then stick (the restart
    validators compare the sink against the table it created) are rejected at
    construction instead."""

    @pytest.mark.parametrize("col", ["~year", "~month", "~day", "~hour"])
    def test_virtual_time_column_is_rejected(self, col):
        # year/month/day/hour are derived from timestamp_column; a virtual one
        # would be a permanently empty partition level.
        with pytest.raises(ValueError, match="physical partitions"):
            QuixTSDataLakeSink(
                s3_prefix="p", table_name="t", hive_columns=["year", col]
            )

    def test_bare_tilde_is_rejected(self):
        with pytest.raises(ValueError, match="must be column names"):
            QuixTSDataLakeSink(s3_prefix="p", table_name="t", hive_columns=["~"])

    @pytest.mark.parametrize(
        "cols", [["year", "year"], ["driver", "~driver"], ["~driver", "~driver"]]
    )
    def test_duplicate_column_is_rejected(self, cols):
        with pytest.raises(ValueError, match="more than once"):
            QuixTSDataLakeSink(s3_prefix="p", table_name="t", hive_columns=cols)

    def test_sort_column_cannot_be_a_physical_partition(self):
        # A physical column is stripped from every file, so no file can be
        # sorted by it; recording it blind would promise pruning that never comes.
        with pytest.raises(ValueError, match="physical partition column"):
            QuixTSDataLakeSink(
                s3_prefix="p", table_name="t", hive_columns=["year"], sort_column="year"
            )

    def test_sort_column_may_be_virtual_or_plain(self):
        QuixTSDataLakeSink(
            s3_prefix="p",
            table_name="t",
            hive_columns=["year", "~driver"],
            sort_column="driver",
        )
        QuixTSDataLakeSink(
            s3_prefix="p", table_name="t", hive_columns=["year"], sort_column="seq"
        )

    def test_stats_columns_excluding_ordering_columns_warns(self, caplog):
        QuixTSDataLakeSink(
            s3_prefix="p", table_name="t", sort_column="seq", stats_columns=["speed"]
        )
        assert "excludes the ordering column(s) ['seq', 'ts_ms']" in caplog.text

    def test_stats_columns_covering_ordering_columns_is_quiet(self, caplog):
        QuixTSDataLakeSink(
            s3_prefix="p",
            table_name="t",
            sort_column="seq",
            stats_columns=["seq", "ts_ms"],
        )
        assert "excludes the ordering column" not in caplog.text


# =============================================================================
# 2. Timestamp Column Mapping Tests
# =============================================================================


class TestTimestampColumnMapping:
    """Tests for timestamp detection and column extraction."""

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

        df = pd.DataFrame({"ts_ms": [timestamp_value], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        # All timestamps resolve to 2024-01-01 00:00:00 UTC
        assert result_df["year"].iloc[0] == "2024"
        assert result_df["month"].iloc[0] == "01"
        assert result_df["day"].iloc[0] == "01"
        assert result_df["hour"].iloc[0] == "00"

    def test_add_timestamp_columns_already_datetime(self, sink_factory):
        """Test that datetime columns pass through without conversion."""
        sink = sink_factory(hive_columns=["year", "month"])

        dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        df = pd.DataFrame({"ts_ms": [dt], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        assert result_df["year"].iloc[0] == "2024"
        assert result_df["month"].iloc[0] == "06"

    def test_timestamp_column_year_extraction(self, sink_factory):
        """Test year column extraction format."""
        sink = sink_factory(hive_columns=["year"])

        df = pd.DataFrame({"ts_ms": [1704067200000], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        assert result_df["year"].iloc[0] == "2024"
        assert isinstance(result_df["year"].iloc[0], str)

    def test_timestamp_column_month_zero_padding(self, sink_factory):
        """Test month column is zero-padded (01-12)."""
        sink = sink_factory(hive_columns=["month"])

        # January (should be "01", not "1")
        df = pd.DataFrame({"ts_ms": [1704067200000], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        assert result_df["month"].iloc[0] == "01"
        assert len(result_df["month"].iloc[0]) == 2

    def test_timestamp_column_day_zero_padding(self, sink_factory):
        """Test day column is zero-padded (01-31)."""
        sink = sink_factory(hive_columns=["day"])

        df = pd.DataFrame({"ts_ms": [1704067200000], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        assert result_df["day"].iloc[0] == "01"
        assert len(result_df["day"].iloc[0]) == 2

    def test_timestamp_column_hour_zero_padding(self, sink_factory):
        """Test hour column is zero-padded (00-23)."""
        sink = sink_factory(hive_columns=["hour"])

        df = pd.DataFrame({"ts_ms": [1704067200000], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        assert result_df["hour"].iloc[0] == "00"
        assert len(result_df["hour"].iloc[0]) == 2

    def test_only_specified_columns_are_added(self, sink_factory):
        """Test that only specified hive columns are added."""
        sink = sink_factory(hive_columns=["year", "day"])  # No month, no hour

        df = pd.DataFrame({"ts_ms": [1704067200000], "value": [1]})
        result_df = sink._add_timestamp_columns(df)

        assert "year" in result_df.columns
        assert "day" in result_df.columns
        assert "month" not in result_df.columns
        assert "hour" not in result_df.columns

    def test_add_timestamp_columns_does_not_mutate_timestamp_column_dtype(
        self, sink_factory
    ):
        """
        Regression: extracting year/month/day/hour for time-based hive
        partitioning must not change the dtype of the source timestamp
        column. ``ts_ms`` is a system column the sink injects from the
        Kafka ``item.timestamp`` (always int64 ms); its dtype is part of
        the contract with readers — files written under different
        ``HIVE_COLUMNS`` configurations must store ``ts_ms`` with the same
        type, otherwise downstream readers see the same column as BIGINT
        in some files and TIMESTAMP in others.
        """
        sink = sink_factory(hive_columns=["year", "month", "day", "hour"])

        df = pd.DataFrame({"ts_ms": [1704067200000, 1704067260000], "value": [1, 2]})
        original_dtype = df["ts_ms"].dtype

        result_df = sink._add_timestamp_columns(df)

        # Derived columns still correct.
        assert result_df["year"].iloc[0] == "2024"
        assert result_df["month"].iloc[0] == "01"
        assert result_df["day"].iloc[0] == "01"
        assert result_df["hour"].iloc[0] == "00"

        # Source ts_ms column is untouched — same dtype, same values.
        assert result_df["ts_ms"].dtype == original_dtype, (
            f"ts_ms dtype changed from {original_dtype} to "
            f"{result_df['ts_ms'].dtype}; partitioning logic must not "
            f"mutate the data column"
        )
        assert list(result_df["ts_ms"]) == [1704067200000, 1704067260000]


# =============================================================================
# 3. Empty Dict Handling Tests
# =============================================================================


class TestEmptyDictHandling:
    """Tests for empty dictionary to null conversion."""

    def test_null_empty_dicts_converts_empty_to_none(self, sink_factory):
        """Test that empty dicts are converted to None."""
        sink = sink_factory()

        df = pd.DataFrame({"col": [{}, {}, {}]})
        sink._null_empty_dicts(df)

        assert df["col"].iloc[0] is None
        assert df["col"].iloc[1] is None
        assert df["col"].iloc[2] is None

    def test_null_empty_dicts_preserves_non_empty(self, sink_factory):
        """Test that non-empty dicts are preserved."""
        sink = sink_factory()

        df = pd.DataFrame({"col": [{"a": 1}, {"b": 2}]})
        sink._null_empty_dicts(df)

        assert df["col"].iloc[0] == {"a": 1}
        assert df["col"].iloc[1] == {"b": 2}

    def test_null_empty_dicts_mixed_column(self, sink_factory):
        """Test handling of mixed empty and non-empty dicts."""
        sink = sink_factory()

        df = pd.DataFrame({"col": [{"a": 1}, {}, {"c": 3}, {}]})
        sink._null_empty_dicts(df)

        assert df["col"].iloc[0] == {"a": 1}
        assert df["col"].iloc[1] is None
        assert df["col"].iloc[2] == {"c": 3}
        assert df["col"].iloc[3] is None

    def test_null_empty_dicts_non_dict_column_unchanged(self, sink_factory):
        """Test that non-dict columns are not modified."""
        sink = sink_factory()

        df = pd.DataFrame({"col": [1, 2, 3]})
        sink._null_empty_dicts(df)

        assert list(df["col"]) == [1, 2, 3]


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

    def test_write_empty_batch_writes_and_registers_nothing(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        """An empty batch must not produce a 0-row parquet file, let alone
        register one in the manifest."""
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        sink.write(SinkBatch(topic="test", partition=0))  # must not raise

        mock_blob_client.put_object_async.assert_not_called()
        mock_catalog_client.post.assert_not_called()

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

    def test_write_does_not_leak_pandas_index_into_files_or_stats(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        """A groupby slice keeps the batch's row positions as its index; that
        index must not be materialised as an ``__index_level_0__`` column in
        the parquet file, nor show up as a numeric zone map in the manifest."""
        sink = sink_factory(
            hive_columns=["machine"],
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True

        # Interleave the partition values so the M1 group has index [0, 2]
        # (non-contiguous -> not a RangeIndex -> Arrow would store it).
        records = [
            {
                "value": {"machine": m, "v": i, "ts_ms": 1704067200000 + i},
                "key": f"k{i}",
                "timestamp": 1704067200000 + i,
                "offset": i,
            }
            for i, m in enumerate(["M1", "M2", "M1"])
        ]

        sink.write(sample_batch(records=records))

        for call in mock_blob_client.put_object_async.call_args_list:
            schema = pq.read_schema(io.BytesIO(call[0][1]))
            assert "__index_level_0__" not in schema.names
        body = mock_catalog_client.post.call_args.kwargs["json"]
        for f in body["files"]:
            assert "__index_level_0__" not in f["column_stats"]

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
        """RESTART to an existing virtual-partitioned table must NOT raise, for
        both catalog states the sink can meet: the physical-only spec this sink
        writes, and the legacy full spec (physical + virtual) an earlier version
        wrote alongside ``properties.virtual_partitions``."""
        sink = sink_factory(
            hive_columns=["year", "month", "~driver"],
            catalog_url="http://catalog:8080",
        )
        sink._catalog = mock_catalog_client

        sink._validate_partition_strategy({"partition_spec": ["year", "month"]})
        sink._validate_partition_strategy(
            {
                "partition_spec": ["year", "month", "driver"],
                "properties": {"virtual_partitions": ["driver"]},
            }
        )


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
        """An existing table is never re-created; sink-owned properties it is
        missing are synced onto it instead."""
        sink = sink_factory(
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client

        # Mock table already exists, without any sink-owned properties
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

        # The one PUT is a metadata sync, not a create: no created_by stamp,
        # the existing spec untouched, the sink's properties added.
        mock_catalog_client.put.assert_called_once()
        body = mock_catalog_client.put.call_args.kwargs["json"]
        assert "created_by" not in body["properties"]
        assert body["partition_spec"] == []
        assert body["properties"]["timestamp_column"] == "ts_ms"
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


# =============================================================================
# 6b. Catalog table metadata: physical-only spec, sink-owned properties synced
# =============================================================================


def _existing_table(mock_catalog_client, metadata):
    """Make the catalog report an existing table described by ``metadata``."""
    table = MagicMock(status_code=200)
    table.json.return_value = metadata
    health = MagicMock(status_code=200)
    mock_catalog_client.get.side_effect = lambda path, **kw: (
        health if "/health" in path else table
    )


class TestCatalogTableMetadata:
    """``partition_spec`` is physical-only — it is the folder tree the catalog
    validates every add-files call against, and a file can only carry physical
    partition values. The virtual tree, timestamp_column and sort_column live in
    sink-owned properties, which are synced to an existing table on start."""

    # What the catalog holds for a physical-only table another sink created,
    # including properties the sink does not own and must not disturb.
    META = {
        "location": "s3://bucket/ws/prefix/test_table",
        "schema": {"type": "struct", "fields": [{"name": "speed"}]},
        "partition_spec": ["year", "month"],
        "properties": {
            "created_by": "someone-else",
            "file_count": "12",
            "expected_partitions": ["year", "month"],
            "timestamp_column": "ts_ms",
        },
    }

    def _sink(self, sink_factory, mock_catalog_client, **kwargs):
        sink = sink_factory(catalog_url="http://catalog:8080", **kwargs)
        sink._catalog = mock_catalog_client
        return sink

    def test_create_sends_physical_spec_and_declares_virtual_levels(
        self, sink_factory, mock_catalog_client
    ):
        sink = self._sink(
            sink_factory,
            mock_catalog_client,
            hive_columns=["year", "month", "~driver"],
            sort_column="seq",
        )
        sink._register_table()

        body = mock_catalog_client.put.call_args.kwargs["json"]
        assert body["partition_spec"] == ["year", "month"]
        assert body["properties"] == {
            "created_by": "quixstreams-quix-lake-sink",
            "auto_discovered": "false",
            "expected_partitions": ["year", "month", "driver"],
            "virtual_partitions": ["driver"],
            "timestamp_column": "ts_ms",
            "sort_column": "seq",
        }
        assert sink.table_registered

    def test_existing_table_in_sync_is_left_alone(
        self, sink_factory, mock_catalog_client
    ):
        _existing_table(mock_catalog_client, self.META)
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "month"]
        )

        sink._register_table()

        mock_catalog_client.put.assert_not_called()
        assert sink.table_registered

    def test_adding_virtual_level_to_existing_table_updates_properties_only(
        self, sink_factory, mock_catalog_client
    ):
        # The table already has data under year=/month=. Adding ~driver (and a
        # sort_column) is a metadata change: no crash, no spec change, and
        # nothing the sink does not own is disturbed.
        _existing_table(mock_catalog_client, self.META)
        sink = self._sink(
            sink_factory,
            mock_catalog_client,
            hive_columns=["year", "month", "~driver"],
            sort_column="seq",
        )

        sink._register_table()  # must not raise

        body = mock_catalog_client.put.call_args.kwargs["json"]
        assert body["partition_spec"] == ["year", "month"]
        assert body["properties"] == {
            "created_by": "someone-else",
            "file_count": "12",
            "expected_partitions": ["year", "month", "driver"],
            "virtual_partitions": ["driver"],
            "timestamp_column": "ts_ms",
            "sort_column": "seq",
        }
        assert body["location"] == self.META["location"]
        assert body["schema"] == self.META["schema"]
        assert sink.table_registered

    def test_unsetting_sort_column_and_virtual_levels_removes_them(
        self, sink_factory, mock_catalog_client
    ):
        meta = {
            **self.META,
            "properties": {
                **self.META["properties"],
                "expected_partitions": ["year", "month", "driver"],
                "virtual_partitions": ["driver"],
                "sort_column": "seq",
            },
        }
        _existing_table(mock_catalog_client, meta)
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "month"]
        )

        sink._register_table()

        props = mock_catalog_client.put.call_args.kwargs["json"]["properties"]
        assert "sort_column" not in props  # falls back to timestamp_column
        assert "virtual_partitions" not in props
        assert props["expected_partitions"] == ["year", "month"]

    def test_legacy_virtual_entries_are_stripped_from_partition_spec(
        self, sink_factory, mock_catalog_client
    ):
        # An earlier sink version put the virtual level in partition_spec, which
        # makes the catalog reject every add-files call once the table has data
        # (no file carries a 'driver' partition value). Restart must not raise,
        # and must repair the spec.
        meta = {
            **self.META,
            "partition_spec": ["year", "month", "driver"],
            "properties": {
                **self.META["properties"],
                "expected_partitions": ["year", "month", "driver"],
                "virtual_partitions": ["driver"],
            },
        }
        _existing_table(mock_catalog_client, meta)
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "month", "~driver"]
        )

        sink._register_table()

        body = mock_catalog_client.put.call_args.kwargs["json"]
        assert body["partition_spec"] == ["year", "month"]
        assert body["properties"]["virtual_partitions"] == ["driver"]

    def test_physical_to_virtual_swap_is_rejected(
        self, sink_factory, mock_catalog_client
    ):
        # machine is foldered on disk; making it virtual would stop foldering it.
        meta = {**self.META, "partition_spec": ["year", "machine"]}
        _existing_table(mock_catalog_client, meta)
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "~machine"]
        )

        with pytest.raises(ValueError, match="Partition strategy mismatch"):
            sink._register_table()
        mock_catalog_client.put.assert_not_called()
        assert not sink.table_registered

    def test_virtual_to_physical_swap_is_rejected(
        self, sink_factory, mock_catalog_client
    ):
        meta = {
            **self.META,
            "partition_spec": ["year"],
            "properties": {**self.META["properties"], "virtual_partitions": ["driver"]},
        }
        _existing_table(mock_catalog_client, meta)
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "driver"]
        )

        with pytest.raises(ValueError, match="Partition strategy mismatch"):
            sink._register_table()
        mock_catalog_client.put.assert_not_called()

    def test_metadata_update_failure_raises(self, sink_factory, mock_catalog_client):
        _existing_table(mock_catalog_client, self.META)
        mock_catalog_client.put.return_value = MagicMock(status_code=500, text="boom")
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "month", "~driver"]
        )

        with pytest.raises(RuntimeError, match="Failed to update table"):
            sink._register_table()
        assert not sink.table_registered

    def test_manifest_partition_values_are_physical_only(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        # A file holds every driver, so it has no driver value to register; the
        # catalog checks these keys against the physical-only spec.
        sink = self._sink(
            sink_factory, mock_catalog_client, hive_columns=["year", "~driver"]
        )
        sink.table_registered = True
        batch = SinkBatch(topic="test", partition=0)
        for i, d in enumerate(["HAM", "VER"]):
            batch.append(
                value={"driver": d, "ts_ms": 1704067200000},
                key=f"k{i}",
                timestamp=1704067200000,
                headers=[],
                offset=i,
            )

        sink.write(batch)

        files = mock_catalog_client.post.call_args.kwargs["json"]["files"]
        assert len(files) == 1
        assert files[0]["partition_values"] == {"year": "2024"}


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

    def test_pending_uploads_cleared_on_failure(
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
        # Nothing landed, so there was nothing to discard.
        mock_blob_client.delete_objects.assert_not_called()

    def test_validate_structure_error_propagates(self, sink_factory, mock_blob_client):
        """Test that storage errors during structure validation propagate."""
        sink = sink_factory()

        # Make list_objects raise a storage error
        mock_blob_client.list_objects.side_effect = OSError("Storage unavailable")

        with pytest.raises(OSError, match="Storage unavailable"):
            sink._validate_existing_table_structure()


# =============================================================================
# 7b. Write attempt hygiene: retries must not leave orphans or duplicates
# =============================================================================


def _machines_batch():
    """Two partition groups (M1, M2) so one can succeed while the other fails."""
    batch = SinkBatch(topic="test", partition=0)
    for i, m in enumerate(["M1", "M2"]):
        batch.append(
            value={"machine": m, "v": i, "ts_ms": 1704067200000 + i},
            key=f"k{i}",
            timestamp=1704067200000 + i,
            headers=[],
            offset=i,
        )
    return batch


def _future(error: Exception = None) -> MagicMock:
    f = MagicMock()
    if error is not None:
        f.result.side_effect = error
    else:
        f.result.return_value = None
    return f


def _manifest_bodies(mock_catalog_client):
    return [
        c.kwargs["json"]
        for c in mock_catalog_client.post.call_args_list
        if "manifest" in str(c)
    ]


class TestWriteAttemptHygiene:
    """write() retries in two phases. A failed upload attempt must discard what
    it landed and forget what it queued; a failed manifest call must be retried
    on its own, never by re-uploading the data."""

    def test_manifest_failure_does_not_reupload_data(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        mock_catalog_client.post.side_effect = Exception("Manifest error")

        with pytest.raises(Exception, match="Manifest error"):
            sink.write(sample_batch())

        # The data landed exactly once; only the manifest call was retried.
        assert mock_blob_client.put_object_async.call_count == 1
        assert mock_catalog_client.post.call_count == 3
        # The catalog may have recorded the files before the error reached us,
        # so they are NOT deleted: the checkpoint replays the batch instead.
        mock_blob_client.delete_objects.assert_not_called()

    def test_transient_manifest_failure_retries_registration_only(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        ok = MagicMock(status_code=200)
        mock_catalog_client.post.side_effect = [Exception("blip"), ok]

        sink.write(sample_batch())  # must not raise

        assert mock_blob_client.put_object_async.call_count == 1
        bodies = _manifest_bodies(mock_catalog_client)
        assert len(bodies) == 2
        # Same file registered on both tries -> no fresh uuid was minted.
        assert bodies[0]["files"][0]["file_path"] == bodies[1]["files"][0]["file_path"]

    def test_failed_attempt_discards_the_files_it_landed(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(
            hive_columns=["machine"],
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        # M1 always lands, M2 always fails -> every attempt fails.
        mock_blob_client.put_object_async.side_effect = lambda key, _b: (
            _future(Exception("Upload failed")) if "machine=M2" in key else _future()
        )

        with pytest.raises(Exception, match="Upload failed"):
            sink.write(_machines_batch())

        uploads = mock_blob_client.put_object_async.call_args_list
        assert len(uploads) == 6  # 2 files x 3 attempts
        # Each attempt discarded exactly the M1 file IT uploaded.
        landed_m1 = [c[0][0] for c in uploads if "machine=M1" in c[0][0]]
        discarded = [c[0][0] for c in mock_blob_client.delete_objects.call_args_list]
        assert discarded == [[k] for k in landed_m1]
        mock_catalog_client.post.assert_not_called()

    def test_failed_attempt_discards_its_sidecars_and_forgets_them(
        self, sink_factory, mock_blob_client
    ):
        # Data upload fails, sidecar upload succeeds: the sidecar indexes a file
        # that never landed, so it must be deleted, and its future must not be
        # carried into the next attempt.
        sink = sink_factory(hive_columns=["year", "~driver"])
        mock_blob_client.put_object_async.side_effect = lambda key, _b: (
            _future() if "/.vidx/" in key else _future(Exception("Upload failed"))
        )
        batch = SinkBatch(topic="test", partition=0)
        batch.append(
            value={"driver": "HAM", "ts_ms": 1704067200000},
            key="k",
            timestamp=1704067200000,
            headers=[],
            offset=0,
        )

        with pytest.raises(Exception, match="Upload failed"):
            sink.write(batch)

        sidecar_keys = [
            c[0][0]
            for c in mock_blob_client.put_object_async.call_args_list
            if "/.vidx/" in c[0][0]
        ]
        assert len(sidecar_keys) == 3  # one per attempt, none carried over
        discarded = [c[0][0] for c in mock_blob_client.delete_objects.call_args_list]
        assert discarded == [[k] for k in sidecar_keys]
        assert sink._pending_sidecar_futures == []
        assert sink._pending_futures == []

    def test_submission_failure_midway_does_not_double_register_survivors(
        self, sink_factory, mock_blob_client, mock_catalog_client
    ):
        # Attempt 1 queues M1, then fails to even queue M2 (the executor call
        # itself raises). M1's future would otherwise still be pending when
        # attempt 2 runs and get registered alongside attempt 2's files.
        sink = sink_factory(
            hive_columns=["machine"],
            catalog_url="http://catalog:8080",
            auto_discover=True,
        )
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        calls = {"n": 0}

        def route(key, _b):
            calls["n"] += 1
            if calls["n"] == 2:  # 2nd submission of attempt 1 = M2
                raise RuntimeError("executor rejected the upload")
            return _future()

        mock_blob_client.put_object_async.side_effect = route

        sink.write(_machines_batch())  # attempt 2 succeeds

        uploads = [c[0][0] for c in mock_blob_client.put_object_async.call_args_list]
        assert len(uploads) == 4  # attempt 1: M1 + (M2 raised); attempt 2: M1, M2
        # Attempt 1's M1 was discarded...
        mock_blob_client.delete_objects.assert_called_once_with([uploads[0]])
        # ...and only attempt 2's two files reached the manifest.
        bodies = _manifest_bodies(mock_catalog_client)
        assert len(bodies) == 1
        registered = {f["file_path"].rsplit("/", 1)[1] for f in bodies[0]["files"]}
        assert registered == {u.rsplit("/", 1)[1] for u in uploads[2:]}

    def test_discard_failure_is_logged_not_raised(
        self, sink_factory, mock_blob_client, caplog
    ):
        sink = sink_factory(hive_columns=["machine"])
        mock_blob_client.put_object_async.side_effect = lambda key, _b: (
            _future(Exception("Upload failed")) if "machine=M2" in key else _future()
        )
        mock_blob_client.delete_objects.side_effect = OSError("storage down")

        # The ORIGINAL upload error surfaces, not the delete error.
        with pytest.raises(Exception, match="Upload failed"):
            sink.write(_machines_batch())
        assert "left as orphans" in caplog.text


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

    @pytest.mark.parametrize(
        "values",
        [
            [1.0, float("inf")],
            [float("-inf"), 2.0],
            [float("nan"), float("nan")],
        ],
        ids=["pos-inf", "neg-inf", "all-nan"],
    )
    def test_non_finite_bounds_skip_the_column(self, sink_factory, values):
        # +/-inf and NaN survive pc.min_max (NaN is not an Arrow null) and cannot
        # be JSON-encoded: ``requests`` serialises with allow_nan=False, so one
        # such value would make the manifest call raise before any HTTP
        # happens. The column is left unpruned; its neighbours are unaffected.
        sink = sink_factory()
        table = pa.table(
            {
                "x": pa.array(values, type=pa.float64()),
                "ok": pa.array([1, 2], type=pa.int64()),
            }
        )
        stats = sink._compute_column_stats(table)
        assert "x" not in stats
        assert stats["ok"] == {
            "type": "numeric",
            "min": 1.0,
            "max": 2.0,
            "null_count": 0,
            "value_count": 2,
        }

    def test_nan_alongside_finite_values_is_ignored(self, sink_factory):
        # Arrow's min_max skips NaN when finite values are present, so the
        # column keeps its (finite) zone map.
        sink = sink_factory()
        table = pa.table({"x": pa.array([1.0, float("nan"), 3.0], type=pa.float64())})
        stats = sink._compute_column_stats(table)
        assert stats["x"]["min"] == 1.0
        assert stats["x"]["max"] == 3.0

    def test_write_with_inf_value_produces_json_safe_manifest(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        # End-to-end: a poison value in the batch must not make the manifest
        # body unserialisable (which would fail before any HTTP call and drag
        # the whole write into the retry loop, re-uploading the parquet).
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        records = [
            {
                "value": {"speed": float("inf"), "rpm": 7, "ts_ms": 1704067200000},
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 0,
            }
        ]

        sink.write(sample_batch(records=records))

        # Exactly one upload -> the write did not retry.
        assert mock_blob_client.put_object_async.call_count == 1
        body = mock_catalog_client.post.call_args.kwargs["json"]
        json.dumps(body, allow_nan=False)  # what requests does; must not raise
        cs = body["files"][0]["column_stats"]
        assert "speed" not in cs
        assert cs["rpm"]["min"] == 7.0

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

    def test_stats_columns_empty_list_disables_stats(self, sink_factory):
        # ``[]`` is the only way to switch stats off; it must not collapse into
        # None ("every column"), which is the opposite of the caller's intent.
        sink = sink_factory(stats_columns=[])
        table = pa.table({"speed": pa.array([1, 2], type=pa.int64())})
        assert sink._compute_column_stats(table) == {}

    def test_stats_not_computed_without_a_catalog(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        # The catalog is the only consumer of zone maps; the default
        # (no catalog) config must not compute them just to throw them away.
        sink = sink_factory()
        with patch.object(sink, "_compute_column_stats") as compute:
            sink.write(sample_batch())
        compute.assert_not_called()

    def test_stats_computed_when_a_catalog_is_configured(
        self, sink_factory, sample_batch, mock_blob_client, mock_catalog_client
    ):
        sink = sink_factory(catalog_url="http://catalog:8080", auto_discover=True)
        sink._catalog = mock_catalog_client
        sink.table_registered = True
        with patch.object(sink, "_compute_column_stats", return_value={}) as compute:
            sink.write(sample_batch())
        compute.assert_called_once()

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

    # -- Sidecar schema stability --------------------------------------------
    # The documented read path is one glob over every file's sidecar
    # (``read_parquet('.../.vidx/*.parquet')``), which fails outright if two
    # files disagree on a column's type or presence. pa.concat_tables is the
    # in-process proxy for that glob: it raises on any schema mismatch.

    def _sidecar_tables(self, mock_blob_client):
        _, sidecars = self._uploads(mock_blob_client)
        return [pq.read_table(io.BytesIO(c[0][1])) for c in sidecars]

    def test_sidecar_schema_is_stable_across_batches(
        self, sink_factory, mock_blob_client
    ):
        sink = sink_factory(hive_columns=["year", "~driver", "~session"])
        # Batch A: driver is an int, session present. Batch B: driver is a
        # string, session absent. Per-batch inference would give two schemas.
        sink.write(
            self._batch(
                [
                    {
                        "value": {
                            "driver": 44,
                            "session": "Q1",
                            "ts_ms": 1704067200000,
                        },
                        "key": "k1",
                        "timestamp": 1704067200000,
                        "offset": 0,
                    }
                ]
            )
        )
        sink.write(
            self._batch(
                [
                    {
                        "value": {"driver": "HAM", "ts_ms": 1704067200000},
                        "key": "k2",
                        "timestamp": 1704067200000,
                        "offset": 1,
                    }
                ]
            )
        )

        a, b = self._sidecar_tables(mock_blob_client)
        assert a.schema.equals(b.schema)
        # Every configured virtual column + every physical column, all utf8.
        assert a.schema.names == ["driver", "session", "year"]
        assert all(t == pa.string() for t in a.schema.types)
        merged = pa.concat_tables([a, b])  # the glob; must not raise
        assert merged.column("driver").to_pylist() == ["44", "HAM"]
        assert merged.column("session").to_pylist() == ["Q1", None]
        assert merged.column("year").to_pylist() == ["2024", "2024"]

    def test_sidecar_ids_render_identically_with_and_without_nulls(
        self, sink_factory, mock_blob_client
    ):
        # pandas upcasts an int column to float64 as soon as a null joins it;
        # the sidecar must still say "42", not "42.0", or the same driver
        # shows up twice in the tree.
        sink = sink_factory(hive_columns=["year", "~driver"])
        records = [
            {
                "value": {"driver": d, "ts_ms": 1704067200000},
                "key": f"k{i}",
                "timestamp": 1704067200000,
                "offset": i,
            }
            for i, d in enumerate([42, None])
        ]
        sink.write(self._batch(records))
        (t,) = self._sidecar_tables(mock_blob_client)
        assert t.column("driver").to_pylist() == ["42", None]

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
        # Submit succeeds; the future raises when awaited (_settle_uploads).
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
        # partition_spec is the PHYSICAL tree only; the virtual level is
        # declared in properties (full order + which entries are virtual).
        assert body["partition_spec"] == ["year", "month"]
        assert body["properties"]["expected_partitions"] == ["year", "month", "driver"]
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
