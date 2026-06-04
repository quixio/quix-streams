"""
Tests for QuixTSDataLakeSink

Comprehensive unit and integration tests for the Quix Lake Blob Storage Sink,
covering initialization, timestamp mapping, partition handling, write operations,
catalog integration, and error handling.
"""

import io
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
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

    def test_write_empty_batch_handled(self, sink_factory, mock_blob_client):
        """Test that empty batch is handled gracefully (writes empty parquet)."""
        sink = sink_factory()
        batch = SinkBatch(topic="test", partition=0)

        # Should not raise
        sink.write(batch)

        # Note: The sink writes an empty parquet file for empty batches.
        # This is the current behavior - verify it completes without error.
        mock_blob_client.put_object_async.assert_called_once()

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
