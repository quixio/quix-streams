"""
Tests for QuixTSDataLakeSink

Comprehensive unit and integration tests for the Quix Lake Blob Storage Sink,
covering initialization, timestamp mapping, partition handling, write operations,
catalog integration, and error handling.
"""

import io
import os
import sys
import tempfile
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
# 9. Integer Precision Tests
# =============================================================================


class TestIntegerPrecision:
    """Tests that large integers (> 2^53) survive the full write-read cycle.

    Float64 can only represent integers exactly up to 2^53 (~9.0e15).
    Values like nanosecond timestamps (~1.7e18) must be stored as int64
    in Parquet to avoid silent rounding.

    Every test seeds EXACT values and asserts the EXACT same values come back.
    No NaN seeding — all rows have identical schemas unless explicitly testing gaps.
    """

    # These values are deliberately chosen to FAIL under float64:
    #   float(1773824667000000002) == 1773824667000000000.0  (last digits lost)
    LARGE_INTS = [
        1773824667000000002,
        1773824667000000005,
        1773824667000000009,
    ]

    def _capture_parquet_bytes(self, mock_blob_client) -> bytes:
        """Extract Parquet bytes from the mock blob client's upload call."""
        call_args = mock_blob_client.put_object_async.call_args
        return call_args[0][1]

    def _make_records(self, values, extra_fields=None):
        """Build records with consistent schema — no missing keys, no NaN."""
        records = []
        for i, v in enumerate(values):
            value_dict = {"ts_ns": v, "label": f"row{i}"}
            if extra_fields:
                value_dict.update(extra_fields)
            records.append(
                {
                    "value": value_dict,
                    "key": f"k{i}",
                    "timestamp": 1704067200000,
                    "offset": i,
                }
            )
        return records

    def test_large_int_seed_and_query_exact_values(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Seed 1773824667000000002, query back 1773824667000000002. No NaN."""
        sink = sink_factory(hive_columns=[])
        batch = sample_batch(records=self._make_records(self.LARGE_INTS))
        sink.write(batch)

        parquet_bytes = self._capture_parquet_bytes(mock_blob_client)
        table = pq.read_table(io.BytesIO(parquet_bytes))

        # Assert Arrow type is int64, NOT double/float64
        assert pa.types.is_int64(table.schema.field("ts_ns").type), (
            f"Expected int64, got {table.schema.field('ts_ns').type}"
        )

        # Assert exact values — not float-rounded
        read_values = table.column("ts_ns").to_pylist()
        for expected, actual in zip(self.LARGE_INTS, read_values):
            assert actual == expected, (
                f"Precision lost: seeded {expected}, got back {actual}"
            )

    def test_large_int_duckdb_query(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Write Parquet, query with DuckDB, verify exact int64 values."""
        duckdb = pytest.importorskip("duckdb")

        sink = sink_factory(hive_columns=[])
        batch = sample_batch(records=self._make_records(self.LARGE_INTS))
        sink.write(batch)

        parquet_bytes = self._capture_parquet_bytes(mock_blob_client)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            f.write(parquet_bytes)
            tmp_path = f.name

        try:
            conn = duckdb.connect()
            result = conn.execute(
                f"SELECT ts_ns FROM read_parquet('{tmp_path}') ORDER BY ts_ns"
            ).fetchall()
            queried_values = [row[0] for row in result]

            for expected, actual in zip(sorted(self.LARGE_INTS), queried_values):
                assert actual == expected, (
                    f"DuckDB precision lost: seeded {expected}, got back {actual}"
                )

            # Also verify DuckDB sees BIGINT, not DOUBLE
            schema = conn.execute(
                f"DESCRIBE SELECT ts_ns FROM read_parquet('{tmp_path}')"
            ).fetchall()
            col_type = schema[0][1]
            assert "INT" in col_type.upper(), (
                f"Expected integer type in DuckDB, got {col_type}"
            )
        finally:
            os.unlink(tmp_path)

    def test_large_int_with_nan_gaps(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Large integers stay exact even when other rows are missing the column."""
        sink = sink_factory(hive_columns=[])

        records = [
            {
                "value": {"ts_ns": 1773824667000000002, "label": "has_ts"},
                "key": "k0",
                "timestamp": 1704067200000,
                "offset": 0,
            },
            {
                "value": {"label": "no_ts"},  # ts_ns missing → NaN gap
                "key": "k1",
                "timestamp": 1704067200000,
                "offset": 1,
            },
            {
                "value": {"ts_ns": 1773824667000000009, "label": "has_ts2"},
                "key": "k2",
                "timestamp": 1704067200000,
                "offset": 2,
            },
        ]
        batch = sample_batch(records=records)
        sink.write(batch)

        parquet_bytes = self._capture_parquet_bytes(mock_blob_client)
        table = pq.read_table(io.BytesIO(parquet_bytes))

        assert pa.types.is_int64(table.schema.field("ts_ns").type), (
            f"Expected int64, got {table.schema.field('ts_ns').type}"
        )
        assert table.column("ts_ns").to_pylist() == [
            1773824667000000002,
            None,
            1773824667000000009,
        ]

    def test_large_int_with_partitions(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Large integers survive when Hive partitioning is enabled. No NaN."""
        sink = sink_factory(hive_columns=["year", "month"])

        records = self._make_records(
            self.LARGE_INTS, extra_fields={"ts_ms": 1704067200000}
        )
        batch = sample_batch(records=records)
        sink.write(batch)

        parquet_bytes = self._capture_parquet_bytes(mock_blob_client)
        table = pq.read_table(io.BytesIO(parquet_bytes))

        assert pa.types.is_int64(table.schema.field("ts_ns").type)
        assert table.column("ts_ns").to_pylist() == self.LARGE_INTS

    def test_large_int_parquet_file_roundtrip(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Write to real file, read back with PyArrow + pandas. No NaN."""
        sink = sink_factory(hive_columns=[])
        batch = sample_batch(records=self._make_records(self.LARGE_INTS))
        sink.write(batch)

        parquet_bytes = self._capture_parquet_bytes(mock_blob_client)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            f.write(parquet_bytes)
            tmp_path = f.name

        try:
            table = pq.read_table(tmp_path)
            assert pa.types.is_int64(table.schema.field("ts_ns").type)
            assert table.column("ts_ns").to_pylist() == self.LARGE_INTS

            df = table.to_pandas()
            for expected, actual in zip(self.LARGE_INTS, df["ts_ns"]):
                assert int(actual) == expected, (
                    f"Precision lost: seeded {expected}, got {int(actual)}"
                )
        finally:
            os.unlink(tmp_path)

    def test_mixed_int_sizes_preserve_precision(
        self, sink_factory, sample_batch, mock_blob_client
    ):
        """Small ints, large ints, zero, negative — all exact. No NaN."""
        sink = sink_factory(hive_columns=[])

        values = [42, 1773824667000000002, 0, 1773824667000000009, -1]
        records = [
            {
                "value": {"num": v, "label": f"row{i}"},
                "key": f"k{i}",
                "timestamp": 1704067200000,
                "offset": i,
            }
            for i, v in enumerate(values)
        ]
        batch = sample_batch(records=records)
        sink.write(batch)

        parquet_bytes = self._capture_parquet_bytes(mock_blob_client)
        table = pq.read_table(io.BytesIO(parquet_bytes))

        assert pa.types.is_int64(table.schema.field("num").type)
        assert table.column("num").to_pylist() == values
