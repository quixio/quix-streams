import logging
from quixstreams.sinks import SinkBatch, BatchingSink, SinkBackpressureError
from quixstreams.logging import LogLevel
from typing import Any, List, Literal, Optional
from datetime import datetime
from io import BytesIO

import time  # For sleep in backoff
import random  # For jitter in backoff


try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.schema import Schema, NestedField
    from pyiceberg.types import StringType, TimestampType
    from pyiceberg.exceptions import CommitFailedException  # Import the exception
except ImportError as exc:
    raise ImportError(
        'Package "pyarrow" is missing: '
        "run pip install quixstreams[iceberg] to fix it"
    ) from exc


DataCatalogSpec = Literal["aws_glue"]


class IcebergSink(BatchingSink):
    """
    IcebergSink is a sink that writes batches of data to an Apache Iceberg table stored in AWS S3,
    using the AWS Glue Data Catalog as a default catalog (only implemented for now).

    It serializes incoming data batches into Parquet format and appends them to the Iceberg table,
    updating the table schema as necessary on fly.
    """

    def __init__(
        self,
        table_name: str,
        aws_s3_uri: str,
        s3_region_name: Optional[str] = None,
        data_catalog_spec: DataCatalogSpec = "aws_glue",
        schema: Optional[Schema] = None,
        partition_spec: Optional[PartitionSpec] = None,
        loglevel: LogLevel = "INFO",
    ):
        """
        Initializes the S3Sink with the specified configuration.

        Parameters:
            table_name (str): The name of the Iceberg table.
            aws_s3_uri (str): The S3 URI where the table data will be stored (e.g., 's3://your-bucket/warehouse/').
            s3_region_name (Optional[str]): The AWS region where the S3 bucket and Glue catalog are located.
            data_catalog_spec (DataCatalogSpec): The data catalog specification to use (default is 'aws_glue').
            schema (Optional[Schema]): The Iceberg table schema. If None, a default schema is used.
            partition_spec (Optional[PartitionSpec]): The partition specification for the table. If None, a default is used.
            format: The data serialization format to use (default is ParquetFormat()).
            loglevel (LogLevel): The logging level for the logger (default is 'INFO').
        """
        super().__init__()

        # Configure logging.
        self._logger = logging.getLogger("IcebergSink")
        log_format = (
            "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(name)s] : %(message)s"
        )
        logging.basicConfig(format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
        self._logger.setLevel(loglevel)

        # Initialize the Iceberg catalog.
        if data_catalog_spec == "aws_glue":
            # Configure Iceberg Catalog using AWS Glue.
            self.catalog = GlueCatalog(name="glue_catalog", region_name=s3_region_name)
        else:
            raise ValueError(f"Unsupported data_catalog_spec: {data_catalog_spec}")

        # Set up the schema.
        if schema is None:
            # Define a default schema if none is provided.
            schema = Schema(
                NestedField(1, "_timestamp", TimestampType(), required=False),
                NestedField(2, "_key", StringType(), required=False),
            )

        # Set up the partition specification.
        if partition_spec is None:
            # Map field names to field IDs from the schema.
            field_ids = {field.name: field.field_id for field in schema.fields}

            # Create partition fields.
            partition_fields = [
                PartitionField(
                    source_id=field_ids["_key"],
                    field_id=1000,  # Unique partition field ID.
                    transform=IdentityTransform(),
                    name="_key",
                ),
                PartitionField(
                    source_id=field_ids["_timestamp"],
                    field_id=1001,
                    transform=DayTransform(),
                    name="day",
                ),
            ]

            # Create the new PartitionSpec.
            partition_spec = PartitionSpec(schema=schema, fields=partition_fields)

            # Create the Iceberg table if it doesn't exist.
            self.table = self.catalog.create_table_if_not_exists(
                identifier=table_name,
                schema=schema,
                location=aws_s3_uri,
                partition_spec=partition_spec,
                properties={"write.distribution-mode": "fanout"},
            )
            self._logger.info(f"Loaded Iceberg table '{table_name}' at '{aws_s3_uri}'.")

    def write(self, batch: SinkBatch):
        """
        Writes a batch of data to the Iceberg table.
        Implements retry logic to handle concurrent write conflicts.

        Parameters:
            batch (SinkBatch): The batch of data to write.
        """
        try:
            # Serialize batch data into Parquet format.
            data = self._format.serialize_batch_values(batch)

            # Read data into a PyArrow Table.
            input_buffer = pa.BufferReader(data)
            parquet_table = pq.read_table(input_buffer)

            # Reload the table to get the latest metadata
            self.table = self.catalog.load_table(self.table.name())

            # Update the table schema if necessary.
            with self.table.update_schema() as update:
                update.union_by_name(parquet_table.schema)

            append_start_epoch = time.time()
            self.table.append(parquet_table)
            self._logger.info(
                f"Appended {len(list(batch))} records to {self.table.name()} table in {time.time() - append_start_epoch}s."
            )

        except CommitFailedException as e:
            # Handle commit conflict
            self._logger.warning(f"Commit conflict detected.: {e}")
            sleep_time = random.uniform(0, 5)  # noqa: S311
            raise SinkBackpressureError(sleep_time, batch.topic, batch.partition)
        except Exception as e:
            self._logger.error(f"Error writing data to Iceberg table: {e}")
            raise

    def _serialize_batch_values(self, values: List[Any]) -> bytes:
        # TODO: Handle data flattening. Nested properties will cause this to crash.

        # Get all unique keys (columns) across all rows
        all_keys = set()
        for row in values:
            all_keys.update(row.value.keys())

        # Normalize rows: Ensure all rows have the same keys, filling missing ones with None
        normalized_values = [
            {key: row.value.get(key, None) for key in all_keys} for row in values
        ]

        columns = {
            "_timestamp": [
                datetime.fromtimestamp(row.timestamp / 1000.0) for row in values
            ],
            "_key": [bytes.decode(row.key) for row in values],
        }

        # Convert normalized values to a pyarrow Table
        columns = {
            **columns,
            **{key: [row[key] for row in normalized_values] for key in all_keys},
        }

        table = pa.Table.from_pydict(columns)

        with BytesIO() as f:
            pq.write_table(table, f, compression=self._compression_type)
            value_bytes = f.getvalue()

            # if self._compress and self._compression_type == "none":  # Handle manual gzip if no Parquet compression
            # value_bytes = gzip.compress(value_bytes)

            return value_bytes
