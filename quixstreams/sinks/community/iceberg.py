from dataclasses import dataclass
import logging
import random
import time
from datetime import datetime
from io import BytesIO
from typing import Optional, Literal, get_args
from importlib import import_module


from quixstreams.sinks import SinkBatch, BatchingSink, SinkBackpressureError

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.schema import Schema, NestedField
    from pyiceberg.types import StringType, TimestampType
    from pyiceberg.exceptions import CommitFailedException  # Import the exception
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        f"do 'pip install quixstreams[iceberg]' to use this sink"
    ) from exc


logger = logging.getLogger(__name__)

DataCatalogSpec = Literal["aws_glue"]


@dataclass
class IcebergSinkConfig:
    location: str
    auth: dict


class IcebergAWSConfig(IcebergSinkConfig):
    def __init__(
        self,
        aws_s3_uri: str,
        aws_region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        """
        :param aws_s3_uri: The S3 URI where the table data will be stored
            (e.g., 's3://your-bucket/warehouse/').
        :param aws_region: The AWS region for the S3 bucket and Glue catalog.
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
            when using AWS Glue.
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
            when using AWS Glue.
        :param aws_session_token: a session token (or will be generated for you).
            NOTE: can alternatively set the AWS_SESSION_TOKEN environment variable when
            using AWS Glue.
        """
        self.location = aws_s3_uri
        self.auth = {
            "client.region": aws_region,
            "client.access-key-id": aws_access_key_id,
            "client.secret-access-key": aws_secret_access_key,
            "client.session-token": aws_session_token,
        }


class IcebergSink(BatchingSink):
    """
    IcebergSink writes batches of data to an Apache Iceberg table.

    The data will by default include the kafka message key, value, and timestamp.

    It serializes incoming data batches into Parquet format and appends them to the
    Iceberg table, updating the table schema as necessary.

    Currently supports Apache Iceberg hosted in:
    - AWS

    Currently supports data catalogs:
    - AWS Glue
    """

    def __init__(
        self,
        table_name: str,
        config: IcebergAWSConfig,
        data_catalog_spec: DataCatalogSpec,
        schema: Optional[Schema] = None,
        partition_spec: Optional[PartitionSpec] = None,
    ):
        """
        :param table_name: The name of the Iceberg table.
        :param config: An IcebergConfig with all the various connection parameters.
        :param data_catalog_spec: data cataloger to use (ex. for AWS Glue, "aws_glue").
        :param schema: The Iceberg table schema. If None, a default schema is used.
        :param partition_spec: The partition specification for the table.
            If None, a default is used.

        Example setup using an AWS-hosted Iceberg with AWS Glue:

        from quixstreams.sinks.community.iceberg import IcebergSink, IcebergAWSConfig

        iceberg_config = IcebergAWSConfig(
            aws_s3_uri="", aws_region="", aws_access_key_id="", aws_secret_access_key=""
        )

        iceberg_sink = IcebergSink(
            table_name="glue.sink-test",
            config=iceberg_config,
            data_catalog_spec="aws_glue",
        )

        app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
        topic = app.topic('sink_topic')
        sdf = app.dataframe(topic=topic).print(metadata=True)
        sdf.sink(iceberg_sink)

        if __name__ == "__main__":
            app.run()
        """

        super().__init__()
        self.iceberg_config = config

        # Configure Iceberg Catalog
        data_catalog_cls = _import_data_catalog(data_catalog_spec)
        self.data_catalog = data_catalog_cls(
            name=f"{data_catalog_spec}_catalog",
            **self.iceberg_config.auth,
        )

        # Set up the schema.
        if schema is None:
            # Define a default schema if none is provided.
            schema = Schema(
                fields=(
                    NestedField(
                        field_id=1,
                        name="_timestamp",
                        field_type=TimestampType(),
                        required=False,
                    ),
                    NestedField(
                        field_id=2, name="_key", field_type=StringType(), required=False
                    ),
                )
            )

        # Set up the partition specification.
        if partition_spec is None:
            # Map field names to field IDs from the schema.
            field_ids = {field.name: field.field_id for field in schema.fields}

            # Create partition fields for kafka key and timestamp.
            partition_fields = (
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
            )

            # Create the new PartitionSpec.
            partition_spec = PartitionSpec(fields=partition_fields)

            # Create the Iceberg table if it doesn't exist.
            self.table = self.data_catalog.create_table_if_not_exists(
                identifier=table_name,
                schema=schema,
                location=self.iceberg_config.location,
                partition_spec=partition_spec,
                properties={"write.distribution-mode": "fanout"},
            )
            logger.info(
                f"Loaded Iceberg table '{table_name}' at '{self.iceberg_config.location}'."
            )

    def write(self, batch: SinkBatch):
        """
        Writes a batch of data to the Iceberg table.
        Implements retry logic to handle concurrent write conflicts.

        :param batch: The batch of data to write.
        """
        try:
            # Serialize batch data into Parquet format.
            data = _serialize_batch_values(batch)

            # Read data into a PyArrow Table.
            input_buffer = pa.BufferReader(data)
            parquet_table = pq.read_table(input_buffer)

            # Reload the table to get the latest metadata
            self.table = self.data_catalog.load_table(self.table.name())

            # Update the table schema if necessary.
            with self.table.update_schema() as update:
                update.union_by_name(parquet_table.schema)

            append_start_epoch = time.time()
            self.table.append(parquet_table)
            logger.info(
                f"Appended {len(list(batch))} records to {self.table.name()} table "
                f"in {time.time() - append_start_epoch}s."
            )

        except CommitFailedException as e:
            # Handle commit conflict
            logger.warning(f"Commit conflict detected.: {e}")
            # encourage staggered backoff
            sleep_time = random.uniform(0, 5)  # noqa: S311
            raise SinkBackpressureError(sleep_time, batch.topic, batch.partition)
        except Exception as e:
            logger.error(f"Error writing data to Iceberg table: {e}")
            raise


def _serialize_batch_values(batch: SinkBatch) -> bytes:
    """
    Dynamically unpacks each kafka message's value (its dict keys/"columns") within the
    provided batch and preps the messages for reading into a PyArrow Table.
    """
    # TODO: Handle data flattening. Nested properties will cause this to crash.
    # TODO: possible optimizations with all the iterative batch transformations

    # Get all unique "keys" (columns) across all rows
    all_keys = set()
    for row in batch:
        all_keys.update(row.value.keys())

    # Normalize rows: Ensure all rows have the same keys, filling missing ones with None
    normalized_values = [
        {key: row.value.get(key, None) for key in all_keys} for row in batch
    ]

    columns = {
        "_timestamp": [datetime.fromtimestamp(row.timestamp / 1000.0) for row in batch],
        "_key": [
            row.key.decode() if isinstance(row.key, bytes) else row.key for row in batch
        ],
    }

    # Convert normalized values to a pyarrow Table
    columns = {
        **columns,
        **{key: [row[key] for row in normalized_values] for key in all_keys},
    }

    table = pa.Table.from_pydict(columns)

    with BytesIO() as f:
        pq.write_table(table, f, compression="snappy")
        return f.getvalue()


def _import_data_catalog(data_catalog_spec):
    """
    A way to dynamically load data catalogs which may require other imports
    """
    if data_catalog_spec not in get_args(DataCatalogSpec):
        raise ValueError(f"Unsupported data_catalog_spec: {data_catalog_spec}")

    data_catalogs = {"aws_glue": ("[iceberg_aws]", "glue.GlueCatalog")}

    install_name, module_path = data_catalogs[data_catalog_spec]
    module, catalog_cls_name = module_path.split(".")
    try:
        return getattr(import_module(f"pyiceberg.catalog.{module}"), catalog_cls_name)
    except ImportError as exc:
        raise ImportError(
            f"Package {exc.name} is missing: "
            f"do 'pip install quixstreams{install_name}' to use "
            f"data_catalog_spec {data_catalog_spec}"
        ) from exc
