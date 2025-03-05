import json
import logging
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Optional

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    from google.oauth2 import service_account
except ImportError as exc:
    raise ImportError(
        'Package "google-cloud-bigquery" is missing: '
        "run pip install quixstreams[bigquery] to fix it"
    ) from exc

from quixstreams.exceptions import QuixException
from quixstreams.models import HeadersTuples
from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBatch,
)

__all__ = ("BigQuerySink", "BigQuerySinkException")

logger = logging.getLogger(__name__)

# A column name for the records keys
_KEY_COLUMN_NAME = "__key"

# A column name for the records timestamps
_TIMESTAMP_COLUMN_NAME = "timestamp"

# A mapping of Python types to BigQuery column types for schema updates
_BIGQUERY_TYPES_MAP: dict[type, str] = {
    int: "FLOAT64",
    float: "FLOAT64",
    Decimal: "BIGNUMERIC",
    str: "STRING",
    bytes: "BYTES",
    datetime: "DATETIME",
    date: "DATE",
    list: "JSON",
    dict: "JSON",
    tuple: "JSON",
    bool: "BOOLEAN",
}


class BigQuerySinkException(QuixException): ...


class BigQuerySink(BatchingSink):
    def __init__(
        self,
        project_id: str,
        location: str,
        dataset_id: str,
        table_name: str,
        service_account_json: Optional[str] = None,
        schema_auto_update: bool = True,
        ddl_timeout: float = 10.0,
        insert_timeout: float = 10.0,
        retry_timeout: float = 30.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        **kwargs,
    ):
        """
        A connector to sink processed data to Google Cloud BigQuery.

        It batches the processed records in memory per topic partition, and flushes them to BigQuery at the checkpoint.

        >***NOTE***: BigQuerySink can accept only dictionaries.
        > If the record values are not dicts, you need to convert them to dicts before
        > sinking.

        The column names and types are inferred from individual records.
        Each key in the record's dictionary will be inserted as a column to the resulting BigQuery table.

        If the column is not present in the schema, the sink will try to add new nullable columns on the fly with types inferred from individual values.
        The existing columns will not be affected.
        To disable this behavior, pass `schema_auto_update=False` and define the necessary schema upfront.
        The minimal schema must define two columns: "timestamp" of type TIMESTAMP, and "__key" with a type of the expected message key.

        :param project_id: a Google project id.
        :param location: a BigQuery location.
        :param dataset_id: a BigQuery dataset id.
            If the dataset does not exist, the sink will try to create it.
        :param table_name: BigQuery table name.
            If the table does not exist, the sink will try to create it with a default schema.
        :param service_account_json: an optional JSON string with service account credentials
            to connect to BigQuery.
            The internal `google.cloud.bigquery.Client` will use the Application Default Credentials if not provided.
            See https://cloud.google.com/docs/authentication/provide-credentials-adc for more info.
            Default - `None`.
        :param schema_auto_update: if True, the sink will try to create a dataset and a table if they don't exist.
            It will also add missing columns on the fly with types inferred from individual values.
        :param ddl_timeout: a timeout for a single DDL operation (adding tables, columns, etc.).
            Default - 10s.
        :param insert_timeout: a timeout for a single INSERT operation.
            Default - 10s.
        :param retry_timeout: a total timeout for each request to BigQuery API.
            During this timeout, a request can be retried according
            to the client's default retrying policy.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        :param kwargs: Additional keyword arguments passed to `bigquery.Client`.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self.location = location
        self.project_id = project_id
        self.dataset_id = f"{self.project_id}.{dataset_id}"
        self.table_id = f"{self.dataset_id}.{table_name}"
        self.ddl_timeout = ddl_timeout
        self.insert_timeout = insert_timeout
        self.retry = bigquery.DEFAULT_RETRY.with_timeout(timeout=retry_timeout)
        self.schema_auto_update = schema_auto_update

        kwargs["project"] = self.project_id
        if service_account_json is not None:
            # Parse the service account credentials from JSON
            service_account_info = json.loads(service_account_json, strict=False)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            kwargs["credentials"] = credentials
        self._client: Optional[bigquery.Client] = None
        self._client_settings = kwargs

    def setup(self):
        if not self._client:
            self._client = bigquery.Client(**self._client_settings)
            logger.info("Successfully authenticated to BigQuery.")
            if self.schema_auto_update:
                # Initialize a table in BigQuery if it doesn't exist already
                self._init_table()

    def write(self, batch: SinkBatch):
        rows = []
        cols_types = {}

        for item in batch:
            row = {}
            # Check the message key type and add it to the row if it's not None
            if item.key is not None:
                key_type = type(item.key)
                cols_types.setdefault(_KEY_COLUMN_NAME, key_type)
                row[_KEY_COLUMN_NAME] = item.key

            # Iterate over keys in the value dictionary and collect their types
            # to add new columns to the schema.
            # None values are skipped from inserting.
            for key, value in item.value.items():
                if value is not None:
                    # Collect types for all keys to add new columns
                    # with proper column types
                    cols_types.setdefault(key, type(value))
                    row[key] = value

            # Add timestamp in seconds (BigQuery expects it this way)
            row[_TIMESTAMP_COLUMN_NAME] = item.timestamp / 1000
            rows.append(row)

        table = self._client.get_table(self.table_id, timeout=self.ddl_timeout)
        if self.schema_auto_update:
            self._add_new_columns(table=table, columns=cols_types)
        self._insert_rows(table=table, rows=rows)

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        if not isinstance(value, Mapping):
            raise TypeError(
                f'Sink "{self.__class__.__name__}" supports only dictionaries,'
                f" got {type(value)}"
            )
        return super().add(
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    def _init_table(self):
        """
        Initialize dataset and schema in BigQuery.
        If either dataset or schema don't exist, they will be created.
        """

        # Ensure the dataset exists in BigQuery
        try:
            self._client.get_dataset(self.dataset_id)
            logger.debug(f'Dataset "{self.dataset_id}" already exists')
        except NotFound:
            logger.debug(f'Creating dataset "{self.dataset_id}"')
            dataset = bigquery.Dataset(self.dataset_id)
            dataset.location = self.location
            self._client.create_dataset(dataset, timeout=self.ddl_timeout)
            logger.debug(f'Created dataset "{self.dataset_id}"')

        # Ensure the table exists in BigQuery
        try:
            self._client.get_table(self.table_id, timeout=self.ddl_timeout)
            logger.debug(f'Table "{self.table_id}" already exists')
        except NotFound:
            logger.debug(f'Creating table "{self.table_id}"')
            schema = [
                bigquery.SchemaField(
                    name="timestamp", field_type="TIMESTAMP", mode="REQUIRED"
                )
            ]
            table = bigquery.Table(self.table_id, schema=schema)
            self._client.create_table(table, timeout=self.ddl_timeout)
            logger.debug(f'Created table "{self.table_id}"')

    def _add_new_columns(self, table: bigquery.Table, columns: dict[str, type]):
        """
        Add new columns to BigQuery table in case they don't exist.
        The existing columns will not be affected.

        :param table: a BigQuery table.
        :param columns: a mapping {<name>: <python type>} for all columns
            in the INSERT query.
        """

        original_schema = table.schema
        original_columns = set(c.name for c in table.schema)
        columns_to_add = []

        # Iterate over all columns and add new ones if they don't already exist
        for col_name, py_type in columns.items():
            # Map Python type to BigQuery type for each column
            if col_name not in original_columns:
                bigquery_col_type = _BIGQUERY_TYPES_MAP.get(py_type)
                if bigquery_col_type is None:
                    raise BigQuerySinkException(
                        f'Failed to add new column "{col_name}": cannot map '
                        f'Python type "{py_type}" to the BigQuery column type'
                    )
                columns_to_add.append(bigquery.SchemaField(col_name, bigquery_col_type))

        # Update the table schema in BigQuery
        if columns_to_add:
            new_schema = original_schema[:]
            new_schema.extend(columns_to_add)
            table.schema = new_schema

            columns_str = ", ".join(
                f'"{c.name} ({c.field_type})"' for c in columns_to_add
            )
            logger.info(
                f"Adding new columns "
                f'to the table "{table.full_table_id}": {columns_str}'
            )
            self._client.update_table(
                table, fields=["schema"], timeout=self.ddl_timeout, retry=self.retry
            )

    def _insert_rows(self, table: bigquery.Table, rows: list[dict]):
        _start = time.monotonic()
        errors = self._client.insert_rows(
            table=table,
            rows=rows,
            retry=self.retry,
            timeout=self.insert_timeout,
        )
        if not errors:
            time_elapsed = round(time.monotonic() - _start, 3)
            logger.debug(
                f'Inserted {len(rows)} to the BigQuery table "{table.table_id}"; '
                f"time_elapsed={time_elapsed}s"
            )
        else:
            raise BigQuerySinkException(
                f'Failed to insert rows to "{table.table_id}"; '
                f"first 5 errors: {errors[:5]}"
            )
