<a id="quixstreams.sinks.base.sink"></a>

## quixstreams.sinks.base.sink

<a id="quixstreams.sinks.base.sink.BaseSink"></a>

### BaseSink

```python
class BaseSink(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L11)

This is a base class for all sinks.

Subclass it and implement its methods to create your own sink.

Note that Sinks are currently in beta, and their design may change over time.

<a id="quixstreams.sinks.base.sink.BaseSink.flush"></a>

<br><br>

#### BaseSink.flush

```python
@abc.abstractmethod
def flush(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L21)

This method is triggered by the Checkpoint class when it commits.

You can use `flush()` to write the batched data to the destination (in case of
a batching sink), or confirm the delivery of the previously sent messages
(in case of a streaming sink).

If flush() fails, the checkpoint will be aborted.

<a id="quixstreams.sinks.base.sink.BaseSink.add"></a>

<br><br>

#### BaseSink.add

```python
@abc.abstractmethod
def add(value: Any, key: Any, timestamp: int,
        headers: List[Tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L33)

This method is triggered on every new processed record being sent to this sink.

You can use it to accumulate batches of data before sending them outside, or
to send results right away in a streaming manner and confirm a delivery later
on flush().

<a id="quixstreams.sinks.base.sink.BaseSink.on_paused"></a>

<br><br>

#### BaseSink.on\_paused

```python
def on_paused(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L51)

This method is triggered when the sink is paused due to backpressure, when
the `SinkBackpressureError` is raised.

Here you can react to the backpressure events.

<a id="quixstreams.sinks.base.sink.BatchingSink"></a>

### BatchingSink

```python
class BatchingSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L60)

A base class for batching sinks, that need to accumulate the data first before
sending it to the external destinatios.

Examples: databases, objects stores, and other destinations where
writing every message is not optimal.

It automatically handles batching, keeping batches in memory per topic-partition.

You may subclass it and override the `write()` method to implement a custom
batching sink.

<a id="quixstreams.sinks.base.sink.BatchingSink.write"></a>

<br><br>

#### BatchingSink.write

```python
@abc.abstractmethod
def write(batch: SinkBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L83)

This method implements actual writing to the external destination.

It may also raise `SinkBackpressureError` if the destination cannot accept new
writes at the moment.
When this happens, the accumulated batch is dropped and the app pauses the
corresponding topic partition.

<a id="quixstreams.sinks.base.sink.BatchingSink.add"></a>

<br><br>

#### BatchingSink.add

```python
def add(value: Any, key: Any, timestamp: int,
        headers: List[Tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L93)

Add a new record to in-memory batch.

<a id="quixstreams.sinks.base.sink.BatchingSink.flush"></a>

<br><br>

#### BatchingSink.flush

```python
def flush(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L115)

Flush an accumulated batch to the destination and drop it afterward.

<a id="quixstreams.sinks.base.sink.BatchingSink.on_paused"></a>

<br><br>

#### BatchingSink.on\_paused

```python
def on_paused(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L135)

When the destination is already backpressure, drop the accumulated batch.

<a id="quixstreams.sinks.base.batch"></a>

## quixstreams.sinks.base.batch

<a id="quixstreams.sinks.base.batch.SinkBatch"></a>

### SinkBatch

```python
class SinkBatch()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/batch.py#L12)

A batch to accumulate processed data by `BatchingSink` between the checkpoints.

Batches are created automatically by the implementations of `BatchingSink`.


<br>
***Arguments:***

- `topic`: a topic name
- `partition`: a partition number

<a id="quixstreams.sinks.base.batch.SinkBatch.iter_chunks"></a>

<br><br>

#### SinkBatch.iter\_chunks

```python
def iter_chunks(n: int) -> Iterable[Iterable[SinkItem]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/batch.py#L69)

Iterate over batch data in chunks of length n.
The last batch may be shorter.

<a id="quixstreams.sinks.base.exceptions"></a>

## quixstreams.sinks.base.exceptions

<a id="quixstreams.sinks.base.exceptions.SinkBackpressureError"></a>

### SinkBackpressureError

```python
class SinkBackpressureError(QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/exceptions.py#L6)

An exception to be raised by Sinks during flush() call

to signal a backpressure event to the application.

When raised, the app will drop the accumulated sink batch,
pause the corresponding topic partition for
a timeout specified in `retry_after`, and resume it when it's elapsed.


<br>
***Arguments:***

- `retry_after`: a timeout in seconds to pause for
- `topic`: a topic name to pause
- `partition`: a partition number to pause

<a id="quixstreams.sinks.core.influxdb3"></a>

## quixstreams.sinks.core.influxdb3

<a id="quixstreams.sinks.core.influxdb3.InfluxDB3Sink"></a>

### InfluxDB3Sink

```python
class InfluxDB3Sink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/influxdb3.py#L23)

<a id="quixstreams.sinks.core.influxdb3.InfluxDB3Sink.__init__"></a>

<br><br>

#### InfluxDB3Sink.\_\_init\_\_

```python
def __init__(token: str,
             host: str,
             organization_id: str,
             database: str,
             measurement: str,
             fields_keys: Iterable[str] = (),
             tags_keys: Iterable[str] = (),
             time_key: Optional[str] = None,
             time_precision: WritePrecision = WritePrecision.MS,
             include_metadata_tags: bool = False,
             batch_size: int = 1000,
             enable_gzip: bool = True,
             request_timeout_ms: int = 10_000,
             debug: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/influxdb3.py#L24)

A connector to sink processed data to InfluxDB v3.

It batches the processed records in memory per topic partition, converts
them to the InfluxDB format, and flushes them to InfluxDB at the checkpoint.

The InfluxDB sink transparently handles backpressure if the destination instance
cannot accept more data at the moment
(e.g., when InfluxDB returns an HTTP 429 error with the "retry_after" header set).
When this happens, the sink will notify the Application to pause consuming
from the backpressured topic partition until the "retry_after" timeout elapses.

>***NOTE***: InfluxDB3Sink can accept only dictionaries.
> If the record values are not dicts, you need to convert them to dicts before
> sinking.


<br>
***Arguments:***

- `token`: InfluxDB access token
- `host`: InfluxDB host in format "https://<host>"
- `organization_id`: InfluxDB organization_id
- `database`: database name
- `fields_keys`: a list of keys to be used as "fields" when writing to InfluxDB.
If present, it must not overlap with "tags_keys".
If empty, the whole record value will be used.
>***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
Default - `()`.
- `tags_keys`: a list of keys to be used as "tags" when writing to InfluxDB.
If present, it must not overlap with "fields_keys".
These keys will be popped from the value dictionary
automatically because InfluxDB doesn't allow the same keys be
both in tags and fields.
If empty, no tags will be sent.
>***NOTE***: InfluxDB client always converts tag values to strings.
Default - `()`.
- `time_key`: a key to be used as "time" when writing to InfluxDB.
By default, the record timestamp will be used with "ms" time precision.
When using a custom key, you may need to adjust the `time_precision` setting
to match.
- `time_precision`: a time precision to use when writing to InfluxDB.
- `include_metadata_tags`: if True, includes record's key, topic,
and partition as tags.
Default - `False`.
- `batch_size`: how many records to write to InfluxDB in one request.
Note that it only affects the size of one write request, and not the number
of records flushed on each checkpoint.
Default - `1000`.
- `enable_gzip`: if True, enables gzip compression for writes.
Default - `True`.
- `request_timeout_ms`: an HTTP request timeout in milliseconds.
Default - `10000`.
- `debug`: if True, print debug logs from InfluxDB client.
Default - `False`.

<a id="quixstreams.sinks.core.csv"></a>

## quixstreams.sinks.core.csv

<a id="quixstreams.sinks.core.csv.CSVSink"></a>

### CSVSink

```python
class CSVSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/csv.py#L9)

<a id="quixstreams.sinks.core.csv.CSVSink.__init__"></a>

<br><br>

#### CSVSink.\_\_init\_\_

```python
def __init__(path: str,
             dialect: str = "excel",
             key_serializer: Callable[[Any], str] = str,
             value_serializer: Callable[[Any], str] = json.dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/csv.py#L10)

A base CSV sink that writes data from all assigned partitions to a single file.

It's best to be used for local debugging.

Column format:
    (key, value, timestamp, topic, partition, offset)


<br>
***Arguments:***

- `path`: a path to CSV file
- `dialect`: a CSV dialect to use. It affects quoting and delimiters.
See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
Default - `"excel"`.
- `key_serializer`: a callable to convert keys to strings.
Default - `str`.
- `value_serializer`: a callable to convert values to strings.
Default - `json.dumps`.

<a id="quixstreams.sinks.community.iceberg"></a>

## quixstreams.sinks.community.iceberg

<a id="quixstreams.sinks.community.iceberg.AWSIcebergConfig"></a>

### AWSIcebergConfig

```python
class AWSIcebergConfig(BaseIcebergConfig)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L42)

<a id="quixstreams.sinks.community.iceberg.AWSIcebergConfig.__init__"></a>

<br><br>

#### AWSIcebergConfig.\_\_init\_\_

```python
def __init__(aws_s3_uri: str,
             aws_region: Optional[str] = None,
             aws_access_key_id: Optional[str] = None,
             aws_secret_access_key: Optional[str] = None,
             aws_session_token: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L43)

Configure IcebergSink to work with AWS Glue.


<br>
***Arguments:***

- `aws_s3_uri`: The S3 URI where the table data will be stored
(e.g., 's3://your-bucket/warehouse/').
- `aws_region`: The AWS region for the S3 bucket and Glue catalog.
- `aws_access_key_id`: the AWS access key ID.
NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
when using AWS Glue.
- `aws_secret_access_key`: the AWS secret access key.
NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
when using AWS Glue.
- `aws_session_token`: a session token (or will be generated for you).
NOTE: can alternatively set the AWS_SESSION_TOKEN environment variable when
using AWS Glue.

<a id="quixstreams.sinks.community.iceberg.IcebergSink"></a>

### IcebergSink

```python
class IcebergSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L76)

IcebergSink writes batches of data to an Apache Iceberg table.

The data will by default include the kafka message key, value, and timestamp.

It serializes incoming data batches into Parquet format and appends them to the
Iceberg table, updating the table schema as necessary.

Currently, supports Apache Iceberg hosted in:

- AWS

Supported data catalogs:

- AWS Glue


<br>
***Arguments:***

- `table_name`: The name of the Iceberg table.
- `config`: An IcebergConfig with all the various connection parameters.
- `data_catalog_spec`: data cataloger to use (ex. for AWS Glue, "aws_glue").
- `schema`: The Iceberg table schema. If None, a default schema is used.
- `partition_spec`: The partition specification for the table.
If None, a default is used.

Example setup using an AWS-hosted Iceberg with AWS Glue:

```
from quixstreams import Application
from quixstreams.sinks.community.iceberg import IcebergSink, AWSIcebergConfig

# Configure S3 bucket credentials
iceberg_config = AWSIcebergConfig(
    aws_s3_uri="", aws_region="", aws_access_key_id="", aws_secret_access_key=""
)

# Configure the sink to write data to S3 with the AWS Glue catalog spec
iceberg_sink = IcebergSink(
    table_name="glue.sink-test",
    config=iceberg_config,
    data_catalog_spec="aws_glue",
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink_topic')

# Do some processing here
sdf = app.dataframe(topic=topic).print(metadata=True)

# Sink results to the IcebergSink
sdf.sink(iceberg_sink)


if __name__ == "__main__":
    # Start the application
    app.run()
```

<a id="quixstreams.sinks.community.iceberg.IcebergSink.write"></a>

<br><br>

#### IcebergSink.write

```python
def write(batch: SinkBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L174)

Writes a batch of data to the Iceberg table.

Implements retry logic to handle concurrent write conflicts.


<br>
***Arguments:***

- `batch`: The batch of data to write.

<a id="quixstreams.sinks.community.bigquery"></a>

## quixstreams.sinks.community.bigquery

<a id="quixstreams.sinks.community.bigquery.BigQuerySink"></a>

### BigQuerySink

```python
class BigQuerySink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/bigquery.py#L53)

<a id="quixstreams.sinks.community.bigquery.BigQuerySink.__init__"></a>

<br><br>

#### BigQuerySink.\_\_init\_\_

```python
def __init__(project_id: str,
             location: str,
             dataset_id: str,
             table_name: str,
             service_account_json: Optional[str] = None,
             schema_auto_update: bool = True,
             ddl_timeout: float = 10.0,
             insert_timeout: float = 10.0,
             retry_timeout: float = 30.0,
             **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/bigquery.py#L54)

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


<br>
***Arguments:***

- `project_id`: a Google project id.
- `location`: a BigQuery location.
- `dataset_id`: a BigQuery dataset id.
If the dataset does not exist, the sink will try to create it.
- `table_name`: BigQuery table name.
If the table does not exist, the sink will try to create it with a default schema.
- `service_account_json`: an optional JSON string with service account credentials
to connect to BigQuery.
The internal `google.cloud.bigquery.Client` will use the Application Default Credentials if not provided.
See https://cloud.google.com/docs/authentication/provide-credentials-adc for more info.
Default - `None`.
- `schema_auto_update`: if True, the sink will try to create a dataset and a table if they don't exist.
It will also add missing columns on the fly with types inferred from individual values.
- `ddl_timeout`: a timeout for a single DDL operation (adding tables, columns, etc.).
Default - 10s.
- `insert_timeout`: a timeout for a single INSERT operation.
Default - 10s.
- `retry_timeout`: a total timeout for each request to BigQuery API.
During this timeout, a request can be retried according
to the client's default retrying policy.
- `kwargs`: Additional keyword arguments passed to `bigquery.Client`.

<a id="quixstreams.sinks.community.file.sink"></a>

## quixstreams.sinks.community.file.sink

<a id="quixstreams.sinks.community.file.sink.InvalidFormatError"></a>

### InvalidFormatError

```python
class InvalidFormatError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L24)

Raised when the format is specified incorrectly.

<a id="quixstreams.sinks.community.file.sink.FileSink"></a>

### FileSink

```python
class FileSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L30)

Writes batches of data to files on disk using specified formats.

Messages are grouped by their topic and partition. Data from messages with
the same topic and partition are saved in the same directory. Each batch of
messages is serialized and saved to a file within that directory. Files are
named using the batch's starting offset to ensure uniqueness and order.

If `append` is set to `True`, the sink will attempt to append data to an
existing file rather than creating a new one. This is only supported for
formats that allow appending.

<a id="quixstreams.sinks.community.file.sink.FileSink.__init__"></a>

<br><br>

#### FileSink.\_\_init\_\_

```python
def __init__(output_dir: str,
             format: Union[FormatName, Format],
             append: bool = False) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L44)

Initializes the FileSink.


<br>
***Arguments:***

- `output_dir`: The directory where files will be written.
- `format`: The data serialization format to use. This can be either a
format name ("json", "parquet") or an instance of a `Format`
subclass.
- `append`: If `True`, data will be appended to existing files when possible.
Note that not all formats support appending. Defaults to `False`.

**Raises**:

- `ValueError`: If `append` is `True` but the specified format does not
support appending.

<a id="quixstreams.sinks.community.file.sink.FileSink.write"></a>

<br><br>

#### FileSink.write

```python
def write(batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L68)

Writes a batch of data to files on disk, grouping data by topic and partition.

If `append` is `True` and an existing file is found, data will be appended to
the last file. Otherwise, a new file is created based on the batch's starting
offset.


<br>
***Arguments:***

- `batch`: The batch of data to write.

<a id="quixstreams.sinks.community.file.formats.base"></a>

## quixstreams.sinks.community.file.formats.base

<a id="quixstreams.sinks.community.file.formats.base.Format"></a>

### Format

```python
class Format(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L8)

Base class for formatting batches in file sinks.

This abstract base class defines the interface for batch formatting
in file sinks. Subclasses should implement the `file_extension`
property and the `serialize` method to define how batches are
formatted and saved.

<a id="quixstreams.sinks.community.file.formats.base.Format.file_extension"></a>

<br><br>

#### Format.file\_extension

```python
@property
@abstractmethod
def file_extension() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L20)

Returns the file extension used for output files.


<br>
***Returns:***

The file extension as a string.

<a id="quixstreams.sinks.community.file.formats.base.Format.supports_append"></a>

<br><br>

#### Format.supports\_append

```python
@property
@abstractmethod
def supports_append() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L30)

Indicates if the format supports appending data to an existing file.


<br>
***Returns:***

True if appending is supported, otherwise False.

<a id="quixstreams.sinks.community.file.formats.base.Format.serialize"></a>

<br><br>

#### Format.serialize

```python
@abstractmethod
def serialize(batch: SinkBatch) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L39)

Serializes a batch of messages into bytes.


<br>
***Arguments:***

- `batch`: The batch of messages to serialize.


<br>
***Returns:***

The serialized batch as bytes.

<a id="quixstreams.sinks.community.file.formats.json"></a>

## quixstreams.sinks.community.file.formats.json

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat"></a>

### JSONFormat

```python
class JSONFormat(Format)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L14)

Serializes batches of messages into JSON Lines format with optional gzip
compression.

This class provides functionality to serialize a `SinkBatch` into bytes
in JSON Lines format. It supports optional gzip compression and allows
for custom JSON serialization through the `dumps` parameter.

This format supports appending to existing files.

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat.__init__"></a>

<br><br>

#### JSONFormat.\_\_init\_\_

```python
def __init__(file_extension: str = ".jsonl",
             compress: bool = False,
             dumps: Optional[Callable[[Any], str]] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L28)

Initializes the JSONFormat.


<br>
***Arguments:***

- `file_extension`: The file extension to use for output files.
Defaults to ".jsonl".
- `compress`: If `True`, compresses the output using gzip and
appends ".gz" to the file extension. Defaults to `False`.
- `dumps`: A custom function to serialize objects to JSON-formatted
strings. If provided, the `compact` option is ignored.

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat.file_extension"></a>

<br><br>

#### JSONFormat.file\_extension

```python
@property
def file_extension() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L57)

Returns the file extension used for output files.


<br>
***Returns:***

The file extension as a string.

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat.serialize"></a>

<br><br>

#### JSONFormat.serialize

```python
def serialize(batch: SinkBatch) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L65)

Serializes a `SinkBatch` into bytes in JSON Lines format.

Each item in the batch is converted into a JSON object with
"_timestamp", "_key", and "_value" fields. If the message key is
in bytes, it is decoded to a string.


<br>
***Arguments:***

- `batch`: The `SinkBatch` to serialize.


<br>
***Returns:***

The serialized batch in JSON Lines format, optionally
compressed with gzip.

<a id="quixstreams.sinks.community.file.formats.parquet"></a>

## quixstreams.sinks.community.file.formats.parquet

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat"></a>

### ParquetFormat

```python
class ParquetFormat(Format)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L16)

Serializes batches of messages into Parquet format.

This class provides functionality to serialize a `SinkBatch` into bytes
in Parquet format using PyArrow. It allows setting the file extension
and compression algorithm used for the Parquet files.

This format does not support appending to existing files.

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat.__init__"></a>

<br><br>

#### ParquetFormat.\_\_init\_\_

```python
def __init__(file_extension: str = ".parquet",
             compression: Compression = "snappy") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L29)

Initializes the ParquetFormat.


<br>
***Arguments:***

- `file_extension`: The file extension to use for output files.
Defaults to ".parquet".
- `compression`: The compression algorithm to use for Parquet files.
Allowed values are "none", "snappy", "gzip", "brotli", "lz4",
or "zstd". Defaults to "snappy".

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat.file_extension"></a>

<br><br>

#### ParquetFormat.file\_extension

```python
@property
def file_extension() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L47)

Returns the file extension used for output files.


<br>
***Returns:***

The file extension as a string.

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat.serialize"></a>

<br><br>

#### ParquetFormat.serialize

```python
def serialize(batch: SinkBatch) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L55)

Serializes a `SinkBatch` into bytes in Parquet format.

Each item in the batch is converted into a dictionary with "_timestamp",
"_key", and the keys from the message value. If the message key is in
bytes, it is decoded to a string.

Missing fields in messages are filled with `None` to ensure all rows
have the same columns.


<br>
***Arguments:***

- `batch`: The `SinkBatch` to serialize.


<br>
***Returns:***

The serialized batch as bytes in Parquet format.

<a id="quixstreams.sinks.community.pubsub"></a>

## quixstreams.sinks.community.pubsub

<a id="quixstreams.sinks.community.pubsub.PubSubTopicNotFoundError"></a>

### PubSubTopicNotFoundError

```python
class PubSubTopicNotFoundError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L25)

Raised when the specified topic does not exist.

<a id="quixstreams.sinks.community.pubsub.PubSubSink"></a>

### PubSubSink

```python
class PubSubSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L29)

A sink that publishes messages to Google Cloud Pub/Sub.

<a id="quixstreams.sinks.community.pubsub.PubSubSink.__init__"></a>

<br><br>

#### PubSubSink.\_\_init\_\_

```python
def __init__(project_id: str,
             topic_id: str,
             service_account_json: Optional[str] = None,
             value_serializer: Callable[[Any], Union[bytes, str]] = json.dumps,
             key_serializer: Callable[[Any], str] = bytes.decode,
             flush_timeout: int = 5,
             **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L32)

Initialize the PubSubSink.


<br>
***Arguments:***

- `project_id`: GCP project ID.
- `topic_id`: Pub/Sub topic ID.
- `service_account_json`: an optional JSON string with service account credentials
to connect to Pub/Sub.
The internal `PublisherClient` will use the Application Default Credentials if not provided.
See https://cloud.google.com/docs/authentication/provide-credentials-adc for more info.
Default - `None`.
- `value_serializer`: Function to serialize the value to string or bytes
(defaults to json.dumps).
- `key_serializer`: Function to serialize the key to string
(defaults to bytes.decode).
- `kwargs`: Additional keyword arguments passed to PublisherClient.

<a id="quixstreams.sinks.community.pubsub.PubSubSink.add"></a>

<br><br>

#### PubSubSink.add

```python
def add(value: Any, key: Any, timestamp: int,
        headers: list[tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L81)

Publish a message to Pub/Sub.

<a id="quixstreams.sinks.community.pubsub.PubSubSink.flush"></a>

<br><br>

#### PubSubSink.flush

```python
def flush(topic: str, partition: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L114)

Wait for all publish operations to complete successfully.

<a id="quixstreams.sinks.community.postgresql"></a>

## quixstreams.sinks.community.postgresql

<a id="quixstreams.sinks.community.postgresql.PostgreSQLSink"></a>

### PostgreSQLSink

```python
class PostgreSQLSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/postgresql.py#L48)

<a id="quixstreams.sinks.community.postgresql.PostgreSQLSink.__init__"></a>

<br><br>

#### PostgreSQLSink.\_\_init\_\_

```python
def __init__(host: str,
             port: int,
             dbname: str,
             user: str,
             password: str,
             table_name: str,
             schema_auto_update: bool = True,
             **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/postgresql.py#L49)

A connector to sink topic data to PostgreSQL.


<br>
***Arguments:***

- `host`: PostgreSQL server address.
- `port`: PostgreSQL server port.
- `dbname`: PostgreSQL database name.
- `user`: Database user name.
- `password`: Database user password.
- `table_name`: PostgreSQL table name.
- `schema_auto_update`: Automatically update the schema when new columns are detected.
- `ddl_timeout`: Timeout for DDL operations such as table creation or schema updates.
- `kwargs`: Additional parameters for `psycopg2.connect`.

