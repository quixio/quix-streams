# InfluxDB v3 Sink

InfluxDB is an open source time series database for metrics, events, and real-time analytics.

Quix Streams provides a sink to write processed data to InfluxDB v3.

>***NOTE***: This sink only supports InfluxDB v3. Versions 1 and 2 are not supported.

## How To Install
The dependencies for this sink are not included to the default `quixstreams` package.

To install them, run the following command:

```commandline
pip install quixstreams[influxdb3]
```

## How To Use

To sink data to InfluxDB, you need to create an instance of `InfluxDB3Sink` and pass 
it to the `StreamingDataFrame.sink()` method:

```python
from quixstreams import Application
from quixstreams.sinks.core.influxdb3 import InfluxDB3Sink

app = Application(broker_address="localhost:9092")
topic = app.topic("numbers-topic")

# Initialize InfluxDB3Sink
influx_sink = InfluxDB3Sink(
    token="<influxdb-access-token>",
    host="<influxdb-host>",
    organization_id="<influxdb-org>",
    database="<influxdb-database>",
    measurement="numbers",
    fields_keys=["number"],
    tags_keys=["tag"]
)

sdf = app.dataframe(topic)
# Do some processing here ...
# Sink data to InfluxDB
sdf.sink(influx_sink)

if __name__ == '__main__':
    app.run()
```

## How It Works
`InfluxDB3Sink` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to the InfluxDB instance when a checkpoint has been committed.

Under the hood, it transforms data to the Influx format using  and writes processed records in batches.

### What data can be sent to InfluxDB?

`InfluxDB3Sink` can accept only dictionaries values.

If the record values are not dicts, you need to convert them to dicts using `StreamingDataFrame.apply()` before sinking.

The structure of the sinked data is defined by the `fields_keys` and `tags_keys` parameters provided to the sink class.

- `fields_keys` - a list of keys to be used as "fields" when writing to InfluxDB.  
If present, its keys cannot overlap with any in `tags_keys`.  
If empty, the whole record value will be used.
The fields' values can only be strings, floats, integers, or booleans.

- `tags_keys` - a list of keys to be used as "tags" when writing to InfluxDB.
If present, its keys cannot overlap with any in `fields_keys`.  
These keys will be popped from the value dictionary automatically because InfluxDB doesn't allow the same keys be both in tags and fields.  
If empty, no tags will be sent.
>***NOTE***: InfluxDB client always converts tag values to strings.

To learn more about schema design and data types in InfluxDB, please read [InfluxDB schema design recommendations](https://docs.influxdata.com/influxdb/cloud-serverless/write-data/best-practices/schema-design/).

## Delivery Guarantees
`InfluxDB3Sink` provides at-least-once guarantees, and the same records may be written multiple times in case of errors during processing.  

## Backpressure Handling
InfluxDB sink automatically handles events when the database cannot accept new data due to write limits.  

When this happens, the application loses the accumulated in-memory batch and pauses the corresponding topic partition for a timeout duration returned by InfluxDB API (it returns an HTTP error with 429 status code and a `Retry-After` header with a timeout).  
When the timeout expires, the app automatically resumes the partition to re-process the data and sink it again.

## Configuration
InfluxDB3Sink accepts the following configuration parameters:

- `token` - InfluxDB access token.

- `host` - InfluxDB host in format "https://<host>"

- `organization_id` - InfluxDB organization ID.

- `database` - a database name.

- `measurement` - a measurement name, required.
  
- `fields_keys` - an iterable (list) of strings used as InfluxDB "fields".  
  Also accepts a single-argument callable that receives the current message data as a dict and returns an iterable of strings.
  - If present, it must not overlap with "tags_keys".
  - If empty, the whole record value will be used.  
See the [What data can be sent to InfluxDB](#what-data-can-be-sent-to-influxdb) for more info.

- `tags_keys` - an iterable (list) of strings used as InfluxDB "tags".  
  Also accepts a single-argument callable that receives the current message data as a 
  dict and returns an iterable of strings.
  - If present, it must not overlap with "fields_keys".
  - Given keys are popped from the value dictionary since the same key
    cannot be both a tag and field.
  - If empty, no tags will be sent.  
See the [What data can be sent to InfluxDB](#what-data-can-be-sent-to-influxdb) for more info.

- `time_setter` - an optional column name to use as "time" for InfluxDB.  
  Also accepts a callable which receives the current message data and
  returns either the desired time or `None` (use default).  
  - The time can be an `int`, `string` (RFC3339 format), or `datetime`.
  - The time must match the `time_precision` argument if not a `datetime` object, else raises.
  - By default, a record's kafka timestamp with `"ms"` time precision is used.

- `time_precision` - a time precision to use when writing to InfluxDB.  
Default - `ms`.

- `include_metadata_tags` - if True, includes the record's key, topic, and partition as tags.  
Default - `False`.

- `convert_ints_to_floats` - if True, converts all integer values to floats.  
Default - `False`.

- `batch_size` - the number of records to write to InfluxDB in one request.    
Note that it only affects the size of one write request, and not the number of records flushed on each checkpoint.    
Default - `1000`.

- `enable_gzip` - if True, enables gzip compression for writes.    
Default - `True`.

- `request_timeout_ms` - an HTTP request timeout in milliseconds.   
Default - `10000`.

- `debug` - if True, print debug logs from InfluxDB client.  
Default - `False`.

## Testing Locally

Rather than connect to a hosted InfluxDB3 instance, you can alternatively test your 
application using a local instance of InfluxDb3 using Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name influxdb3 \
    -p 8181:8181 \
    quay.io/influxdb/influxdb3-core:latest \
    serve --node-id=host0 --object-store=memory
    ```

2. Use the following settings for `InfluxDB3Sink` to connect:

    ```python
    InfluxDB3Sink(
        host="http://localhost:8181",   # be sure to add http
        organization_id="local",        # unused, but required
        token="local",                  # unused, but required
   )
    ```

The `database` you provide will be auto-created for you.