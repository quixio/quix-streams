# InfluxDB v3 Sink

InfluxDB is an open source time series database for metrics, events, and real-time analytics.

Quix Streams provides a Sink for InfluxDB v3 to write the processed data to it.  

>***NOTE***: This sink only supports InfluxDB v3. Versions 1 and 2 are not supported.

## How To Use InfluxDB Sink

To sink data to InfluxDB, you need to create an instance of `InfluxDB3Sink` and pass 
it to the `StreamingDataFrame.sink()` method:

```python
from quixstreams import Application
from quixstreams.sinks.influxdb3 import InfluxDB3Sink

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
```

## How the InfluxDB Sink works
`InfluxDB3Sink` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to the InfluxDB instance when a checkpoint has been committed.

Under the hood, it transforms data to the Influx format using  and writes processed records in batches.

### What data can be sent to InfluxDB

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

## Delivery guarantees
`InfluxDB3Sink` provides at-least-once guarantees, and the same records may be written multiple times in case of errors during processing.  

## Backpressure handling
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
  
- `fields_keys` - a list of keys to be used as "fields" when writing to InfluxDB.  
See the [What data can be sent to InfluxDB](#what-data-can-be-sent-to-influxdb) for more info.

- `tags_keys` - a list of keys to be used as "tags" when writing to InfluxDB.  
See the [What data can be sent to InfluxDB](#what-data-can-be-sent-to-influxdb) for more info.

            
- `time_key` - a key to be used as "time" when writing to InfluxDB.  
By default, the record timestamp will be used with millisecond time precision.
When using a custom key, you may need to adjust the `time_precision` setting to match.

- `time_precision` - a time precision to use when writing to InfluxDB.  
Default - `ms`.

- `include_metadata_tags` - if True, includes the record's key, topic, and partition as tags.  
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
