# InfluxDB v1 Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

InfluxDB is an open source time series database for metrics, events, and real-time analytics.

Quix Streams provides a sink to write processed data to InfluxDB v1.

>***NOTE***: This sink only supports InfluxDB v1. 

## How To Install
The dependencies for this sink are not included to the default `quixstreams` package.

To install them, run the following command:

```commandline
pip install quixstreams[influxdb1]
```

## How To Use

To sink data to InfluxDB v1, you need to create an instance of `InfluxDB1Sink` and pass 
it to the `StreamingDataFrame.sink()` method:

```python
from quixstreams import Application
from quixstreams.sinks.community.influxdb1 import InfluxDB1Sink

app = Application(broker_address="localhost:9092")
topic = app.topic("numbers-topic")

# Initialize InfluxDB1Sink
influx_sink = InfluxDB1Sink(
    host="<influxdb-host>",
    database="<influxdb-database>",
    username="<infludb-username>",
    password="<influxdb-password>",
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
`InfluxDB1Sink` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to the InfluxDB instance when a checkpoint has been committed.

Under the hood, it transforms data to the Influx format using  and writes processed records in batches.

### What data can be sent to InfluxDB?

`InfluxDB1Sink` can accept only dictionaries values.

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
`InfluxDB1Sink` provides at-least-once guarantees, and the same records may be written multiple times in case of errors during processing.  

## Backpressure Handling
InfluxDB1Sink automatically handles events when the database cannot accept new data due to write limits.  

When this happens, the application loses the accumulated in-memory batch and pauses the corresponding topic partition for a timeout duration returned by InfluxDB API (it returns an HTTP error with 429 status code and a `Retry-After` header with a timeout).  
When the timeout expires, the app automatically resumes the partition to re-process the data and sink it again.

## Configuration
InfluxDB1Sink accepts the following configuration parameters:

- `host` - InfluxDB host in format "https://<host>"

- `pot` - InfluxDB port. Default - `8086`.

- `database` - a database name.

- `username` - a username.

- `password` - a password.

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

- `request_timeout_ms` - an HTTP request timeout in milliseconds.   
Default - `10000`.
