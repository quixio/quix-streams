# InfluxDB v3 Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.


InfluxDB is an open source time series database for metrics, events, and real-time analytics.

Quix Streams provides a source to extract "measurements" from InfluxDB v3 databases 
  and dump them to a Kafka topic.

>***NOTE***: This source only supports InfluxDB v3. Versions 1 and 2 are not supported.

## How to Install

Install Quix Streams with the following optional dependencies:

```bash
pip install quixstreams[influxdb3]
```

## How it Works

`InfluxDB3Source` extracts data from a specified set of measurements in a
  database (or all available ones if none are specified).

It processes measurements sequentially by gathering/producing a tumbling
  "time_delta"-sized window of data, starting from a specified 'start_date' and 
  eventually stopping at a specified 'end_date', completing that measurement.

It then starts the next measurement, continuing until all are complete.

Note that 'end_date' is optional; when not provided, it will run indefinitely for a 
  single measurement (which means no other measurements will be processed!).

## How to Use

Import and instantiate an `InfluxDB3Source` instance and hand it to an Application using
`app.add_source(<InfluxDB3Source>)` or instead to a StreamingDataFrame with 
`app.dataframe(source=<InfluxDB3Source>)` if further data manipulation is required.

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.influxdb3 import InfluxDB3Source
from datetime import datetime, timedelta, timezone

app = Application(broker_address="localhost:9092")
topic = app.topic("influx-topic")

influx = InfluxDB3Source(
    token="<influxdb-access-token>",
    host="<influxdb-host>",
    organization_id="<influxdb-org>",
    database="<influxdb-database>",
    measurements="my-measurement",
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=2),
    end_date=datetime.now(tz=timezone.utc),
)

app = Application(
    broker_address="<YOUR BROKER INFO>",
    consumer_group="<YOUR GROUP>",
)

sdf = app.dataframe(source=influx).print(metadata=True)
# YOUR LOGIC HERE!
sdf.to_topic(topic)

if __name__ == "__main__":
    app.run()
```


## Configuration
Here are the InfluxDB-related configurations to be aware of (see [InfluxDB3Source API](../../api-reference/sources.md#influxdb3source) for all parameters).

### Required:

- `host`: Host URL of the InfluxDB instance.
- `token`: Authentication token for InfluxDB.
- `organization_id`: Organization name in InfluxDB.
- `database`: Database name in InfluxDB.


### Optional:
- `key_setter`: sets the kafka message key for a measurement record.  
  By default, will set the key to the measurement's name.
- `timestamp_setter`: sets the kafka message timestamp for a measurement record.  
  By default, the timestamp will be the Kafka default (Kafka produce time).
- `start_date`: The start datetime for querying InfluxDB.  
  Uses current time by default.
- `end_date`: The end datetime for querying InfluxDB.  
  If none provided, runs indefinitely for a single measurement.
- `measurements`: The measurements to query.  
  If None, all measurements will be processed.
- `measurement_column_name`: The column name used for appending the measurement name to the record.
  Default: `_measurement_name`.
- `sql_query`: Custom SQL query for retrieving data.
  Query expects a `{start_time}`, `{end_time}`, and `{measurement_name}` for later formatting.  
  If provided, it overrides the default window-query logic.
- `time_delta`: Time interval for batching queries, e.g. "5m" for 5 minutes.  
  Default: `5m`.
- `delay`: Add a delay (in seconds) between producing batches.  
  Default: `0`.
- `max_retries`: Maximum number of retries for querying or producing;  
  Note that consecutive retries have a multiplicative backoff.
  Default: `5`.


## Testing Locally

Rather than connect to a hosted InfluxDB3 instance, you can alternatively test your 
application using a local instance of Influxdb3 using Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name influxdb3 \
    -p 8181:8181 \
    quay.io/influxdb/influxdb3-core:latest \
    serve --node-id=host0 --object-store=memory
    ```

2. Use the following settings for `InfluxDB3Source` to connect:

    ```python
    InfluxDB3Source(
        host="http://localhost:8181",   # be sure to add http
        organization_id="local",        # unused, but required
        token="local",                  # unused, but required
        database="<YOUR DB>",
    )
    ```

Note: the database must exist for this to successfully run.