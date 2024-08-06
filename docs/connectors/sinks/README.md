# Sinks (beta)

In many stream processing use cases the results need to be written to external destinations to be shared with other subsystems. 

Quix Streams provides a Sink API to achieve that.

An example using InfluxDB Sink:

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

## Sinks Are Terminal
When `.sink()` is called on a StreamingDataFrame instance, it marks the end of the processing pipeline, and 
 the StreamingDataFrame can't be changed anymore.

Make sure you call `StreamingDataFrame.sink()` as the last operation.


## Supported Sinks

Currently, Quix Streams provides these sinks out of the box:
- [CSV Sink](csv-sink.md) - a simple CSV sinks that writes data to a single CSV file.
- [InfluxDB 3 Sink](influxdb3-sink.md) - a sink to write data to InfluxDB 3.

It's also possible to implement your own custom sinks.  
Please see the [Creating a Custom Sink](custom-sinks.md) page on how to do that.
