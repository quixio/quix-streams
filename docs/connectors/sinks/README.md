# Sinks (beta)

In many stream processing use cases the results need to be written to external destinations to be shared with other subsystems. 

Quix Streams provides a sink API to achieve that.

An example using InfluxDB Sink:

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
```

## Sinks Are Terminal Operations
`StreamingDataFrame.sink()` is special in that it's "terminal": 
**no additional operations can be added to it once called** (with branching, the branch
becomes terminal).

This is to ensure no further mutations can be applied to the outbound data.

_However_, you can continue other operations with other branches, including using
the same `Sink` to push another value (with another `SDF.sink()` call).

[Learn more about _branching_ here](../../advanced/branching.md).

### Branching after SDF.sink()

It is still possible to branch after using `SDF.sink()` assuming _you do NOT reassign 
with it_ (it returns `None`):

```python
sdf = app.dataframe(topic)
sdf = sdf.apply()

# Approach 1... Allows branching from `sdf`
sdf.sink()

# Approach 2...Disables branching from `sdf`
sdf = sdf.sink()
```

### Suggested Use of SDF.sink()

If further operations are required (or you want to preserve various operations for
other branches), it's recommended to use `SDF.sink()` as a standalone operation:

```python
sdf = app.dataframe(topic)
# [other operations here...]
sdf = sdf.apply().apply()  # last transforms before a sink
sdf.sink(influx_sink)  # do sink as a standalone call, no reassignment
sdf = sdf.apply()  # continue different operations with another branch...
```

## Supported Sinks

Currently, Quix Streams provides these sinks out of the box:

- [CSV Sink](csv-sink.md) - a simple CSV sinks that writes data to a single CSV file.
- [InfluxDB 3 Sink](influxdb3-sink.md) - a sink to write data to InfluxDB 3.

It's also possible to implement your own custom sinks.  
Please see the [Creating a Custom Sink](custom-sinks.md) page on how to do that.

## Performance Considerations
Since the implementation of `BatchingSink` accumulates data in-memory, it will increase memory usage.

If the batches become large enough, it can also put additional load on the destination and decrease the overall throughput. 

To adjust the number of messages that are batched and written in one go, you may provide a `commit_every` parameter to the `Application`.    
It will limit the amount of data processed and sinked during a single checkpoint.  
Note that it only limits the amount of incoming messages, and not the number of records being written to sinks.

**Example:**

```python
from quixstreams import Application
from quixstreams.sinks.core.influxdb3 import InfluxDB3Sink

# Commit the checkpoints after processing 1000 messages or after a 5 second interval has elapsed (whichever is sooner).
app = Application(
    broker_address="localhost:9092",
    commit_interval=5.0,
    commit_every=1000,
)
topic = app.topic('numbers-topic')
sdf = app.dataframe(topic)

# Create an InfluxDB sink that batches data between checkpoints.
influx_sink = InfluxDB3Sink(
    token="<influxdb-access-token>",
    host="<influxdb-host>",
    organization_id="<influxdb-org>",
    database="<influxdb-database>",
    measurement="numbers",
    fields_keys=["number"],
    tags_keys=["tag"]
)

# The sink will write to InfluxDB across all assigned partitions.
sdf.sink(influx_sink)
```
