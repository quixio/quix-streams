# Creating a Custom Sink

Quix Streams provides basic facilities to implement custom sinks for external destinations (currently in beta).

To create a new sink, users can subclass and implement the following Python base classes:
- `quixstreams.sinks.base.sink.BaseSink` - the basic interface for all sinks.  
The `StreamingDataFrame.sink()` accepts implementations of this class.

- `quixstreams.sinks.base.sink.BatchingSink` - a base class for batching sinks, that need to batch data first before writing it to the external destination.  
Check out [InfluxDBV3Sink](influxdb3-sink.md) and [CSVSink](csv-sink.md) for example implementations of the batching sinks.


Here is the code for `BaseSink` class for the reference:

```python
import abc

class BaseSink(abc.ABC):
    """
    This is a base class for all sinks.

    Subclass and implement its methods to create your own sink.

    Note that Sinks are currently in beta, and their design may change over time.
    """

    @abc.abstractmethod
    def flush(self, topic: str, partition: int):
        """
        This method is triggered by the Checkpoint class when it commits.

        You can use `flush()` to write the batched data to the destination (in case of
        a batching sink), or confirm the delivery of the previously sent messages
        (in case of a streaming sink).

        If flush() fails, the checkpoint will be aborted.
        """

    @abc.abstractmethod
    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: List[Tuple[str, HeaderValue]],
        topic: str,
        partition: int,
        offset: int,
    ):
        """
        This method is triggered on every new processed record being sent to this sink.

        You can use it to accumulate batches of data before sending them outside, or
        to send results right away in a streaming manner and confirm a delivery later
        on flush().
        """

    def on_paused(self, topic: str, partition: int):
        """
        This method is triggered when the sink is paused due to backpressure, when
        the `SinkBackpressureError` is raised.

        Here you can react to the backpressure events.
        """
```


## Sinks workflow

During the processing, Sinks do the following operations:

1. When the records arrive to sink, the application calls `BaseSink.add()` method.    
At this point, the sink implementation can decide what to do with the new record.  
For example, the `BatchingSink` will add a record to an in-memory batch.  
Other sinks may write the data straight away.

2. When the current checkpoint is committed, the app calls `BaseSink.flush()`.  
This is the moment when the sink can either write the accumulated data (like `BatchingSink`), or confirm the delivery of the previously written data.
   1. If the destination cannot accept new data, sinks can raise a special exception `SinkBackpressureError(topic, partition, retry_after)` and specify the timeout for the writes to be retried later.  
   2. The application will react to `SinkBackpressureError` by pausing the corresponding topic-partition for the given time and seeking the partition offset back to the beginning of the checkpoint.  
   3. When the timeout elapses, the app will resume consuming from this partition, re-process the data, and try to sink it again.

3. If any of the sinks fail during `flush()`, the application will abort the checkpoint, and the data needs to be re-processed again. 


## Performance considerations
Since the implementation of `BatchingSink` accumulates data in-memory, it will increase memory usage.

If the batches become large enough, it can also put additional load on the destination and decrease the overall throughput. 

To adjust how much data is batched and written in one go, users may provide a `commit_every` parameter to the `Application`.    
It will limit the amount of data processed and sinked during a single checkpoint.  
Note that it only limits the amount of incoming messages, and not the number of records being written to sinks.

**Example:**

```python
from quixstreams import Application
from quixstreams.sinks.influxdb_v3 import InfluxDBV3Sink

# Commit the checkpoints after processing 1000 messages OR after 5s interval is elapsed.
app = Application(
    broker_address='localhost:9092',
    commit_interval=5.0, 
    commit_every=1000,  
)
topic = app.topic('numbers-topic')
sdf = app.dataframe(topic)

# Create an Influx sink that batches data between checkpoints.
influx_sink = InfluxDBV3Sink(
    token="<influxdb-access-token>",
    host="<influxdb-host>",
    organization_id="<influxdb-org>",
    database="<influxdb-database>",
    measurement="numbers",
    fields_keys=['number'],
    tags_keys=['tag']
)

# The Influx sink will write to InfluxDB after processing 1000 messages 
# across all assigned partitions.
sdf.sink(influx_sink)
```
