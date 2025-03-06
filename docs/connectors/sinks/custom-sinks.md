# Creating a Custom Sink

Quix Streams provides basic facilities to implement custom sinks for external destinations (currently in beta).

To create a new sink, extend and implement the following Python base classes:
- `quixstreams.sinks.base.sink.BaseSink` - the parent interface for all sinks.  
The `StreamingDataFrame.sink()` accepts implementations of this class.

- `quixstreams.sinks.base.sink.BatchingSink` - a base class for batching sinks, that need to batch data first before writing it to the external destination.

Check out [InfluxDB3Sink](../../api-reference/sinks.md#influxdb3sink) and [CSVSink](../../api-reference/sinks.md#csvsink) for example implementations of the batching sinks.


## Sinks Workflow

During processing, sinks do the following operations:

1. When a new record arrives, the application calls `BaseSink.add()` method.    
At this point, the sink implementation can decide what to do with the new record.  
For example, the `BatchingSink` will add a record to an in-memory batch.  
Other sinks may write the data straight away.

2. When the current checkpoint is committed, the app calls `BaseSink.flush()`.  
For example, `BatchingSink` will write the accumulated data during `flush()`.
   1. If the destination cannot accept new data, sinks can raise a special exception `SinkBackpressureError(topic, partition, retry_after)` and specify the timeout for the writes to be retried later.  
   2. The application will react to `SinkBackpressureError` by pausing the corresponding topic-partition for the given time and seeking the partition offset back to the beginning of the checkpoint.  
   3. When the timeout elapses, the app will resume consuming from this partition, re-process the data, and try to sink it again.

3. If any of the sinks fail during `flush()`, the application will abort the checkpoint, and the data will be re-processed again. 


## Backpressure Handling

In some cases, destinations may not be able to accept large amounts of data sinked from the streaming pipelines.

For example, some databases may rate limit the number of bytes written in a given timeframe, or simply timeout if there's too much data to write.

To handle these scenarios, Sinks provide the **backpressure mechanism**.

The Sink can tell the application to pause consuming from the given topic-partition and reprocess the data later by raising a [SinkBackpressureError(topic, partition, retry_after)](../../api-reference/sinks.md#sinkbackpressureerror).

The app will catch the `SinkBackpressureError`, pause the topic-partition for the `retry_after` timeout, seek the partition offset back to the beginning of the checkpoint, and remove this partition from the current checkpoint commit.

This way, the processing will continue, and the backpressured data will be re-processed and sinked again after the timeout.


**Example:**

```python
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError


class MyDatabaseSink(BatchingSink):
    """
    Some sink writing data to a database
    """
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
           on_client_connect_success=on_client_connect_success,
           on_client_connect_failure=on_client_connect_failure
        )
        self._db_connection = None

    def setup(self):
        self._db_connection = my_database.connect('<connection credentials>')
        self._db_connection.test()

    def write(self, batch: SinkBatch):
        # Simulate some DB connection here
        data = [{'value': item.value} for item in batch]
        try:
            # Try to write data to the db
            self._db_connection.write(data)
        except TimeoutError:
            # In case of timeout, tell the app to wait for 30s 
            # and retry the writing later
            raise SinkBackpressureError(
               retry_after=30.0, 
               topic=batch.topic, 
               partition=batch.partition,
            )
```
