# Amazon Kinesis Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes data to an Amazon Kinesis Data Stream. The sink preserves the original Kafka message key, but currently does not include timestamp, offset, or headers in the published messages.

## How To Install

To use the Kinesis sink, you need to install the required dependencies:

```bash
pip install quixstreams[kinesis]
```

## How It Works

`KinesisSink` is a streaming sink that publishes messages to Kinesis Data Streams. For each message:

- The value is serialized (defaults to JSON)
- The key is converted to a string
- Messages are published in batches of up to 500 records
- The sink ensures that the order of messages is preserved within each partition. This means that messages are sent to Kinesis in the same order they are received from Kafka for each specific partition.

**Important:** The Kinesis stream must already exist. The sink does not create the stream automatically. If the stream does not exist, an error will be raised when initializing the sink.

## How To Use

Create an instance of `KinesisSink` and pass it to the `StreamingDataFrame.sink()` method:

```python
import os
from quixstreams import Application
from quixstreams.sinks.community.kinesis import KinesisSink

app = Application(broker_address="localhost:9092")
topic = app.topic("topic-name")

# Configure the sink
kinesis_sink = KinesisSink(
    stream_name="<stream name>",
    # Optional: customize serialization
    value_serializer=str,
    key_serializer=str,
    # Optional: Additional keyword arguments are passed to the boto3 client
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="us-west-2",
)

sdf = app.dataframe(topic=topic)
sdf.sink(kinesis_sink)

if __name__ == "__main__":
    app.run()
```

### Configuration Options

- `stream_name`: The name of the Kinesis stream
- `value_serializer`: Function to serialize message values to string (default: `json.dumps`)
- `key_serializer`: Function to serialize message keys to string (default: `bytes.decode`)
- Additional keyword arguments are passed to the `boto3.client`

## Error Handling and Delivery Guarantees

The sink provides **at-least-once** delivery guarantees, which means:

- Messages are published in batches for better performance
- During checkpointing, the sink waits for all pending publishes to complete
- If any messages fail to publish, a `SinkBackpressureError` is raised
- When `SinkBackpressureError` occurs:
  - The application will retry the entire batch from the last successful offset
  - Some messages that were successfully published in the failed batch may be published again
  - This ensures no messages are lost, but some might be delivered more than once

This behavior makes the sink reliable but means downstream systems should be prepared to handle duplicate messages. If your application requires exactly-once semantics, you'll need to implement deduplication logic in your consumer.
