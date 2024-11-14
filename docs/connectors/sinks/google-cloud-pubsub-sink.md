# Google Cloud Pub/Sub Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes data to a Google Cloud Pub/Sub topic. The sink preserves the original Kafka message metadata including key, timestamp, offset, and headers.

## Prerequisites

To use the Pub/Sub sink, you need to install the required dependencies:

```bash
pip install quixstreams[pubsub]
```

## How the Pub/Sub Sink Works

`PubSubSink` is a streaming sink that publishes messages to Pub/Sub topics. For each message:

- The value is serialized (defaults to JSON)
- The key is converted to string
- Additional metadata (timestamp, offset, headers) is included as attributes
- Messages are published asynchronously

## How To Use Pub/Sub Sink

Create an instance of `PubSubSink` and pass it to the `StreamingDataFrame.sink()` method:

```python
from google.api_core import retry
from google.cloud.pubsub_v1.types import PublisherOptions
from quixstreams import Application
from quixstreams.sinks.community.pubsub import PubSubSink

# Configure the sink
pubsub_sink = PubSubSink(
    project_id="your-project-id",
    topic_id="your-topic-id",
    # Optional: service account credentials as a JSON string
    service_account_json='{"type": "service_account", "project_id": "your-project", ...}',
    # Optional: customize serialization and flush timeout
    value_serializer=json.dumps,
    key_serializer=str,
    flush_timeout=10,
    # Optional: Additional keyword arguments are passed to the PublisherClient
    publisher_options=PublisherOptions(
        # Configure publisher options to retry on any exception
        retry=retry.Retry(predicate=retry.if_exception_type(Exception)),
    )
)

app = Application(broker_address="localhost:9092")
topic = app.topic("input_topic")

# Do some processing here
sdf = app.dataframe(topic=topic).print(metadata=True)

# Sink results to the PubSubSink
sdf.sink(pubsub_sink)

if __name__ == "__main__":
    app.run()
```

### Configuration Options

- `project_id`: Your Google Cloud project ID
- `topic_id`: The ID of the Pub/Sub topic
- `service_account_json`: A JSON string containing service account credentials for authentication
- `value_serializer`: Function to serialize message values (default: `json.dumps`)
- `key_serializer`: Function to serialize message keys (default: `bytes.decode`)
- `flush_timeout`: Maximum time in seconds to wait for pending publishes during flush (default: 5)
- Additional keyword arguments are passed to the Pub/Sub `PublisherClient`

## Error Handling and Delivery Guarantees

The sink provides **at-least-once** delivery guarantees, which means:

- Messages are published asynchronously for better performance
- During checkpointing, the sink waits for all pending publishes to complete
- The wait time is controlled by `flush_timeout` parameter (defaults to 5 seconds)
- If any messages fail to publish within the flush timeout, a `SinkBackpressureError` is raised
- When `SinkBackpressureError` occurs:
  - The application will retry the entire batch from the last successful offset
  - Some messages that were successfully published in the failed batch may be published again
  - This ensures no messages are lost, but some might be delivered more than once

This behavior makes the sink reliable but means downstream systems should be prepared to handle duplicate messages. If your application requires exactly-once semantics, you'll need to implement deduplication logic in your consumer.

## Local Testing

You can test the PubSubSink locally using the Google Cloud Pub/Sub emulator. Here's how to set it up:

1. Start the emulator using Docker:

    ```shell
    docker run -d --name pubsub-emulator \
        -p 8085:8085 \
        gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
        gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
    ```

2. Set the environment variable to point to the local emulator:

    ```shell
    export PUBSUB_EMULATOR_HOST=localhost:8085
    ```

When this environment variable is set, the PubSubSink will automatically connect to the local emulator instead of Google Cloud. No service account credentials are required when using the emulator.
