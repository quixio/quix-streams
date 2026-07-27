# MQTT Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes data to an MQTT broker. The sink publishes messages to MQTT topics based on the Kafka message key and preserves message ordering within each topic.

## How To Install

To use MQTTSink, you need to install the required dependencies:

```bash
pip install quixstreams[mqtt]
```

## How It Works

`MQTTSink` is a streaming sink that publishes messages to an MQTT broker.

Messages can optionally be retained and include MQTT properties.

For each message:

- The value is serialized (defaults to JSON)
- The key is converted to a string and used as the topic suffix
- It is published to `{topic_root}/{key}`

## How To Use

Create an instance of `MQTTSink` and pass it to the `StreamingDataFrame.sink()` method:

```python
from quixstreams import Application
from quixstreams.sinks.community.mqtt import MQTTSink

app = Application(broker_address="localhost:9092")
topic = app.topic("topic-name")

# Configure the sink
mqtt_sink = MQTTSink(
    client_id="my-mqtt-publisher",
    server="mqtt.broker.com",
    port=1883,
    topic_root="sensors",
    username="your_username",
    password="your_password",
)

sdf = app.dataframe(topic=topic)
sdf.sink(mqtt_sink)

if __name__ == "__main__":
    app.run()
```

## Configuration Options

### Required:

- `client_id`: MQTT client identifier
- `server`: MQTT broker server address
- `port`: MQTT broker server port
- `topic_root`: Root topic to publish messages to

### Optional:

- `username`: Username for MQTT broker authentication.
- `password`: Password for MQTT broker authentication.
- `version`: MQTT protocol version ("3.1", "3.1.1", or "5").
    **Default**: `"3.1.1"`
- `tls_enabled`: Whether to use TLS encryption.
    **Default**: `True`
- `key_serializer`: Function to serialize message keys to string.
    **Default**: `bytes.decode`
- `value_serializer`: Function to serialize message values.
    **Default**: `json.dumps`
- `qos`: Quality of Service level (0 or 1; 2 not yet supported).
    **Default**: `1`
- `mqtt_flush_timeout_seconds`: How long to wait for publish acknowledgment before failing.
    **Default**: `10`
- `retain`: Whether to retain messages for new subscribers. Can be a boolean or callable.
    **Default**: `False`
- `properties`: MQTT properties (MQTT v5 only). Can be a Properties instance or callable.
- `on_client_connect_success`: Optional callback for successful client authentication.
- `on_client_connect_failure`: Optional callback for failed client authentication.

## Error Handling and Delivery Guarantees

The sink provides delivery guarantees based on the configured QoS level:

- **QoS 0**: At most once delivery; messages are published without MQTT broker acknowledgment
- **QoS 1**: At least once delivery; messages are published with MQTT broker acknowledgment

During checkpointing, the sink waits for all pending publish acknowledgments to complete:

- The wait time is controlled by `mqtt_flush_timeout_seconds` parameter
- If any messages fail to publish within the flush timeout, an error is raised
- When errors occur:
  - The application will retry the entire batch from the last successful offset
  - Some messages that were successfully published in the failed batch may be published again
  - This ensures no messages are lost, but some might be delivered more than once

This behavior makes the sink reliable but downstream systems should be prepared to handle duplicate messages.

## Testing Locally

You can test `MQTTSink` locally using a local MQTT broker like Mosquitto:

1. Run Mosquitto with custom config:

    ```bash
    # Create mosquitto config and run container
    docker run --rm -d --name mosquitto -p 1883:1883 -p 9001:9001 \
      --entrypoint sh eclipse-mosquitto \
      -c 'echo -e "listener 1883\nallow_anonymous true" > /mosquitto/config/mosquitto.conf && exec /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf'
    ```

2. Configure `MQTTSink` to connect to it:

    ```python
    mqtt_sink = MQTTSink(
        client_id="test-publisher",
        server="localhost",
        port=1883,
        topic_root="test",
        tls_enabled=False,  # Disable TLS for local testing
    )
    ```
