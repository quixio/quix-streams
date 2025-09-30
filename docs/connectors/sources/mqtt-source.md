# MQTT Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source enables reading from an MQTT broker, dumping it to a
kafka topic using desired `StreamingDataFrame`-based transformations.

## How To Install

To use the MQTT source, you need to install the required dependencies:

```bash
pip install quixstreams[mqtt]
```

## How It Works

`MQTTSource` subscribes to an MQTT topic and produces its messages to a Kafka topic.

Messages are read in a streaming fashion and processed as they arrive, offering
real-time data ingestion from MQTT brokers.

The source supports various MQTT protocol versions and provides customizable
message handling through key, value, and timestamp setters.

You can learn more details about the [expected kafka message format](#message-data-formatschema) below.

## How To Use

To use MQTT Source, hand `MQTTSource` to `app.dataframe()`.

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.mqtt import MQTTSource

mqtt_source = MQTTSource(
    topic="sensors/temperature",
    client_id="my-mqtt-client",
    server="mqtt.broker.com",
    port=1883,
    username="your_username",
    password="your_password",
    version="3.1.1",
)

app = Application(
    broker_address="localhost:9092",
    consumer_group="mqtt-consumer",
)

sdf = app.dataframe(source=mqtt_source).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

## Configuration

Here are some important configurations to be aware of (see [MQTT Source API](../../api-reference/sources.md#mqttsource) for all parameters).

### Required:

- `topic`: MQTT topic to subscribe to. Use '#' as a wildcard for consuming from a base/prefix (e.g., "my-topic-base/#").
- `client_id`: MQTT client identifier.
- `server`: MQTT broker server address.
- `port`: MQTT broker server port.

### Optional:

- `username`: Username for MQTT broker authentication.
- `password`: Password for MQTT broker authentication.
- `version`: MQTT protocol version ("3.1", "3.1.1", or "5").
    **Default**: `"3.1.1"`
- `tls_enabled`: Whether to use TLS encryption.
    **Default**: `True`
- `qos`: Quality of Service level (0 or 1; 2 not yet supported).
    **Default**: `1`
- `payload_deserializer`: An optional payload deserializer. Useful when payloads are used by key, value, or timestamp setters.
    **Default**: JSON deserializer
- `key_setter`: Function to extract message key from MQTT message.
    **Default**: Uses "_key" from payload or topic name
- `value_setter`: Function to extract message value from MQTT message.
    **Default**: Raw message payload
- `timestamp_setter`: Function to extract timestamp from MQTT message.
    **Default**: Uses "_timestamp" from payload or message timestamp
- `properties`: MQTT properties (MQTT v5 only).
- `on_client_connect_success`: Optional callback for successful client authentication.
- `on_client_connect_failure`: Optional callback for failed client authentication.

## Message Data Format/Schema

This is the default format of messages handled by `Application`:

- Message `key` will be extracted using the `key_setter` function. By default, it uses the "_key" field from the payload (if JSON) or falls back to the MQTT topic name.

- Message `value` will be extracted using the `value_setter` function. By default, it returns the raw MQTT message payload in bytes.

- Message `timestamp` will be extracted using the `timestamp_setter` function. By default, it uses the "_timestamp" field from the payload (if JSON) or falls back to the MQTT message timestamp.

## Processing/Delivery Guarantees

`MQTTSource` processing guarantees depend on the configured QoS level:

- QoS 0: At most once delivery - messages are considered read without any explicit acknowledgement
- QoS 1: At least once delivery - messages are considered read after successful client acknowledgement

> NOTE: Kafka-level guarantees are `at-least-once`.

## Testing Locally

You can test `MQTTSource` locally using a local MQTT broker like Mosquitto:

1. Run Mosquitto with custom config:

    ```bash
    # Create mosquitto config and run container
    docker run --rm -d --name mosquitto -p 1883:1883 -p 9001:9001 \
      --entrypoint sh eclipse-mosquitto \
      -c 'echo -e "listener 1883\nallow_anonymous true" > /mosquitto/config/mosquitto.conf && exec /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf'
    ```

2. Configure `MQTTSource` to connect to it:

    ```python
    # NOTE: no username/password
    mqtt_source = MQTTSource(
        topic="test/topic",
        client_id="my-test-client",
        server="localhost",
        port=1883,
        tls_enabled=False,  # Disable TLS for local testing
    )
    ```