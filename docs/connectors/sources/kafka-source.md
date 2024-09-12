# Kafka Source

A source that reads data from a Kafka topic and produce it to another Kafka topic. The two topics can be located on different Kafka clusters.

This source supports exactly-once guarantees.

## How to use the Kafka Source

To use a Kafka source, you need to create an instance of `KafkaSource` and pass it to the `app.dataframe()` method.

```python
from quixstreams import Application
from quixstreams.sources import KafkaSource

def main():
  app = Application()
  source = KafkaSource(
    name="my-source",
    app_config=app.config,
    topic="source-topic",
    broker_address="source-broker-address"
  )

  sdf = app.dataframe(source=source)
  sdf.print(metadata=True)

  app.run(sdf)

if __name__ == "__main__":
  main()
```

## Topic

The Kafka Source only deal with bytes. It read the remote keys and values as bytes and produce them directly as bytes. You can configure the key and value deserializer used by the Streaming Dataframe with the `key_deserializer` and `value_deserializer` paramaters.
