# Kafka Replicator Source

A source that reads data from a Kafka topic and produce it to another Kafka topic. The two topics can be located on different Kafka clusters.

This source supports exactly-once guarantees.

## How to use the Kafka Replicator Source

To use a Kafka Replicator source, you need to create an instance of `KafkaReplicatorSource` and pass it to the `app.dataframe()` method.

```python
from quixstreams import Application
from quixstreams.sources import KafkaReplicatorSource

def main():
  app = Application()
  source = KafkaReplicatorSource(
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

The Kafka Replicator source only deals with bytes. It reads the remote keys and values as bytes and produces them directly as bytes.
You can configure the key and value deserializer used by the Streaming Dataframe with the `key_deserializer` and `value_deserializer` paramaters.

## Consumer group

The Kafka Replicator consumer group is build from the application consumer group and the source name. It is used to store the processed messages offset. Changing either the application consumer group or the source name will reset the Kafka Replicator state. For more information see [the glossary](https://quix.io/docs/kb/glossary.html?h=consumer+group#consumer-group)

The Kafka Replicator commit the messages offset on the destination cluster. Only read access on the source cluser is necessary.
