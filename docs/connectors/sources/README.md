# Sources (beta)

The first step of any stream processing pipeline is to get data. Quix streams provide a source API to connect your data source to Kafka and a StreamingDataframe easily.

For example, using a CSV file:

```python
from quixstreams import Application
from quixstreams.sources import CSVSource

def main():
  app = Application()
  source = CSVSource(path="input.csv")

  sdf = app.dataframe(source=source)
  sdf.print(metadata=True)

  app.run(sdf)

if __name__ == "__main__":
  main()
```

## Supported sources

Quix streams provide a source out of the box.

* [CSVSource](./csv-source.md): A source that reads data from a single CSV file.

You can also implement your own, have a look at [Creating a Custom Source](custom-sources.md) for documentation on how to do that.

## Multiprocessing

For good performances, sources are run in a separate process. Setting up, monitoring, and tearing down the process is handled by Quix streams. For multiplatform support, the [spawn](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) start method is used to create the source process and as a side-effect, the source object needs to be pickleable. If a source needs to handle un-pickleable objects, it's best to initialize those in the source subprocess (in the source `start` method).

## Topics

Sources work by sending data to a Kafka topic. Then the StreamingDataframe consume from that topic.
Each source provides a default topic based on it's configuration. You can override the topic used by
specyfying a topic in the `app.dataframe()` method.

For example, a topic with 4 partitions

```python
from quixstreams import Application
from quixstreams.sources import CSVSource
from quixstreams.models.topics import TopicConfig

def main():
  app = Application()
  source = CSVSource(path="input.csv")
  topic = app.topic("my_csv_source", config=TopicConfig(num_partitions=4, replication_factor=1))

  sdf = app.dataframe(topic=topic, source=source)
  sdf.print(metadata=True)

  app.run(sdf)

if __name__ == "__main__":
  main()
```
