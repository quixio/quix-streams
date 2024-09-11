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

For good performance, each source runs in a subprocess. Quix Streams automatically manages the subprocess's setting up, monitoring, and tearing down. 

For multiplatform support, Quix Streams starts the source process using the [spawn](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) approach. As a side effect, each Source instance must be pickleable. If a source needs to handle unpickleable objects, it's best to initialize those in the source subprocess (in the source `start` method).  

## Topics

Sources work by sending data to Kafka topics. Then StreamingDataFrames consume these topics.

Each source provides a default topic based on its configuration. You can override the default topic by  
specifying a topic using the `app.dataframe()` method. 

**Example**

Provide a custom topic with four partitions to the source. 

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
