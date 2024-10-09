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
    
    app.run()
    
if __name__ == "__main__":
    main()
```

## Supported sources

Quix Streams provides the following sources out of the box:

* [CSVSource](./csv-source.md): A source that reads data from a single CSV file.
* [KafkaReplicatorSource](./kafka-source.md): A source that replicates a topic from a Kafka broker to your application broker.
* [QuixEnvironmentSource](./quix-source.md): A source that replicates a topic from a Quix Cloud environment to your application broker.

To create a custom source, read [Creating a Custom Source](custom-sources.md).

## Multiprocessing

For good performance, each source runs in a subprocess. Quix Streams automatically manages the subprocess's setting up, monitoring, and tearing down. 

For multiplatform support, Quix Streams starts the source process using the [spawn](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) approach. As a side effect, each Source instance must be pickleable. If a source needs to handle unpickleable objects, it's best to initialize those in the source subprocess (in the `BaseSource.start` or `Source.run` methods).  

## Customize Topic Configuration

Sources work by sending data to intermediate Kafka topics, which StreamingDataFrames then consume and process.

By default, each Source provides a default topic based on its configuration.  
To customize the topic config, pass a new `Topic` object to the `app.dataframe()` method together with the Source instance. 

**Example:**

Provide a custom topic with four partitions to the source. 

```python
from quixstreams import Application
from quixstreams.sources import CSVSource
from quixstreams.models.topics import TopicConfig

def main():
    app = Application()
    # Create a CSVSource
    source = CSVSource(path="input.csv")
    
    # Define a topic for the CSVSource with a custom config
    topic = app.topic("my_csv_source", config=TopicConfig(num_partitions=4, replication_factor=1))
    
    # Pass the topic together with the CSVSource to a dataframe
    # When the CSVSource starts, it will produce data to this topic
    sdf = app.dataframe(topic=topic, source=source)
    sdf.print(metadata=True)
    
    app.run()

if __name__ == "__main__":
    main()
```

## Standalone sources

So far we have covered how to run Sources and process data within the same application.  

When you scale your processing applications to more instances, you may need to run only a single instance of the Source.  
For example, when the source is reading data from some Websocket API, and you want to process it with multiple apps.  

To achieve that, Sources can be run in a standalone mode.

**Example**

Running an imaginary Websocket source in a standalone mode to read data only once. 

```python
from quixstreams import Application

def main():
    app = Application()
    
    # Create an instance of SomeWebsocketSource
    source = SomeWebsocketSource(url="wss://example.com")
    
    # Register the source in the app
    app.add_source(source)
    
    # Start the application
    # The app will start SomeWebsocketSource, and it will produce data to the default intermediate topic
    app.run()

if __name__ == "__main__":
    main()
```

To customize the topic the Source will use, create a new `Topic` and pass it to the `app.add_source()` method:

```python
from quixstreams import Application
from quixstreams.sources import CSVSource
from quixstreams.models.topics import TopicConfig

def main():
    app = Application()
    # Create an instance of SomeWebsocketSource
    source = SomeWebsocketSource(url="wss://example.com")
    
    # Define a topic for the CSVSource with a custom config
    topic = app.topic("some-websocket-source", config=TopicConfig(num_partitions=4, replication_factor=1))
    
    # Register the source and topic in the application
    app.add_source(source=source, topic=topic)
    
    # Start the application
    app.run()

if __name__ == "__main__":
    main()
```
