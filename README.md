![Quix - React to data, fast](https://github.com/quixio/.github/blob/main/profile/quixstreams-banner.jpg)

[//]: <> (This will be a banner image w/ the name e.g. Quix Streams.)

[![Quix on Twitter](https://img.shields.io/twitter/url?label=Twitter&style=social&url=https%3A%2F%2Ftwitter.com%2Fquix_io)](https://twitter.com/quix_io)
[![The Stream Community Slack](https://img.shields.io/badge/-The%20Stream%20Slack-blueviolet)](https://quix.io/slack-invite)
[![Linkedin](https://img.shields.io/badge/LinkedIn-0A66C2.svg?logo=linkedin)](https://www.linkedin.com/company/70925173/)
[![Events](https://img.shields.io/badge/-Events-blueviolet)](https://quix.io/community#events)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000.svg?logo=youtube)](https://www.youtube.com/channel/UCrijXvbQg67m9-le28c7rPA)
[![Docs](https://img.shields.io/badge/-Docs-blueviolet)](https://www.quix.io/docs/sdk/introduction.html)
[![Roadmap](https://img.shields.io/badge/-Roadmap-red)](https://github.com/orgs/quixai/projects/1)

# What is Quix Streams?

<b>Quix Streams</b> is a stream processing library, focused on <b>time-series data</b> and ease of use. It's designed to be used for high-frequency telemetry services when you need to process <b>high volumes</b> of time-series data with up to nanosecond precision. It uses a message broker such as <b>Apache Kafka</b>, instead of a database, so you can process time-series data with high performance and save resources without introducing delay.

Quix Streams <b>does not use</b> any domain-specific language or embedded framework, it's a library that you can use in your code base. This means that with Quix Streams you can use any external library for your chosen language. For example in Python, you can leverage Pandas, NumPy, PyTorch, TensorFlow, Transformers, OpenCV. 

Quix Streams currently supports the following languages:

- Python 
- C#

Quix Streams is [designed to be extended](/arch-notes.md) to multiple programming languages. 

You can use Quix Streams to:

- produce time-series and event data to a Kafka topic.
- consume time-series and event data from a Kafka topic.
- process data by creating pipelines using <b>publish–subscribe</b> pattern.
- Group data by streams to send different type of data (Timeseries, events, metadata or binary) into one ordered stream of data.

## What is time-series data?

Time-series data is a series of data points indexed in time order. Typically, time-series data is collected at regular intervals, such as days, hours, minutes, seconds, or milliseconds. In a data frame representation, each row of the data frame corresponds to a single time point, and each column contains a different variable or observation measured at that time point.

```
timestamp            value1   value2   value3   value4   value5   value6   value7
2022-01-01 01:20:00  25.3     10.1     32.3     56.2     15.3     12.2     27.1  
2022-01-01 01:20:01  26.2     11.2     31.2     55.1     16.2     13.1     28.2  
2022-01-01 01:20:02  24.1     12.3     30.1     54.3     17.1     14.2     29.1  
2022-01-01 01:20:03  23.4     13.4     29.2     53.2     18.3     15.3     30.2  
2022-01-01 01:20:04  22.6     14.5     28.3     52.1     19.2     16.2     31.1  
2022-01-01 01:20:05  22.4     14.6     28.1     52.8     19.2     16.4     31.1  
...                  ...      ...      ...      ...      ...      ...      ...   
```

Time-series data is often plotted on a graph with the x-axis representing the time at which the data was collected and the y-axis representing the value of the data point.

![Telemetry](./images/telemetry.png)

## Getting started 🏄

### Install Quix Streams

Install Quix streams with the following command: 

```shell
python3 -m pip install quixstreams
```

* To install Quix Streams on Macs with **M1** or **M2** chips, see our special installation guide: [Installing on Quix Streams on a M1/M2 Mac](mac-m1-m2-install.md).

### Install Kafka

This library needs to utilize a message broker to send and receive data. Quix uses [Apache Kafka](https://kafka.apache.org/) because it is the leading message broker in the field of streaming data, with enough performance to support high volumes of time-series data, with minimum latency.

**To install and test Kafka locally**:
* Download the Apache Kafka binary from the [Apache Kafka Download](https://kafka.apache.org/downloads) page.
* Extract the contents of the file to a convenient location (i.e. `kafka_dir`), and start the Kafka services with the following commands:<br><br>

  * **Linux / macOS**
    ```
    <kafka_dir>/bin/zookeeper-server-start.sh config/zookeeper.properties
    <kafka_dir>/bin/zookeeper-server-start.sh config/server.properties
    ```

  * **Windows**
    ```
    <kafka_dir>\bin\windows\zookeeper-server-start.bat.\config\zookeeper.properties
    <kafka_dir>\bin\windows\kafka-server-start.bat .\config\server.properties
    ```
* Create a test topic with the `kafka-topics` script.
  
  * **Linux / macOS**
    `<kafka_dir>/bin/kafka-topics.sh --create --topic mytesttopic --bootstrap-server localhost:9092`

  * **Windows**
    `bin\windows\kafka-topics.bat --create --topic mytesttopic --bootstrap-server localhost:9092`

You can find more detailed instructions in Apache Kafka's [official documentation](https://kafka.apache.org/quickstart).

To get started with Quix Streams, we recommend following the comprehensive [Quick Start guide](https://quix.io/docs/sdk/quickstart.html) in our official documentation. 

However, the following examples will give you a basic idea of how to produce and consume data with Quix Streams.:

### Producing time-series data

Here's an example of how to <b>produce</b> time-series data into a Kafka Topic with Python.

```python
import quixstreams as qx
import time
import datetime
import math
import os


# Quix injects credentials automatically to the client. 
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Open the output topic where to write data out
topic_producer = client.get_topic_producer(topic_id_or_name = os.environ["output"])

# Set stream ID or leave parameters empty to get stream ID generated.
stream = topic_producer.create_stream()
stream.properties.name = "Hello World Python stream"

# Add metadata about time series data you are about to send. 
stream.timeseries.add_definition("ParameterA").set_range(-1.2, 1.2)
stream.timeseries.buffer.time_span_in_milliseconds = 100

print("Sending values for 30 seconds.")

for index in range(0, 3000):
    stream.timeseries \
        .buffer \
        .add_timestamp(datetime.datetime.utcnow()) \
        .add_value("ParameterA", math.sin(index / 200.0) + math.sin(index) / 5.0) \
        .publish()
    time.sleep(0.01)

print("Closing stream")
stream.close()
```

### Consuming time-series data

Here's an example of how to <b>consume</b> time-series data from a Kafka Topic with Python:

```python
import quixstreams as qx
import os
import pandas as pd


client = qx.QuixStreamingClient()

# get the topic consumer for a specific consumer group
topic_consumer = client.get_topic_consumer(topic_id_or_name = os.environ["input"],
                                           consumer_group = "empty-destination")


def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    # do something with the data here
    print(df)


def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # subscribe to new DataFrames being received
    # if you aren't familiar with DataFrames there are other callbacks available
    # refer to the docs here: https://docs.quix.io/sdk/subscribe.html
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler


# subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()
```

Quix Streams allows multiple configurations to leverage resources while consuming and producing data from a Topic depending on the use case, frequency, language, and data types. 

For full documentation of how to [<b>consume</b>](https://www.quix.io/docs/sdk/subscribe.html) and [<b>produce</b>](https://www.quix.io/docs/sdk/publish.html) time-series and event data with Quix Streams, [visit our docs](https://www.quix.io/docs/sdk/introduction.html).

## Library features

The following features are designed to address common issues faced when developing real-time streaming applications:

### Streaming contexts
Streaming contexts allow you to bundle data from one data source into the same scope with supplementary metadata—thus enabling workloads to be horizontally scaled with multiple replicas.

* In the following sample, the `create_stream` function is used to create a stream called _bus-123AAAV_ which gets assigned to one particular consumer and will receive messages in the correct order: 

    ```python
    topic_producer = client.get_topic_producer("data")

    stream = topic_producer.create_stream("bus-123AAAV")
    # Message 1 sent (the stream context)

    stream.properties.name = "BUS 123 AAAV"
    # Message 2 sent (the human-readable identifier the bus)

    stream.timeseries\
        .buffer \
        .add_timestamp(datetime.datetime.utcnow()) \
        .add_value("Lat", math.sin(index / 100.0) + math.sin(index) / 5.0) \
        .add_value("Long", math.sin(index / 200.0) + math.sin(index) / 5.0) \
        .publish()
    # Message 3 sent (the time-series telemetry data from the bus)

    stream.events \
            .add_timestamp_in_nanoseconds(time.time_ns()) \
            .add_value("driver_bell", "Doors 3 bell activated by passenger") \
            .publish()
    # Message 4 sent (an event related to something that happened on the bus)
    ```

### Time-series data serialization and deserialization

Quix Streams serializes and deserializes time-series data using different codecs and optimizations to <b>minimize payloads</b> in order to increase throughput and reduce latency.

* The following example shows data being appended to as stream with the `add_value` method.<br><br>

    ```python
    # Open the producer topic where the data should be published.
    topic_producer = client.get_topic_producer("data")
    # Create a new stream for each device.
    stream = topic_producer.create_stream("bus-123AAAV")
    print("Sending values for 30 seconds.")

    for index in range(0, 3000):
        
        stream.timeseries\
            .add_timestamp(datetime.datetime.utcnow()) \
            .add_value("Lat", math.sin(index / 100.0) + math.sin(index) / 5.0) \
            .add_value("Long", math.sin(index / 200.0) + math.sin(index) / 5.0) \
            .publish()
    ```

### Built-in time-series buffers

If you’re sending data at <b>high frequency</b>, processing each message can be costly. The library provides built-in time-series buffers for producing and consuming, allowing several configurations for balancing between latency and cost.

* For example, you can configure the library to release a packet from the buffer whenever 100 items of timestamped data are collected or when a certain number of milliseconds in data have elapsed (note that this is using time in the data, not the consumer clock).

    ```
    buffer.packet_size = 100
    buffer.time_span_in_milliseconds = 100
    ```

* You can then read from the buffer and process it with the `on_read` function.

    ```python
    def on_read_dataframe(stream: StreamConsumer, df: pd.DataFrame):
        df["total"] = df["price"] * df["items_count"]

        topic_producer.get_or_create_stream(stream.stream_id).timeseries_data.write(df)

    buffer.on_dataframe_released = on_read_dataframe_handler
    ```

### Support for DataFrames

Time-series parameters are emitted at the same time, so they share one timestamp. Handling this data independently is wasteful. The library uses a tabular system that can work for instance with <b>Pandas DataFrames</b> natively. Each row has a timestamp and <b>user-defined tags</b> as indexes.

```python
# Callback triggered for each new data frame
def on_parameter_data_handler(stream: StreamConsumer, df: pd.DataFrame):
    
    # If the braking force applied is more than 50%, we mark HardBraking with True
    df["HardBraking"] = df.apply(lambda row: "True" if row.Brake > 0.5 else "False", axis=1)

    stream_producer.parameters.publish(df)  # Send data back to the stream
```

### Multiple data types

This library allows you to produce and consume different types of mixed data in the same timestamp, like <b>Numbers</b>, <b>Strings</b> or <b>Binary data</b>.

* For example, you can produce both time-series data and large binary blobs together.<br><br>

    Often, you’ll want to combine time series data with binary data. In the following example, we combine bus's onboard camera with telemetry from its ECU unit so we can analyze the onboard camera feed with context.

    ```python 
    # Open the producer topic where to publish data.
    topic_producer = client.get_topic_producer("data")

    # Create a new stream for each device.
    stream = topic_producer.create_stream("bus-123AAAV")

    telemetry = BusVehicle.get_vehicle_telemetry("bus-123AAAV")

    def on_new_camera_frame(frame_bytes):
        
        stream.timeseries\
            .buffer \
            .add_timestamp(datetime.datetime.utcnow()) \
            .add_value("camera_frame", frame_bytes) \
            .add_value("speed", telemetry.get_speed()) \
            .publish()
        
    telemetry.on_new_camera_frame = on_new_camera_frame
    ```

* You can also produce events that include payloads:<br><br>For example, you might need to listen for changes in time-series or binary streams and produce an event (such as "speed limit exceeded"). These  might require some kind of document to send along with the event message (e.g. transaction invoices, or a speeding ticket with photographic proof). Here's an example for a speeding camera:
  
    ```python
    # Callback triggered for each new data frame.
    def on_data_frame_handler(stream: StreamConsumer, df: pd.DataFrame):
            
        # We filter rows where the driver was speeding.
        above_speed_limit = df[df["speed"] > 130]

        # If there is a record of speeding, we sent a ticket.
        if df.shape[0] > O:

            # We find the moment with the highest speed.
            max_speed_moment = df['speed'].idxmax()
            speed = df.loc[max_speed_moment]
            time = df.loc[max_speed_moment]["time"]

            # We create a document that will be consumed by the ticket service.
            speeding_ticket = {
                'vehicle': stream.stream_id,
                        'time': time,
                        'speed': speed,
                        'fine': (speed - 130) * 100,
                        'photo_proof': df.loc[max_speed_moment]["camera_frame"]
                    }

            topic_producer.get_or_create_stream(stream.stream_id) \
                .events \
                .add_timestamp_in_nanoseconds(time) \
                .add_value("ticket", json.dumps(speeding_ticket)) \
                .publish()
    ```

### Support for stateful processing 

Quix Streams includes a state management feature that let's you store intermediate steps in complex calculations. To use it, you can create an instance of `LocalFileStorage` or use one of our helper classes to manage the state such as `InMemoryStorage`. 
Here's an example of a stateful operation sum for a selected column in data.

```python
state = InMemoryStorage(LocalFileStorage())

def on_g_force(stream_consumer: StreamConsumer, data: TimeseriesData):

    for row in data.timestamps:
		# Append G-Force sensor value to accumulated state (SUM).
        state[stream_consumer.stream_id] += abs(row.parameters["gForceX"].numeric_value)
				
		# Attach new column with aggregated values.
        row.add_value("gForceX_sum", state[stream_consumer.stream_id])

	# Send updated rows to the producer topic.
    topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(data)


# read streams
def read_stream(stream_consumer: StreamConsumer):
        # If there is no record for this stream, create a default value.
		if stream_consumer.stream_id not in state:
            state[stream_consumer.stream_id] = 0
		
	# We subscribe to gForceX column.
    stream_consumer.timeseries.create_buffer("gForceX").on_read = on_g_force


topic_consumer.on_stream_received = read_stream
topic_consumer.on_committed = state.flush
```

## Performance and Usability Enhancements

The library also includes a number of other enhancements that are designed to simplify the process of managing configuration and performance when interacting with Kafka:

- <b>No schema registry required</b>: Quix Streams doesn't need a schema registry to send different set of types or parameters, this is handled internally by the protocol. This means that you can send <b>more than one schema per topic</b><br>.

- <b>Message splitting</b>: Quix Streams automatically handles <b>large messages</b> on the producer side, splitting them up if required. You no longer need to worry about Kafka message limits. On the consumer side, those messages are automatically merged back.<br><br>

- <b>Message Broker configuration</b>: Many configuration settings are needed to use Kafka at its best, and the ideal configuration takes time. Quix Streams takes care of Kafka configuration by default but also supports custom configurations.<br><br>

- <b>Checkpointing</b>: Quix Streams supports manual or automatic checkpointing when you consume data from a Kafka Topic. This provides the ability to inform the Message Broker that you have already processed messages up to one point.<br><br>

- <b>Horizontal scaling</b>: Quix Streams handles horizontal scaling using the streaming context feature. You can scale the processing services, from one replica to many and back to one, and the library ensures that the data load is always shared between your replicas reliably.<br>

For a detailed overview of features, [visit our documentation](https://www.quix.io/docs/sdk/introduction.html).


### What's Next

This library is being actively developed. We have some more features planned in the library's [road map](https://github.com/orgs/quixai/projects/1) coming soon. The main highlight a new feature called "streaming data frames" that simplifies stateful stream processing for users coming from a batch processing environment. It eliminates the need for users to manage state in memory, update rolling windows, deal with checkpointing and state persistence, and manage state recovery after a service unexpectedly restarts. By introducing a familiar interface to Pandas DataFrames, we hopes to make stream processing even more accessible to data professionals who are new to streaming data.

The following example shows how you would perform rolling window calculation on a streaming data frame:

```python
# Create a projection for columns we need.
df = input_stream.df[["gForceX", "gForceY", "gForceZ"]] 

# Create new feature by simply combining three columns to one new column.
df["gForceTotal"] = df["gForceX"].abs() + df["gForceY"].abs() + df["gForceZ"].abs()

# Calculate rolling window of previous column for last 10 minutes
df["gForceTotal_avg10s"] = df["gForceTotal"].rolling("10m").mean()

# Loop through the stream row by row as data frow through the service. 
# Async iterator will stop the code if there is no new data incoming from i 
async for row in df:
    print(row)
    await output_stream.write(row)
```
Note that this is exactly how you would do the same calculation on static data in Jupyter notebook—so will be easy to learn for those of you who are used to batch processing. 

There's also no need to get your head around the complexity of stateful processing on streaming data—this will all be managed by the library. Moreover, although it will still feel like Pandas, it will use binary tables under the hood—which adds a significant performance boost compared to traditional Pandas DataFrames.

To find out when the next version is ready, make sure you watch this repo.

## Using Quix Streams with the Quix SaaS platform

This library doesn't have any dependency on any commercial product, but if you use it together with [Quix SaaS platform](https://www.quix.io) you will get some advantages out of the box during your development process such as auto-configuration, monitoring, data explorer, data persistence, pipeline visualization, metrics, and more.

## Contribution Guide

Contributing is a great way to learn and we especially welcome those who haven't contributed to an OSS project before. We're very open to any feedback or code contributions to this OSS project ❤️. Before contributing, please read our [Contributing File](https://github.com/quixai/quix-streams/blob/main/CONTRIBUTING.md) and familiarize yourself with our [architecture](./arch-notes.md) for how you can best give feedback and contribute. 

## Need help?

If you run into any problems, ask on #quix-help in [The Stream Slack channel](https://quix.io/slack-invite), alternatively create an [issue](https://github.com/quixai/quix-streams/issues)

## Roadmap

You can view and contribute to our feature [roadmap](https://github.com/orgs/quixai/projects/1).

## Community 👭

Join other software engineers in [The Stream](https://quix.io/slack-invite), an online community of people interested in all things data streaming. This is a space to both listen to and share learnings.

🙌  [Join our Slack community!](https://quix.io/slack-invite)

## License

Quix Streams is licensed under the Apache 2.0 license. View a copy of the License file [here](https://github.com/quixai/quix-streams/blob/main/LICENSE).

## Stay in touch 👋

You can follow us on [Twitter](https://twitter.com/quix_io) and [Linkedin](https://www.linkedin.com/company/70925173) where we share our latest tutorials, forthcoming community events and the occasional meme.  

If you have any questions or feedback - write to us at support@quix.ai!
