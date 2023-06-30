# Quickstart

Quix Streams provides you with a library for developing real-time streaming applications focused on time-series data.

If you would like to know more about Quix Streams, you can view the [Quix Streams GitHub repository](https://github.com/quixio/quix-streams){target=_blank}. Quix Streams is open source under the Apache 2.0 license.

In this quickstart guide you will learn how to start using Quix Streams as quickly as possible. This guide covers how to:

* Create a consumer
* Create a producer
* Create a producer/consumer transform 
* Connect to the Quix Platform

The typical stream processing pipline you create with Quix Streams involves producers, consumers, and transforms. Producers publish information into a topic, consumers subscribe to read information from a topic. Transforms typically consume data, process it in some way, and then publish the transformed data to a topic, or stream within a topic.

In this guide you'll learn how to create a producer that publishes data to a topic, a consumer that reads data from a topic, and a simple transform that consumes data from a topic, transforms it, and then publishes the new data to a topic.

Initially you will work with your local Kafka installation, and then you'll learn how you can connect to Quix Platform. In Quix Platform you can build your stream processing pipelines graphically.

## Prerequisites

The prerequisites for this guide are as follows:

* [Python 3.x](https://www.python.org/downloads/){target=_blank}.
* You have a local installation of [Kafka](https://kafka.apache.org/documentation/){target=_blank} up and running. You use this to test your code.

Optionally:

* Sign up for a free [Quix account](https://portal.platform.quix.ai/self-sign-up){target=_blank}. You may just want to connect to your own Kafka installation, but if you'd like to connect to the Quix Platform you'll need a free account.

## Getting help

If you need help with this guide, then please join our public Slack community [`The Stream`](https://quix.io/slack-invite){target=_blank}, and ask any questions you have there.

## Install

Make sure you have Python 3.x installed by running:

```shell
python --version
```

Install Quix Streams for Python locally:

```shell
pip install quixstreams
```

You can read information about installation for your platform on the [PyPi page](https://pypi.org/project/quixstreams/){target=_blank} for Quix Streams.

The [README](https://github.com/quixio/quix-streams) for Quix Streams also has the latest information on installation.

!!! note

    The following sections assume you have a local installation of [Kafka](https://kafka.apache.org/){target=_blank} running. 

## Create a Consumer

To create a simple consumer, follow these steps:

1. Create a directory for your project, you can call it anything you want, and change into the project directory. 

2. Create a file called `consumer.py` that contains the following code:

    ```python
    import quixstreams as qx
    import pandas as pd

    # Client connecting to Kafka instance locally without authentication. 
    client = qx.KafkaStreamingClient('127.0.0.1:9092')

    # Open the input topic where to consume data from.
    # For testing purposes we remove consumer group and always read from latest data.
    topic_consumer = client.get_topic_consumer("quickstart-topic", consumer_group=None, auto_offset_reset=qx.AutoOffsetReset.Latest)

    # consume streams
    def on_stream_received_handler(stream_received: qx.StreamConsumer):
        stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler

    # consume data (as Pandas DataFrame)
    def on_dataframe_received_handler(stream: qx.StreamConsumer, df: pd.DataFrame):
        print(df.to_string())

    # Hook up events before initiating read to avoid losing out on any data
    topic_consumer.on_stream_received = on_stream_received_handler

    print("Listening to streams. Press CTRL-C to exit.")
    # Handle graceful exit
    qx.App.run()
    ```

3. Run the code:

    ```shell
    python consumer.py
    ```

The code will wait for published messages and then print information about any messages received to the console. You'll next build a suitable producer than can publish messages to the example topic.

??? example "Understand the code"

    Click on the annotations to understand the consumer code:

    ```python
    import quixstreams as qx
    import pandas as pd # (1)


    # Client connecting to Kafka instance locally without authentication. 
    client = qx.KafkaStreamingClient('127.0.0.1:9092') # (2)

    # Open the input topic where to consume data from.
    # For testing purposes we remove consumer group and always read from latest data.
    input_topic = client.get_topic_consumer("quickstart-topic", consumer_group=None, auto_offset_reset=qx.AutoOffsetReset.Latest) # (3)

    # consume streams
    def on_stream_received_handler(stream_received: qx.StreamConsumer): # (4)
        stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler # (5)

    # consume data (as Pandas DataFrame)
    def on_dataframe_received_handler(stream: qx.StreamConsumer, df: pd.DataFrame): # (6)
        print(df.to_string()) # (7)

    # Hook up events before initiating read to avoid losing out on any data
    input_topic.on_stream_received = on_stream_received_handler # (8)

    print("Listening to streams. Press CTRL-C to exit.")
    # Handle graceful exit
    qx.App.run() # (9)
    ```

    1. Imports the [Pandas library](https://pandas.pydata.org/){target=_blank} can be used to handle tabular data in Quix Streams. This library is supported because it is widely used.
    2. Connects to a Kafka server. In this case the Kafka server is running locally.
    3. Opens the specified topic for reading.
    4. A function definition for the stream callback. This stream event handler will be called for all stream events across all streams.
    5. Registers the Pandas data reader callback. This is registered for data events within a stream, not globally for all streams. This is efficient as you might not need to use this handler on many streams.
    6. This function defines a Pandas data event callback.
    7. The function simply prints a Pandas data frame in this example.
    8. Registers the stream callback.
    9. Runs the application, and registers code to monitor termination signals. On shutdown the code performs tasks such as closing open file handles, flushing buffers, shutting down threads, and freeing up allocated memory. It also closes input and output streams in the correct order, and creates topics that don't exist on startup.

## Create a Producer

To create a simple producer follow these steps:

1. Start a new terminal tab.

2. In your project directory, create a file called `producer.py` that contains the following code:

    ```python
    import quixstreams as qx
    import time
    import datetime
    import math


    # Client connecting to Kafka instance locally without authentication. 
    client = qx.KafkaStreamingClient('127.0.0.1:9092')

    # Open the output topic where to produce data to.
    topic_producer = client.get_topic_producer("quickstart-topic")

    stream = topic_producer.create_stream()
    stream.properties.name = "Hello World python stream"
    stream.properties.metadata["my-metadata"] = "my-metadata-value"
    stream.timeseries.buffer.time_span_in_milliseconds = 100   # Send data in 100 ms chunks

    print("Sending values for 30 seconds.")

    for index in range(0, 3000):
        stream.timeseries \
            .buffer \
            .add_timestamp(datetime.datetime.utcnow()) \
            .add_value("ParameterA", math.sin(index / 200.0)) \
            .add_value("ParameterB", "string value: " + str(index)) \
            .add_value("ParameterC", bytearray.fromhex("51 55 49 58")) \
            .publish()
        time.sleep(0.01)

    print("Closing stream")
    stream.close()
    ```

3. Run the code:

    ```shell
    python producer.py
    ```

The code will publish a series of messages to the specified topic.

4. Switch to the consumer terminal tab and view the messages being displayed. The following shows an example data frame:

    ```
                    time  ParameterA          ParameterB ParameterC
    0  1675695013706982000    0.687444  string value: 2990    b'QUIX'
    1  1675695013719422000    0.683804  string value: 2991    b'QUIX'
    2  1675695013730504000    0.680147  string value: 2992    b'QUIX'
    3  1675695013745346000    0.676473  string value: 2993    b'QUIX'
    4  1675695013756586000    0.672782  string value: 2994    b'QUIX'
    5  1675695013769315000    0.669075  string value: 2995    b'QUIX'
    6  1675695013782740000    0.665351  string value: 2996    b'QUIX'
    7  1675695013796677000    0.661610  string value: 2997    b'QUIX'
    ```

You've now created and tested both a producer and consumer that uses Quix Streams.

??? example "Understand the code"

    Click on the annotations to understand the producer code:

    ```python
    import quixstreams as qx
    import time
    import datetime
    import math


    # Client connecting to Kafka instance locally without authentication. 
    client = qx.KafkaStreamingClient('127.0.0.1:9092') # (1)

    # Open the output topic where to produce data to.
    topic_producer = client.get_topic_producer("quickstart-topic") # (2)

    stream = topic_producer.create_stream() # (3)
    stream.properties.name = "Quixstart Python stream" # (4)
    stream.properties.metadata["my-metadata"] = "my-metadata-value" # (5)
    stream.timeseries.buffer.time_span_in_milliseconds = 100   # (6)

    print("Sending values for 30 seconds.")

    for index in range(0, 3000):
        stream.timeseries \
            .buffer \
            .add_timestamp(datetime.datetime.utcnow()) \
            .add_value("ParameterA", math.sin(index / 200.0)) \
            .add_value("ParameterB", "string value: " + str(index)) \
            .add_value("ParameterC", bytearray.fromhex("51 55 49 58")) \
            .publish() # (7)
        time.sleep(0.01)

    print("Closing stream")
    stream.close() # (8)
    ```

    1. Opens a connection to the Kafka server.
    2. Opens a topic top write parameter data to.
    3. Creates the stream to write to.
    4. Sets a stream property, in this case `name`.
    5. Sets application-specific key-value metadata.
    6. Sets a stream buffer property. In this case `time_span_in_milliseconds` is set to 100. The data is then sent in 100ms chunks.
    7. Writes parameter data to the stream buffer. A time stamp is added. Also, data of different data types can be added, such as numbers, strings, and binary data.
    8. Closes the stream.

## Consumer-producer transform

Typically a transform block in Quix will receive some data on an input topic, perform some processing on the data, and then publish data to an output topic. Example code that does this is shown here:

```python
import quixstreams as qx
import pandas as pd


client = qx.KafkaStreamingClient('127.0.0.1:9092')

print("Opening consumer and producer topics")

topic_consumer = client.get_topic_consumer("quickstart-topic")
topic_producer = client.get_topic_producer("output-topic")

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print(df) 
    print('Data transformed') # Transform your data here
    # write data to output topic
    topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(df)

# read streams
def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler
    
topic_consumer.on_stream_received = on_stream_received_handler

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

# Handle graceful exit
qx.App.run()
```

This example reads data in from the `quickstart-topic` topic, and then writes the transformed data out to the `output-topic` topic. The approach is to use callbacks to make the code event driven. You register a callback to handle data on a stream, and then when data is received, the callback to handle data frames is registered and invoked.

This approach of consuming, transforming, and producing data is a fundamental of building data processing pipelines in Quix.

??? example "Understand the code"

    Click on the annotations to understand the producer/consumer code:

    ```python
    import quixstreams as qx
    import pandas as pd

    client = qx.KafkaStreamingClient('127.0.0.1:9092') # (1)

    print("Opening consumer and producer topics")

    topic_consumer = client.get_topic_consumer("quickstart-topic") # (2)
    topic_producer = client.get_topic_producer("output-topic") # (3)

    def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame): # (4)
        print(df) 
        print('Data transformed') # Transform your data here
        # write data to output topic
        topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(df) # (5)

    # read streams
    def on_stream_received_handler(stream_consumer: qx.StreamConsumer): # (6)
        stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler # (7)
        
    topic_consumer.on_stream_received = on_stream_received_handler # (8)

    # Hook up to termination signal (for docker image) and CTRL-C
    print("Listening to streams. Press CTRL-C to exit.")

    # Handle graceful exit
    qx.App.run() # (9)
    ```

    1. Opens a connection to the Kafka server.
    2. Opens the consumer topic, the topic is created if it does not exist.
    3. Opens the producer topic, the topic is created if it does not exist.
    4. Defines the data frame handler function. In this case it publishes data to the consumer stream.
    5. Gets a stream on the producer topic, creating the stream if it does not exist, and then publishes data to this stream.
    6. Defines the consumer stream data handler. It simply regsiters the data frame handler in this example.
    7. Registers the data frame handler for the consumer stream.
    8. Registers the consumer stream data handler.
    9. Runs the application, and registers code to monitor termination signals. On shutdown the code performs tasks such as closing open file handles, flushing buffers, shutting down threads, and freeing up allocated memory. It also closes input and output streams in the correct order, and creates topics that don't exist on startup.

## Connecting to Quix Platform

As well as being able to connect directly to a Kafka installation, either locally (for development purposes), on premise, or in the cloud, you can also connect to the Quix Platform, the SaaS for building real-time stream processing applications. Quix Platform provides the ability to build stream processing applications in a graphical environment, and deploy the applications to the Quix-hosted infrastructure.

### Obtaining a token

To connect to the Quix Platform using Quix Streams, you will need to provide a token for authentication.

1. Sign up for a free [Quix account](https://portal.platform.quix.ai/self-sign-up){target=_blank}, and log in.

2. In the Quix Platform, click on `Topics` on the left-hand navigation. 

3. Click on the gear icon. The `Broker Settings` dialog is displayed. 

4. Copy `token 1` to the clipboard. You will use that in the code that connects to the Quix platform.

### Code to connect to Quix Platform

The following code snippet shows you how to connect to the Quix Platform:

```python
import quixstreams as qx

# connect to Quix platform with token
client = qx.QuixStreamingClient('<your-token>') # Token 1 from Topics in portal
```

This connects to the Quix Platform, rather than your local Kafka installation, which is the code you saw previously in this guide.

A further example is to rewrite the consumer-producer program you created earlier in this Quickstart, to work with Quix Platform:

```python
import quixstreams as qx
import pandas as pd

client = qx.QuixStreamingClient('<your_sdk_token>')

print("Opening consumer and producer topics")

topic_consumer = client.get_topic_consumer("quickstart-topic")
topic_producer = client.get_topic_producer("output-topic")

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print(df) 
    # Transform your data here.
    print('transformed')
    # write data to output topic
    topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(df)

# read streams
def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler
    
topic_consumer.on_stream_received = on_stream_received_handler

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

# Handle graceful exit
qx.App.run()
```

## Next steps

Try one of the following resources to continue your Quix learning journey:

* [Get a free Quix account](https://portal.platform.quix.ai/self-sign-up){target=_blank}

* [Quix Streams GitHub](https://github.com/quixio/quix-streams){target=_blank}

* [Quix Platform glossary](https://quix.io/docs/platform/glossary.html)

* [The Stream community on Slack](https://quix.io/slack-invite){target=_blank}

* [Stream processing glossary](https://quix.io/stream-processing-glossary/){target=_blank}

* [Sentiment analysis tutorial](https://quix.io/docs/platform/tutorials/sentiment-analysis/index.html)

* [Kafka setup blog post](https://www.quix.io/blog/send-timeseries-data-to-kafka-python/?returnUrl=https://www.quix.io/blog/tutorial/){target=_blank}
