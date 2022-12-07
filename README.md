![Quix - Build realtime applications, faster](https://github.com/quixai/.github/blob/main/profile/quixstreams-banner.jpg)

[//]: <> (This will be a banner image w/ the name e.g. Quix Streams.)

[![Quix on Twitter](https://img.shields.io/twitter/url?label=Twitter&style=social&url=https%3A%2F%2Ftwitter.com%2Fquix_io)](https://twitter.com/quix_io)
[![The Stream Community Slack](https://img.shields.io/badge/-The%20Stream%20Slack-blueviolet)](https://quix.io/slack-invite)
[![Linkedin](https://img.shields.io/badge/LinkedIn-0A66C2.svg?logo=linkedin)](https://www.linkedin.com/company/70925173/)
[![Events](https://img.shields.io/badge/-Events-blueviolet)](https://quix.io/community#events)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000.svg?logo=youtube)](https://www.youtube.com/channel/UCrijXvbQg67m9-le28c7rPA)
[![Docs](https://img.shields.io/badge/-Docs-blueviolet)](https://www.quix.io/docs/sdk/introduction.html)
[![Roadmap](https://img.shields.io/badge/-Roadmap-red)](https://github.com/orgs/quixai/projects/1)

## What is Quix Streams

<b>Quix Streams</b> is a library for developing <b>real-time</b> streaming applications focused on <b>time-series data</b> and high-performance. It's designed to be used for high-frequency telemetry services when you need to process <b>high volume</b> of time-series data in nanoseconds precision. It uses a message broker - such as <b>Apache Kafka</b> - under the hood instead of a database, so you can process time-series data on the fly for high performance and resource savings.

Quix Streams <b>does not use</b> any Domain Specific Language or Embedded framework, it's just another library that you can use in your code base. This means you can use any data processing library of your chosen language together with Quix Streams.

Quix Streams is [designed to be extended](#interoperability-wrappers) to multiple programming languages. We currently support the following languages:
- Python 
- C#

Using Quix Streams, you can:

- Write time-series and non time-series data to a Kafka Topic
- Read time-series and non time-series data from a Kafka Topic
- Process data by reading it from one Kafka Topic, process it and writing back the results to another one.
- Group data by Streams attaching metadata to them

## Getting Started 🏄

### Installing the library
You can <b>install</b> the library using the package manager for Python Packages:

```shell
pip install quix-streams
```

### Prepare your Kafka Cluster

This library needs a message broker under the hood to send and receive data. We use [Apache Kafka](https://kafka.apache.org/) because it is the major message broker in the industry with enough performance to support high volumes of time-series data with minimum latency.

### Writing time-series data
Now you can start writing some time-series data into a Kafka Topic. Here's an example of how to <b>Write</b> time-series data into a Kafka Topic with Python.

```python
import time
import datetime
import math

from quixstreaming import StreamingClient

# Client connecting to Kafka instance locally without authentication. 
client = StreamingClient('127.0.0.1:9092')

# Open the output topic where to write data to.
output_topic = client.open_output_topic("your-kafka-topic")

stream = output_topic.create_stream()
stream.properties.name = "Hello World python stream"
stream.properties.metadata["my-metadata"] = "my-metadata-value"
stream.parameters.buffer.time_span_in_milliseconds = 100   # Send data in 100 ms chunks

print("Sending values for 30 seconds.")

for index in range(0, 3000):
    stream.parameters \
        .buffer \
        .add_timestamp(datetime.datetime.utcnow()) \
        .add_value("ParameterA", math.sin(index / 200.0)) \
        .add_value("ParameterB", "string value: " + str(index)) \
        .add_value("ParameterC", bytearray.fromhex("51 55 49 58")) \
        .write()
    time.sleep(0.01)

print("Closing stream")
stream.close()
```

### Reading time-series data
Once we have setup our producer, it's time to see how to read data from a topic. Here's an example of how to <b>Read</b> time-series data from a Kafka Topic with Python:

```python
import pandas as pd

from quixstreaming import *
from quixstreaming.app import App

# Client connecting to Kafka instance locally without authentication. 
client = StreamingClient('127.0.0.1:9092')

# Open the input topic where to read data from.
# For testing purposes we remove consumer group and always read from latest data.
input_topic = client.open_input_topic("your-kafka-topic", consumer_group=None, auto_offset_reset=AutoOffsetReset.Latest)

# read streams
def on_stream(input_stream: StreamReader):

    # read data (as Pandas DataFrame)
    def on_read_pandas(df: pd.DataFrame):
        print(df.to_string())

    input_stream.parameters.on_read_pandas += on_read_pandas

# Hook up events before initiating read to avoid losing out on any data
input_topic.on_stream_received += on_stream

print("Listening to streams. Press CTRL-C to exit.")
# Handle graceful exit
App.run()
```

Quix Streams allows multiple configurations to leverage resources while reading and writing data from a Topic depending on the use case, frequencies, language and data types. 

For full documentation of how to [<b>Read</b>](https://www.quix.io/docs/sdk/read.html) and [<b>Write</b>](https://www.quix.io/docs/sdk/write.html) time-series and non time-series data with Quix Streams, [visit our docs](https://www.quix.io/docs/sdk/introduction.html).

## Library features

This library provides several features and solves common problems you face when developing real-time streaming applications. 

<details>
    <summary><b>Stream context</b></summary>
    Quix Streams handles stream contexts for you, so all the data from one data source is bundled in the same scope. This allows message broker parelalize loads properly between multiple consumers. 
</details>

<details>
    <summary><b>Stream metadata</b></summary>
    Quix Streams allows to assign metadata attached to a Stream context and to the definition of his Parameters and Events. The library manage that metadata communication behind the scenes only sending and receiving it when necessary.
</details>

<details>
    <summary><b>Data serialization and de-serialization</b></summary>
    Quix streams serialize and deserialize time-series data using different codecs and optimizations to minimize payloads in order to increase throughtput and reduce latency.
</details>

<details>
    <summary><b>Built-in time-series buffers</b></summary>
    If you’re sending data at high frequency, processing each message can be costly. The library provides built-in time-series buffers for reading and writing allowing several configurations for balancing between latency and cost.
</details>

<details>
    <summary><b>Support for time-series data frames</b></summary>
    In many use cases, multiple time-series parameters are emitted at the same time, so they share one timestamp. Handling this data independently is wasteful. This library uses an optimized tabular system and can work for instance with Pandas DataFrames natively. Each row has a timestamp and user-defined tags as indexes.
</details>

<details>
    <summary><b>Multiple data types</b></summary>
    This library allows you to attach different types of data to your timestamps, like Numbers, Strings or Binary data.
</details>

<details>
    <summary><b>In-memory data processing</b></summary>
    Quix Streams is designed to make in-memory data processing very efficient. It uses several cpu and memory optimizations in conjunction with the message broker capabilities to achieve maximum throughput with very minimum latency.
</details>

<details>
    <summary><b>Message splitting</b></summary>
    Quix Streams automatically handles large messages on the producer side, splitting them up if required. You no longer need to worry about Kafka message limits. On the consumer side, those messages are automatically merged back.
</details>

<details>
    <summary><b>Message compression</b></summary>
    Quix Streams compresses messages using built-in codecs like Protobuf, reducing them by an average factor of 10 times. 
</details>

<details>
    <summary><b>Message Broker configuration</b></summary>
    Many configuration settings are needed to use Kafka at its best, and the ideal configuration takes time. The library take care about Kafka configuration by default allowing refined configuration only when needed.
</details>

<details>
    <summary><b>Checkpointing</b></summary>
    Quix Streams allows manual or automatic checkpointing when you read data from a Kafka Topic. This provides the ability to inform the Message Broker that you have already processed messages up to one point.
</details>

<details>
    <summary><b>Horizontal scaling</b></summary>
    Quix Streams handles horizontal scale out of the box via the streaming context feature. You can scale the processing services, from one replica to many and back to one, and the library ensures that the data load is always shared between your replicas consistenly.
</details>

For a detailed overview of features, [visit our docs.](https://www.quix.io/docs/sdk/introduction.html)

### Comming soon

This library is actively in developing process. We have some features planned in the [road map](https://github.com/orgs/quixai/projects/1) of the library comming soon:

(WIP)

## Library architecture notes

### Interoperability wrappers

Quix Streams base library is developed in C#. We use Interoperability wrappers around <b>C# AoT (Ahead of Time) compiled code</b> to implement other languages support like <b>Python</b>. These Interop wrappers are auto-generated using a project called `InteropGenerator` included in the same repository. (Ahead-of-time native compilation was a feature introduced officially on .NET 7. Learn more [here](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/))

You can generate these Wrappers again using the `shell scripts` provided for each platform inside the language specific client. For instance for Python:

- `/PythonClient/buildwindows.bat`: Generates Python Interop wrappers for Windows platform.
- `/PythonClient/buildlinux.sh`: Generates Python Interop wrappers for Linux platform.

These scripts are compiling the C# base library and then using the `InteropGenerator` project to generate the AoT compiled version of the library and the Interops wrappers around that. The result is an structure like this:

```

   ┌───────────────────────────┐
   │  Python client library    │    /PythonClient/lib/quixstreaming
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │  Python Interop wrapper   │    /PythonClient/lib/quixstreaming/native/Python  (auto-generated)
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │  C# AoT compiled library  │    /PythonClient/lib/quixstreaming/native/win64   (auto-generated)
   └───────────────────────────┘

```

The non auto-generated `Python client library` still need to be implemented manually but it is something expected because each language has his own language specific features and naming conventions that we want to keep aligned with the language user expectations. If you want to add a new feature of the library that is common to all the languages you should implement that feature in the C# base library first, re-generate the Interop wrappers, and then modify the Python client library to wire up the new feature of the base library.

### Base library

Quix Streams base library is implemented in C#, therefore if your target language is C# you will use directly that base library without any [Interoperability wrapper](#interoperability-wrappers) involved on the execution. 

This base library is organized in 3 main layers:

```

   ┌───────────────────────────┐
   │      Streaming layer      │    /CSharpClient/Quix.Streams.Streaming
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │       Process layer       │    /CSharpClient/Quix.Streams.Process
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │   Kafka Transport layer   │    /CSharpClient/Quix.Streams.Transport.Kafka
   ├───────────────────────────┤
   │      Transport layer      │    /CSharpClient/Quix.Streams.Transport
   └───────────────────────────┘

```

 Each layer has his own responsabilities:
 
- <b>Streaming layer</b>: It is the main layer of the library that users should use by default. It includes among others, all the <b>syntax sugar</b> needed to have a pleasant experience with the library. Another important responsability of this layer is the <b>embedded time-series buffer</b> system.

- <b>Process layer</b>: This layer is responsible of implementing the `Codecs` serialization and de-serialization of all the <b>Telemetry messages</b> of the Quix Streams protocol like time-series and non time-series messages, stream metadata, stream properties messages, parameters definitions, as well as the creating the [Stream context](#library-features-) scopes responsible of the separation between data comming from different sources. This layer also implement a `Stream Process` system to concatenate different Stream processes that can be used to implement complex low-level Telemetry services.

- <b>Transport layer</b>: This layer is responsible of the <b>communication with the message broker</b> and implementing some features to deal with the own message broker features and limitations. Some of these features are `message splitting`, `checkpointing`, `partition revokation`, `connectivity issues recovering` among others. This layer is also responsible to implement a `wrapping messages system` to allow different messages types of the library Protocol and to define the base classes for the `Codecs` implementation of each messages of that Protocol on ther upper layers of the library. For <b>Kafka</b> support this base library uses internally [Confluent .NET Client for Apache Kafka](https://github.com/confluentinc/confluent-kafka-dotnet), that at the same time uses the library [librdkafka - the Apache Kafka C/C++ client library](https://github.com/edenhill/librdkafka) under the hood.

For wider information and general questions about the architecture of the library you can join to our official [Slack channel](https://quix.io/slack-invite).

## Using Quix Streams with Quix SaaS platform

This library doesn't have any dependency to any comercial product, but if you use it together with [Quix SaaS platform](https://www.quix.io) you will get some advantatges out of box during your development process like auto-configuration, monitoring, data explorer, data persistence, pipeline visualization, metrics and more.

## Contribution Guide

Contributing is a great way to learn and we especially welcome those who haven't contributed to an OSS project before. We're very open to any feedback or code contributions to this OSS project ❤️. Read our [Contributing File](https://github.com/quixai/quix-streams/blob/main/CONTRIBUTING.md) for how you can best give feedback and contribute. 

## Need help?
If you run into any problems, ask on #quixhelp in [The Stream Slack channel](https://quix.io/slack-invite), alternatively create an [issue](https://github.com/quixai/quix-streams/issues)

## Roadmap

You can view and contribute to our feature roadmap [here](https://github.com/orgs/quixai/projects/1)

## Community 👭

Join other software engineers in [The Stream](https://quix.io/slack-invite), an online community of people interested in all things data streaming. This is a space to both listen to and share learnings.

🙌  [Join our Slack community!](https://quix.io/slack-invite)

## License

Quix Streams is licensed under the Apache 2.0 license. View a copy of the License file [here](https://github.com/quixai/quix-streams/blob/main/LICENSE).

## Stay in touch 👋

You can follow us on [Twitter](https://twitter.com/quix_io) and [Linkedin](https://www.linkedin.com/company/70925173) where we share our latest tutorials, forthcoming community events and the occasional meme.  

If you have any questions or feedback - write to us at support@quix.ai!
