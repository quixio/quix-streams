# Introduction

Quix Streams helps simplify the process of developing streaming applications. It’s designed to be used for high-performance telemetry services where you need to process high volumes of data with quick response times in a scalable way.

Quix Streams is available for Python and C\#.

Using Quix Streams, you can:

* [Publish time-series data](publish.md) to a Kafka topic.

* [Subscribe to time-series data](subscribe.md) from a Kafka topic.

* [Process data](process.md) by [subscribing](subscribe.md) to it from one topic and [publishing](publish.md) the results to another topic.

To support these operations, Quix Streams provides several useful features, and solves common problems you may face when developing streaming applications:

## Streaming context

Quix Streams handles [stream contexts](features/streaming-context.md) for you, so all the data from one data source is bundled in the same scope. This enables you to attach metadata to streams.

Stream context simplifies processing streams by providing callbacks on the subscribing side. You can keep working with each context (stream) separately or together, depending on your needs.

Refer to the [Streaming context](features/streaming-context.md) section of this documentation for more information.

## Built-in buffers

If you’re sending data at high frequency, processing each message can be costly. Quix Streams provides built-in buffers for reading and writing, to give you freedom in how you balance latency and cost.

Refer to the [Built-in buffers](features/builtin-buffers.md) section of this documentation for more information.

## Support for data frames

In many use cases, multiple parameters are emitted at the same time, so they share one timestamp. Publishing these parameters independently is wasteful. Quix Streams uses a rows system and can work directly with [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe). Each row has a timestamp and user-defined tags as indexes.

Refer to the [Support for Data Frames](features/data-frames.md) section of this documentation for more information.

## Message splitting

Quix Streams automatically handles large messages on the producer side, splitting them up when required. On the consumer side, those messages are automatically merged back and processed as one.

Refer to the [Message splitting](features/message-splitting.md) section of this documentation for more information.

## Data serialization and deserialization

Quix Streams automatically serializes data from types provided in your programming language. You can work with familiar data types, such as [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe). 

Serialization can be difficult, especially if it is done with performance in mind, but Quix Streams serializes your language types, for you, using built in codecs.

Refer to the [Data serialization](features/data-serialization.md) section of this documentation for more information.

## Multiple data types

Quix Streams enables you to attach different types of data to your timestamps, such as numbers, string and binary data. This gives Quix Streams the ability to adapt to any streaming application use case.

Refer to the [Multiple data types](features/multiple-data-types.md) section of this documentation for more information.

## Checkpointing

Quix Streams enables you to do manual checkpointing when you subscribe to data from a topic. This provides the ability to inform the message broker that you have already processed messages up to one point, called a **checkpoint**.

This is a very important concept when you are developing high-performance streaming applications.

Refer to the [Checkpointing](features/checkpointing.md) section of this documentation for more information.

## Horizontal scaling

Quix Streams provides horizontal scaling using the [streaming context](features/streaming-context.md) feature. Data scientists or engineers do not have to implement horizontal scaling for stream processing themselves. You can scale the processing models, from one replica to many, to ensure that your data is distributed between your model replicas.

Refer to the [Horizontal scaling](features/horizontal-scaling.md) section of this documentation for more information.

## Raw messages

Quix Streams uses an internal protocol which is both data and speed optimized for maximum performance. However, you do need to use Quix Streams on both producer and consumer side to take advantage of this. 

Custom formats can be handled by writing dedicated code. For this, the library provides a way to [publish](publish.md#write-raw-kafka-messages) and [subscribe](subscribe.md#read-raw-kafka-messages) to the raw, unformatted messages, and work with them as bytes. This gives you the ability to implement the protocol as needed and convert between formats.
