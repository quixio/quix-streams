# Introduction

Quix Streams makes it quick and easy to develop streaming applications. It’s designed to be used for high-performance telemetry services where you need to process high volumes of data with quick response time in a scalable way.

Quix Streams is available for Python and C\#.

Using Quix Streams, you can:

  - [Publish time-series data](publish.md) to a Kafka topic

  - [Subscribe to time-series data](subscribe.md) from a Kafka topic

  - [Process data](process.md) by [subscribing](subscribe.md) to it from one topic and [publishing](publish.md) the results to another one or elsewhere.

To support these operations, Quix Streams provides several useful features and solves common problems you may face when developing real-time streaming applications:

## Streaming context

Quix Streams handles [stream contexts](features/streaming-context.md) for you, so all the data from one data source is bundled in the same scope. This allows you to attach metadata to streams.

Stream context simplifies processing streams by providing callbacks on the subscribing side. You can keep working with each context (stream) separately or together, depending on your needs.

Refer to the [Streaming context](features/streaming-context.md) section of this documentation for more information.

## Built-in buffers

If you’re sending data at high frequency, processing each message can be costly. Quix Streams provides a built-in buffers features for reading and writing to give you freedom in balancing between latency and cost.

Refer to the [Built-in buffers](features/builtin-buffers.md) section of this documentation for more information.

## Support for data frames

In many use cases, multiple parameters are emitted at the same time, so they share one timestamp. Publishing these parameters independently is wasteful. Quix Streams uses a rows system and can work with [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe) natively. Each row has a timestamp and user-defined tags as indexes.

Refer to the [Support for Data Frames](features/data-frames.md) section of this documentation for more information.

## Message splitting

Quix Streams automatically handles large messages on the producer side, splitting them up when required. On the consumer side, those messages are automatically merged back and processed as one.

Refer to the [Message splitting](features/message-splitting.md) section of this documentation for more information.

## Data ser/des

Quix Streams automatically serializes data from native types in your language. You can work with familiar data types, such as [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe), without worrying about conversion. Serialization can be difficult, especially if it is done with performance in mind. We serialize native types using our codecs so you don’t have to implement that.

Refer to the [Data serialization](features/data-serialization.md) section of this documentation for more information.

## Multiple data types

Quix Streams allows you to attach different types of data to your timestamps, such as numbers, string and binary data. This gives Quix Streams the ability to adapt to any streaming application use case.

Refer to the [Multiple data types](features/multiple-data-types.md) section of this documentation for more information.

## Checkpointing

Quix Streams allows you to do manual checkpointing when you subscribe to data from a topic. This provides the ability to inform the message broker that you have already processed messages up to one point, called a **checkpoint**.

This is a very important concept when you are developing high-performance streaming applications.

Refer to the [Checkpointing](features/checkpointing.md) section of this documentation for more information.

## Horizontal scaling

Quix Streams provides horizontal scaling using the [streaming context](features/streaming-context.md) feature. Data scientists or engineers do not have to implement horizontal scaling for stream processing themselves. You can scale the processing models, from one replica to many or back while relying on the [callback system](subscribe.md#parallel-processing--this-does-not-exist) to ensure that your data is distributed between your model replicas.

Refer to the [Horizontal scaling](features/horizontal-scaling.md) section of this documentation for more information.

## Raw messages

Quix Streams uses an internal protocol which is both data and speed optimized so we do encourage you to use it, but you need to use Quix Streams on both producer and consumer sides as of today. We have plans to support most common formats in near future, but custom formats will always need to be handled manually.

For this, we have created a way to [publish](publish.md#write-raw-kafka-messages) and [subscribe](subscribe.md#read-raw-kafka-messages) to the raw, unformatted messages and work with them as bytes. This gives you the ability to implement the protocol as needed and convert between formats.
