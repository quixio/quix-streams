# Introduction

The Quix SDK makes it quick and easy to develop streaming applications.
It’s designed to be used for high performance telemetry services where
you need to process high volumes of data in a nanosecond response time.

The SDK is available for Python and C\#.

Using the Quix SDK, you can:

  - [Write time-series data](/sdk/write) to a Kafka Topic

  - [Read time-series data](/sdk/read) from a Kafka Topic

  - [Process data](/sdk/process) by [reading](/sdk/read) it from one
    Topic and [writing](/sdk/write) the results to another one.

To support these operations, the SDK provides several useful features
out of the box, and solves all the common problems you should face when
developing real-time streaming applications:

## Streaming context

The Quix SDK handles [stream contexts](/sdk/features/streaming-context)
for you, so all the data from one data source is bundled in the same
scope. This allows you to attach metadata to streams.

The SDK simplifies the processing of streams by providing callbacks on
the reading side. When processing stream data, you can identify data
from different streams more easily than with the key-value approach used
by other technologies.

Refer to the [Streaming context](/sdk/features/streaming-context)
section of this documentation for more information.

## In-memory data processing

The Quix SDK is designed to make [in-memory data
processing](/sdk/features/in-memory-processing) extremely efficient. We
use high-performance SDK features in conjunction with the message broker
capabilities to achieve maximum throughput with the very minimum
latency.

Refer to the [In-memory data
processing](/sdk/features/in-memory-processing) section of this
documentation for more information.

## Built-in buffers

If you’re sending data at high frequency, processing each message can be
costly. The SDK provides a built-in buffers features for reading and
writing to give you absolute freedom in balancing between latency and
cost.

Refer to the [Built-in buffers](/sdk/features/builtin-buffers) section
of this documentation for more information.

## Support for data frames

In many use cases, multiple parameters are emitted at the same time, so
they share one timestamp. Handling this data independently is wasteful.
The SDK uses a rows system, and can work with [Pandas
DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe)
natively. Each row has a timestamp and user-defined tags as indexes.

Refer to the [Support for Data Frames](/sdk/features/data-frames)
section of this documentation for more information.

## Message splitting

The SDK automatically handles large messages on the producer side,
splitting them up if required. You no longer need to worry about Kafka
message limits. On the consumer side, those messages are automatically
merged back.

Refer to the [Message splitting](/sdk/features/message-splitting)
section of this documentation for more information.

## Message compression

The Quix SDK automatically compresses your messages, reducing them by an
average factor of 10 times. You save money via added efficiency.

The SDK also sends parameter values as the delta between timestamps,
converting strings to flags, and in general reduces payload size for
each message. This happens before compression is applied, so the final
compression ratio is even higher.

Refer to the [Message compression](/sdk/features/message-compression)
section of this documentation for more information.

## Data serialization and de-serialization

The Quix SDK automatically serializes data from native types in your
language. You can work with familiar data types, such as [Pandas
DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe),
without worrying about conversion. Serialization can be painful,
especially if it is done with performance in mind. We serialize native
types using our codecs so you don’t have to worry about that.

Refer to the [Data serialization](/sdk/features/data-serialization)
section of this documentation for more information.

## Multiple data types

The SDK allows you to attach any type of data to your timestamps, like
Numbers, Strings or even raw Binary data. This gives the SDK the ability
to adapt to any streaming application use case.

Refer to the [Multiple data types](/sdk/features/multiple-data-types)
section of this documentation for more information.

## Message Broker configuration including authentication and authorization

Quix handles Kafka configuration efficiently and reliably. Our templates
come with pre-configured certificates and connection settings. Many
configuration settings are needed to use Kafka at its best, and the
ideal configuration takes time\! We take care of this in the SDK so you
don’t have to.

Refer to the [Broker configuration](/sdk/features/broker-configuration)
section of this documentation for more information.

## Checkpointing

The SDK allows you to do manual checkpointing when you read data from a
Topic. This provides the ability to inform the Message Broker that you
have already processed messages up to one point, usually called a
**checkpoint**.

This is a very important concept when you are developing high
performance streaming applications.

Refer to the [Checkpointing](/sdk/features/checkpointing) section of
this documentation for more information.

## Horizontal scaling

The Quix SDK provides horizontal scale out of the box via the [streaming
context](/sdk/features/streaming-context.md) feature. This means a data
scientist or data engineer does not have to implement parallel
processing themselves. You can scale the processing models, from one
replica to many and back to one, and use the [callback system inside the
SDK](/sdk/read.md#_parallel_processing) to ensure that your data load is
always shared between your model replicas.

Refer to the [Horizontal scaling](/sdk/features/horizontal-scaling)
section of this documentation for more information.

## Integrations

The SDK offers integrations out of the box, including data persistence
and historic or real-time APIs with other systems. That means you don’t
have to implement them by yourself.

Refer to the [Integrations](/sdk/features/integrations) section of this
documentation for more information.

## Portability

The Quix SDK is an abstraction layer over a concrete broker technology.
You’re not locked into a specific broker and can innovate over time.

Refer to the [Portability](/sdk/features/portability) section of this
documentation for more information.

## Raw messages

The Quix SDK uses an internal protocol which is both data and speed
optimized so we do encourage you to use it. For that you need to use the
SDK on both producer ( writer ) and consumer ( reader ) sides.

However, in some cases, you simply do not have the ability to run the
Quix SDK on both sides.

To cater for these cases we added the ability to both
[write](/sdk/write.md#write-raw-kafka-messages) and [read](/sdk/read.md#read-raw-kafka-messages)
the raw, unformatted, messages as byte array. This is giving you the
freedom to implement the protocol as needed ( e.g. JSON, comma-separated
rows ).
