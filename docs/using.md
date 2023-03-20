# Using Quix Streams

In this topic you will learn how to use Quix Streams to perform two types of data processing:

1. **Stateless processing** - Here one message is processed at a time, and the message received contains all required data for processing. No state needs to be preserved between messages, or between replicas. The data from the message is used to calculate a new value, which is then typically published to the output stream. 
2. **Stateful processing** - This is where you need to keep track of data between messages, such as keeping a running total of a variable. This is more complicated as state needs to be preserved between messages, and potentially between replicas, where multiple replicas are deployed. In addition, state may need to be preserved in the event of the failure of a deployment - Quix Streams supports checkpointing as a way to enable this.

The following sections will explore these methods of data processing in more detail.

## Topics, streams, partitions, replicas, and consumer groups

The main structure used for data organization in Quix is the topic. For example, the topic might be `iot-telemetry`. To allow for horizontal scaling, a topic is typically divided into multiple streams. You may have multiple devices, or sources, writing data into a topic, so to ensure scaling and message ordering, each source writes into its own stream. Device 1 would write to stream 1, and device 2 to stream 2 and so on. This is the idea of [stream context](./features/streaming-context.md). 

Quix Streams ensures that stream context is preserved, that is, messages inside one stream are always published to the same single partition. This means that inside one stream, a consumer can rely on the order of messages. A partition can contain multiple streams, but a stream is always confined to one partition.

It is possible to organize the code that processes the streams in a topic using the idea of a consumer group. This indicates to the broker that you will process the topic with all available replicas.

Horizontal scaling occurs automatically, because when you deploy multiple replicas, a stream is assigned to a replica. For example, if there are three streams and three replicas, each replica will process a single stream. If you had only one replica, it would need to process all streams in that topic. If you have three streams and two replicas, one replica would process two streams, and the other replica a single stream.

When you create the consumer you specify the consumer group as follows:

```python
topic_consumer = client.get_topic_consumer(os.environ["input"], consumer_group = "empty-transformation")
```

!!! note

    If you don't specify a consumer group, then all messages in all streams in a topic will be processed by all replicas in the microservice deployment.

For further information read about how [Quix Streams works with Kafka](kafka.md).

## Stream data formats

There are two main formats of stream data: 

1. **Event data** - in Quix Streams this is represented with the `qx.EventData` class.
2. **Time-series** - in Quix Streams this is represented with the `qx.TimeseriesData` class (and two other classes: one for Pandas data frame format, and one for raw Kafka data).

Event data refers to data that is independent, whereas time-series data is a variable that changes over time. An example of event data is a financial transaction. It contains all data for the invoice, with a timestamp (the time of the transaction), but a financial transaction itself is not a variable you'd track over time. The invoice may itself contain time-series data though, such as the customer's account balance. 

Time-series data is a variable that is tracked over time, such as temperature from a sensor, or the g-forces in a racing car.

Time-series data has three different representations in Quix Streams, to serve different use cases and developers. The underlying data that these three models represent is the same however. The three representations of that data are:

1. Data (represented by the `qx.TimeseriesData` class)
2. Pandas Data Frame (represented by the `pd.DataFrame` class)
3. DataRaw (represented by the `qx.TimeseriesDataRaw` class)

In this topic you'll learn about the `TimeseriesData` and `pd.DataFrame` formats.

## Registering a callback for stream data

When it comes to registering your callbacks, the first step is to register a stream callback that is invoked when data is first received on a stream. 

```python
topic_consumer.on_stream_received = on_stream_received_handler
```

The `on_stream_received_handler` is typically written to handle a specific data format on that stream. This is explained in the next section.

!!! note

    This callback is invoked for each stream in a topic. This means you will have multiple instances of this callback invoked, if there are multiple streams. 

## Registering callbacks to handle data formats

Specific callbacks are registered to handle each type of stream data.

The following table documents which callbacks to register, depending on the type of stream data you need to handle:

| Stream data format | Callback to register |
|----|----|
| Event data | `stream_consumer.events.on_data_received = on_event_data_received_handler` |
| Time-series data | `stream_consumer.timeseries.on_data_received = on_data_received_handler` |
| Time-series raw data | `stream_consumer.timeseries.on_raw_received = on_raw_received_handler` |
| Time-series data frame | `stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler` |

!!! note

    You can have multiple callbacks registered at the same time, but usually you would work with the data format most suited to your use case. For example, if the source was providing only event data, it only makes sense to register the event data callback.

### Example of callback registration

The following code sample demonstrates how to register a callback to handle data in the data frame format: 

```python
def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

# subscribe to new streams being received. 
# callback will only be registered for an active stream
topic_consumer.on_stream_received = on_stream_received_handler
```

In this example, when a stream becomes active, it registers a callback to handle time-series data in the data frame format.

!!! note

    The callback is registered *only* for the specified stream, and only if that stream is active.

## Converting time-series data

Sometimes you need to convert time-series data into Panda data frames format for processing. That can be done using `to_dataframe`:

```python
df = ts.to_dataframe()
```

## "One message at a time" processing

Now that you have learned about stream data formats and callbacks, the following example shows a simple data processor. 

This processor receives (consumes) data, processes it (transforms), and then publishes generated data (produces) on an output topic. This encapsulates the typical processing pipeline which consists of:

1. Consumer (reads data)
2. Transformer (processes data)
3. Producer (writes data)

The example code demonstrates this:

```python
import quixstreams as qx
import pandas as pd

client = qx.KafkaStreamingClient('127.0.0.1:9092')

print("Opening consumer and producer topics")
topic_consumer = client.get_topic_consumer("quickstart-topic")
topic_producer = client.get_topic_producer("output-topic")

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print(df)
    # Calculate gForceTotal, the sum of vector absolute values 
    df["gForceTotal"] = df["gForceX"].abs() + df["gForceY"].abs() + df["gForceZ"].abs() 
    # write result data to output topic
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

In this example the stream data is inbound in Pandas `DataFrame` [format](https://pandas.pydata.org/docs/reference/frame.html){target=_blank}. 

Note that all information required to calculate `gForceTotal` is contained in the inbound data frame (the X, Y, and Z components of g-force). This is an example of "one message at a time" processing: no state needs to be preserved between messages. 

Further, if multiple replicas were used here, it would require no changes to your code, as each replica, running its own instance of the callback for the target stream, would simply calculate a value for `gForceTotal` based on the data in the data frame it received.

## Stateful processing

With stateful processing, additional complexity is introduced, as data now needs to be preserved between messages, streams, and potentially replicas (where multiple replicas are deployed to handle multiple streams). 

## The problem of using global variables to track state

There are problems with using global variables in your code to track state. The first is that callbacks are registered per-stream. This means that if you modify a global variable in a callback, it will be modified by all streams. 

For example, consider the following problematic code:

```python
...

gForceRunningTotal = 0.0

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    print(df)
    # Calculate gForceTotal, the sum of vector absolute values 
    df["gForceTotal"] = df["gForceX"].abs() + df["gForceY"].abs() + df["gForceZ"].abs() 
    
    # Track running total of all g-forces
    global gForceRunningTotal
    gForceRunningTotal += df["gForceTotal"]

    # write result data to output topic
    topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(df)
...
```

You might think this would give you the running total for a stream, but because the callback is registered for each stream, you'd actually get all streams modifying the global.

If you were running across multiple replicas, you'd get a running total for each replica, because each replica would have its own instance of the global variable. Again, the results would not be as you might expect.

Let's say there were three streams and two replicas, you'd get the running total of two streams for one replica, and the running total for the third stream in the other replica.

In most practical scenarios you'd want to track a running total per stream (say, total g-forces per race car), or perhaps for some variables a running total across all streams. Each of these scenarios is described in the followng sections.

## Tracking running totals per stream

Sometimes you might want to calculate a running total of a variable for a stream. For example, the total g-force a racing car is exposed to. If you use a global variable you'll lose the stream context. All streams will add to the value potentially, and each replica will also have its own instance of the global, further confusing matters.

The solution is to use the stream context to preserve a running total for that stream only. To do this you can use the `stream_id` of a stream to identify its data. Consider the following example:

```python
...
g_running_total_per_stream = {}

def callback_handler (stream_consumer: qx.StreamConsumer, data: qx.TimeseriesData):

    if stream_consumer.stream_id not in g_running_total_per_stream:
        g_running_total_per_stream[stream_consumer.stream_id] = 0
    
    ...

    g_running_total_per_stream[stream_consumer.stream_id] += some_value

...
```

The key point here is that data is tracked per stream context. You keep running totals on a per-stream basis by using the stream ID, `stream_consumer.stream_id` to index a dictionary containing running totals for each stream.

!!! note

    A stream will only ever be processed by one replica.

## Tracking running totals across multiple streams

Sometimes you want to track a running total across all streams in a topic. The problem is that when you scale using replicas, there is no way to share data between all replicas in a consumer group. 

The solution is to write the running total per stream (with stream ID) to an output topic. You can then have another processor in the pipeline to calculate total values from inbound messages.

```python
...
g_running_total_per_stream = {}

def callback_handler (stream_consumer: qx.StreamConsumer, data: qx.TimeseriesData):

    if stream_consumer.stream_id not in g_running_total_per_stream:
        g_running_total_per_stream[stream_consumer.stream_id] = 0
    
    ...

    g_running_total_per_stream[stream_consumer.stream_id] += some_value
    data.add_value("RunningTotal", g_running_total_per_stream[stream_consumer.stream_id])

    topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(data)
...
```

In this case the running total is published to its own stream in the output topic. The next service in the data processing pipeline would be able to sum all running totals across all streams in the output topic.

## Handling system restarts and crashes

One issue you may run into is that in-memory data is not persisted across instance restarts, shutdowns, and instance crashes. This can be mitigated by using the Quix Streams `LocalFileStorage` facility. This will ensure that specified variables are persisted on permanent storage, and this data is preserved across restarts, shutdowns, and system crashes.

The following example code demonstrates a simple use of `LocalFile Storage`:

```python
my_var = qx.InMemoryStorage(qx.LocalFileStorage())
...
topic_consumer.on_stream_received = on_stream_received_handler
topic_consumer.on_committed = my_var.flush
...
```

This ensures that the variable `my_var` is persisted, as periodically (default is 20 seconds) it is flushed to local file storage.

If the system crashes (or is restarted), Kafka resumes message processing from the last committed message. This facility is built into Kafka.

!!! tip

    For this facility to work in Quix Platform you need to enable the State Management feature. You can enable it in the `Deployment` dialog, where you can also specify the size of storage required. When using Quix Streams with a third-party broker such as Kafka, no configuration is required, and data is stored on the local file system.

## Conclusion

In this topic you have learned:

* How to perform simple "one message at a time" processing.
* How to handle the situation where state needs to be preserved, and problems that can arise in naive code.
* How to persist state, so that your data is preserved in the event of restarts or crashes.

## Next steps

Continue your Quix Streams learning journey by reading the following more in-depth topics:

* [Publishing data](publish.md)
* [Subscribing to data](subscribe.md)
* [Processing data](process.md)
* [State management](state-management.md)