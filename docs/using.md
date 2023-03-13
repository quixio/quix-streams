# Using Quix Streams

In this topic you will learn how to use Quix Streams to perform two types of data processing:

1. **One message at a time processing** - Here the message received contains all required data. No state needs to be preserved between messages, or between replicas. The data from the message is used to calculate a new value, which is then typically written to the output stream.
2. **Stateful processing** - This is where you need to keep track of data between messages, such as keeping a running total of a variable. This is more complicated as state needs to be preserved between messages, and potentially between replicas where multiple replicas are deployed. In addition, state needs to be preserved in the event of the failure of a deployment - Quix Streams supports checkpointing as a way to enable this.

The following sections will explore these methods of data processing in more detail.

## Topics, streams, partitions, replicas, and consumer groups

The main structure used for data organization in Quix is the topic. For example, the topic might be `iot-telemetry`. 


Stream context: (also add link to stream context topic)
You may have multiple devices, or sources, writing data into a topic, so to ensure scaling and message ordering, each source writes into its own stream. Device 1 would write to stream 1, and device 2 to stream 2 and so on.

Quix Streams restricts all messages inside one stream to the same single partition. This means that inside one stream, a consumer can rely on the order of messages. A partition can contain multiple streams, but a stream is always confined to one partition.

TBD: add info on replicas and consumer groups Necessary for horizontal scaling.

Horizontal scaling occurs automatically, because when you deploy multiple replicas, a stream is assigned to a replica. For example, if there are three streams and three replicas, each replica will process a single stream. If you had only one replica, it would need to process all streams in that topic.

## Stream data formats

There are two main formats of stream data: event data and time-series data: 

1. Event data, which in Quix Streams is represented with the `qx.EventData` class.
2. Time-series, which in Quix Streams is represented with the `qx.TimeseriesData` class (and two other classes: one for Pandas data frame format, and one for raw Kafka data).

Event data refers to data that is independent, whereas time-series data is a variable that changes over time. An example of event data is a financial transaction. It contains all data for the invoice, with a timestamp (the time of the transaction), but a financial transaction itself is not a variable you'd track over time. The invoice may itself contain time-series data though, such as the customer's account balance. 

Time-series data is a variable that is tracked over time, such as temperature from a sensor, or the g-forces in a racing car.

Time-series data has three formats in Quix Streams:

1. Data (`qx.TimeseriesData`)
2. Pandas Data Frame (`pd.DataFrame`)
3. DataRaw (`qx.TimeseriesDataRaw`)

In this topic you'll learn about the `TimeseriesData` and `pd.DataFrame` formats.

## Registering a callback for stream data

You can register a stream callback that is invoked when data is first received on a stream. 

```python
topic_consumer.on_stream_received = on_stream_received_handler
```

The `on_stream_received_handler` is typically written to handle a specific data format on that stream. This is explained in the next section.

!!! note

    This callback is invoked for each stream in a topic. This means you will have multiple instances of this callback running, if there are multiple streams. 
    
    If there are multiple replicas, and each replica is handling one stream, then each replica will run an instance of this callback. This has significance for how you are going to handle shared state, which is discussed later in this topic.

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

Now that you have learned about stream data formats and callbacks, the following example shows a simple data processor. This processor receives (consumes) data, processes it (transforms), and then publishes generated data (produces) on an output topic. This encapsulates the typical processing pipeline which consists of:

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

In this example the stream data is inbound in Pandas `DataFrame` [format](https://pandas.pydata.org/docs/reference/frame.html). 

Note that all information required to calculate `gForceTotal` is contained in the inbound data frame (the X, Y, and Z components of g-force). This is an example of "one message at a time" processing: no state needs to be preserved between messages. 

Further, if multiple replicas were used here, it would require no changes to your code, as each replica, running its own instance of the callback for the target stream, would simply calculate a value for `gForceTotal` based on the data in the data frame it received.

## Stateful processing

With stateful processing, complexity is introduced as data now needs to be preserved between messages, streams, and potentially replicas (where multiple replicas are deployed to handle multiple streams). 

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

In most practical scenarios you'd want to track a running total per stream (say, g-forces per race car), or perhaps for some variables a running total across all streams. Each of these scenarios is described in the followng sections.

## Tracking running totals per-stream

Running total for a stream. 

Problem - if you use a global variable you'll lose stream context. All streams will add to the value potentially, and each replica will also have its own 

Solution - use stream context/dictionary.

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

## Tracking running totals across multiple streams

Sometimes you want to track a running total across all streams in a topic.

This is not possible as when you scale using replicas, each replica will have its own copy of all variables. 

The solution is to write the running total per stream (with stream ID) to an output topic. You can then have another processor to total values from inbound messages.

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

## Handling system restarts and crashes using checkpointing

TBD

## Conclusion

TBD

## Next steps

TBD

