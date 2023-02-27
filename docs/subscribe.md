# Subscribing to data

Quix Streams allows you to subscribe to data in real time from your topics. In this section, we explain more in-depth how to do it using the library.

## Connect to Quix

In order to start subscribing to data from your kafka topics, you need an instance of `KafkaStreamingClient`. This is the entry point where you begin subscribing to the topics. To create an instance, use the following code:

=== "Python"
	
	``` python
    from quixstreams import KafkaStreamingClient

	client = KafkaStreamingClient('127.0.0.1:9092')
	```

=== "C\#"
	
	``` cs
	var client = new Quix.Streams.Streaming.KafkaStreamingClient("127.0.0.1:9092");
	```

You can find other ways to connect to your message broker in the [Connect](connect.md) section.

## Create a topic consumer

Topics are at the center for stream processing operations. In order to subscribe to data in a topic you need an instance of `TopicConsumer`. This instance allow you to receive all the incoming streams on the specified topic. You can create an instance using the client’s `get_topic_consumer` method, passing the `TOPIC` as the parameter.

=== "Python"
    
    ``` python
    topic_consumer = client.get_topic_consumer(TOPIC)
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.GetTopicConsumer(TOPIC);
    ```

### Consumer group

The [Consumer group](kafka.md#consumer-group) is a concept used when you want to [scale horizontally](features/horizontal-scaling.md). Each consumer group is identified using an ID, which you set optionally when opening a connection to the topic for reading:

=== "Python"
    
    ``` python
    topic_consumer = client.get_topic_consumer("{topic}","{your-consumer-group-id}")
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.GetTopicConsumer("{topic}","{your-consumer-group-id}");
    ```

This is how the message broker knows that all the replicas of your process want to share the load of the incoming streams between replicas. Each replica will receive only a subset of the streams incoming to the Input Topic.

!!! warning

	If you want to consume data from the topic locally for debugging purposes, and the model is also deployed elsewhere, make sure that you change the consumer group ID to prevent clashing with the other deployment. If the clash happens, only one instance will be able to receive data for a partition at the same time.

## Subscribing to streams

=== "Python"  
    Once you have the `TopicConsumer` instance you can start receiving streams. For each stream received from the specified topic, `TopicConsumer` will execute the callback `on_stream_received`. This callback will be invoked every time you receive a new Stream. For example, the following code prints the StreamId for each stream received on that topic:
    
    ``` python
    from quixstreams import TopicConsumer, StreamConsumer

    def on_stream_received_handler(stream_received: StreamConsumer):
        print("Stream received:" + stream_received.stream_id)
    
    topic_consumer.on_stream_received = on_stream_received_handler
    topic_consumer.subscribe()
    ```

    !!! note
        `subscribe()` method indicates to moment to start consuming streams and data from your Topic. This should normally happen after you’ve registered callbacks for all the events you want to listen to.
        `App.run()` can also be used for this and provides other benefits. Find out more about [App.run()](app-management.md)


=== "C\#"
    Once you have the `TopicConsumer` instance you can start reading streams. For each stream received to the specified topic, `TopicConsumer` will execute the event `OnStreamReceived`. You can attach a callback to this event to execute code that reacts when you receive a new stream. For example the following code prints the StreamId for each stream received on that Topic:
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, newStream) =>
    {
        Console.WriteLine($"New stream read: {newStream.StreamId}");
    };
    
    topicConsumer.Subscribe();
    ```

    !!! tip

    	The `Subscribe` method indicates to moment to start consuming streams and data from your Topic. This should normally happen after you’ve registered callbacks for all the events you want to listen to.
        `App.run()` can also be used for this and provides other benefits. Find out more about [App.run()](app-management.md)

## Subscribing to time-series data

### TimeseriesData format

TimeseriesData is the formal class in Quix Streams which represents a time series data packet in memory. The format consists of a list of Timestamps with their corresponding parameter names and values for each timestamp.

You should imagine a TimeseriesData as a table where the Timestamp is the first column of that table and where the parameters are the columns for the values of that table. 

The following table shows an example:

| Timestamp | Speed | Gear |
| --------- | ----- | ---- |
| 1         | 120   | 3    |
| 2         | 123   | 3    |
| 3         | 125   | 3    |
| 6         | 110   | 2    |

=== "Python"
    You can subscribe to time-series data from streams using the `on_data_received` callback of the `StreamConsumer` instance. For instance, in the following example we read and print the first timestamp and value of the parameter `ParameterA` received in the [TimeseriesData](#timeseriesdata-format) packet:
    
    ``` python
    from quixstreams import TopicConsumer, StreamConsumer, TimeseriesData

    def on_stream_received_handler(stream_received: StreamConsumer):
        stream_received.on_data_received = on_timeseries_data_received_handler
    
    def on_timeseries_data_received_handler(stream: StreamConsumer, data: TimeseriesData):
        with data:
            timestamp = data.timestamps[0].timestamp
            num_value = data.timestamps[0].parameters['ParameterA'].numeric_value
            print("ParameterA - " + str(timestamp) + ": " + str(num_value))
    
    topic_consumer.on_stream_received = on_stream_received_handler
    topic_consumer.subscribe()
    ```

    !!! note
        `subscribe()` starts reading from the topic however, `App.run()` can also be used for this and provides other benefits.

        Find out more about [App.run()](app-management.md)

=== "C\#"
    You can subscribe to time-series data from streams using the `OnDataReceived` event of the `StreamConsumer` instance. For instance, in the following example we read and print the first timestamp and value of the parameter `ParameterA` received in the [TimeseriesData](#timeseriesdata-format) packet:
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, streamConsumer) =>
    {
        streamConsumer.Timeseries.OnDataReceived += (sender, args) =>
        {
            var timestamp = args.Data.Timestamps[0].Timestamp;
            var numValue = args.Data.Timestamps[0].Parameters["ParameterA"].NumericValue;
            Console.WriteLine($"ParameterA - {timestamp}: {numValue}");
        };
    };
    
    topicConsumer.Subscribe();
    ```

!!! note
    `subscribe()` starts reading from the topic however, `App.run()` can also be used for this and provides other benefits.

    Find out more about [App.run()](app-management.md)

Quix Streams supports numeric, string, and binary value types. You should use the proper property depending of the value type of your parameter:

=== "Python"
    
      - `numeric_value`: Returns the numeric value of the parameter, represented as a `float` type.    
      - `string_value`: Returns the string value of the parameter, represented as a `string` type.    
      - `binary_value`: Returns the binary value of the parameter, represented as a `bytearray` type.

=== "C\#"
    
      - `NumericValue`: Returns the numeric value of the parameter, represented as a `double` type.    
      - `StringValue`: Returns the string value of the parameter, represented as a `string` type.    
      - `BinaryValue`: Returns the binary value of the parameter, represented as an array of `byte`.

This is a simple example showing how to read `Speed` values of the `TimeseriesData` used in the previous example:

=== "Python"
    
    ``` python
    for ts in data.timestamps:
        timestamp = ts.timestamp_nanoseconds
        numValue = ts.parameters['Speed'].numeric_value
        print("Speed - " + str(timestamp) ": " + str(numValue))
    ```

=== "C\#"

    ``` cs
    foreach (var timestamp in data.Timestamps)
    {
           var timestamp = timestamp.TimestampNanoseconds;
           var numValue = timestamp.Parameters["Speed"].NumericValue;
           Console.WriteLine($"Speed - {timestamp}: {numValue}");
    }
    ```

output:

``` console
Speed - 1: 120
Speed - 2: 123
Speed - 3: 125
Speed - 6: 110
```

### pandas DataFrame format

If you use the Python version of Quix Streams you can use [pandas DataFrame](features/data-frames.md) for reading and writing time-series data. Use the callback `on_dataframe_received` instead of `on_data_received` when reading from a stream:

``` python
from quixstreams import TopicConsumer, StreamConsumer

def on_stream_received_handler(stream_received: StreamConsumer):
    stream_received.on_dataframe_received = on_dataframe_received_handler

def on_dataframe_received_handler(stream: StreamConsumer, df: pd.DataFrame):
    print(df.to_string())

topic_consumer.on_stream_received = on_stream_received_handler
topic_consumer.subscribe()
```
    
Alternatively, you can always convert a [TimeseriesData](#timeseriesdata-format) to a pandas DataFrame using the method `to_dataframe`:

``` python
from quixstreams import TopicConsumer, StreamConsumer, TimeseriesData

def on_stream_received_handler(stream_received: StreamConsumer):
    stream_received.on_data_received = on_timeseries_data_received_handler

def on_timeseries_data_received_handler(stream: StreamConsumer, data: TimeseriesData):
    with data:
        # read from input stream
        df = data.to_dataframe()
        print(df.to_string())

topic_consumer.on_stream_received = on_stream_received_handler
topic_consumer.subscribe()
```

!!! tip

	The conversions from [TimeseriesData](#timeseriesdata-format) to pandas DataFrame have an intrinsic cost overhead. For high-performance models using pandas DataFrame, you should use the `on_dataframe_received` callback provided by the library, which is optimized to do as few conversions as possible.

### Using a Buffer

Quix Streams provides you with an optional programmable buffer which you can tailor to your needs. Using buffers to read data allows you to process data in batches according to your needs. The buffer also helps you to develop models with a high-performance throughput.

=== "Python"
    You can use the `buffer` property embedded in the `timeseries` property of your `stream`, or create a separate instance of that buffer using the `create_buffer` method:

    ``` python
    buffer = newStream.timeseries.create_buffer()
    ```

=== "C\#"
    You can use the `Buffer` property embedded in the `Timeseries` property of your `stream`, or create a separate instance of that buffer using the `CreateBuffer` method:

    ``` cs
    var buffer = newStream.Timeseries.CreateBuffer();
    ```

You can configure a buffer’s input requirements using built-in properties. For example, the following configuration means that the Buffer will release a packet when the time span between first and last timestamp inside the buffer reaches 100 milliseconds:

=== "Python"
    
    ``` python
    buffer.time_span_in_milliseconds = 100
    ```

=== "C\#"
    
    ``` cs
    buffer.TimeSpanInMilliseconds = 100;
    ```

Reading data from that buffer is as simple as using its callback (Python) or events (C#). The buffer uses the same callbacks and events as when reading without buffer. For example, the following code prints the ParameterA value of the first timestamp of each packet released from the buffer:

=== "Python"
    
    ``` python
    from quixstreams import TopicConsumer, StreamConsumer, TimeseriesData

    def on_data_released_handler(stream: StreamConsumer, data: TimeseriesData):
        with data:
            timestamp = data.timestamps[0].timestamp
            num_value = data.timestamps[0].parameters['ParameterA'].numeric_value
            print("ParameterA - " + str(timestamp) + ": " + str(num_value))
    
    buffer.on_data_released = on_data_released_handler
    # buffer.on_dataframe_released and other callbacks are also available, check reading without buffer for more info
    ```

=== "C\#"
    
    ``` cs
    buffer.OnDataReleased += (sender, args) =>
    {
        var timestamp = ags.Data.Timestamps[0].Timestamp;
        var numValue = ags.Data.Timestamps[0].Parameters["ParameterA"].NumericValue;
        Console.WriteLine($"ParameterA - {timestamp}: {numValue}");
    };
    ```

You can configure multiple conditions to determine when the buffer has to release data, if any of these conditions become true, the buffer will release a new packet of data and that data is cleared from the buffer:

=== "Python"
    
      - `buffer.buffer_timeout`: The maximum duration in milliseconds for which the buffer will be held before releasing the data. A packet of data is released when the configured timeout value has elapsed from the last data received in the buffer.    
      - `buffer.packet_size`: The maximum packet size in terms of number of timestamps. Each time the buffer has this amount of timestamps, the packet of data is released.    
      - `buffer.time_span_in_nanoseconds`: The maximum time between timestamps in nanoseconds. When the difference between the earliest and latest buffered timestamp surpasses this number, the packet of data is released.    
      - `buffer.time_span_in_milliseconds`: The maximum time between timestamps in milliseconds. When the difference between the earliest and latest buffered timestamp surpasses this number, the packet of data is released. Note: This is a millisecond converter on top of `time_span_in_nanoseconds`. They both work with the same underlying value.    
      - `buffer.custom_trigger_before_enqueue`: A custom function which is invoked **before** adding a new timestamp to the buffer. If it returns true, the packet of data is released before adding the timestamp to it.    
      - `buffer.custom_trigger`: A custom function which is invoked **after** adding a new timestamp to the buffer. If it returns true, the packet of data is released with the entire buffer content.    
      - `buffer.filter`: A custom function to filter the incoming data before adding it to the buffer. If it returns true, data is added, otherwise it isn’t.

=== "C\#"
    
      - `Buffer.BufferTimeout`: The maximum duration in milliseconds for which the buffer will be held before releasing the data. A packet of data is released when the configured timeout value has elapsed from the last data received in the buffer.    
      - `Buffer.PacketSize`: The maximum packet size in terms of number of timestamps. Each time the buffer has this amount of timestamps, the packet of data is released.    
      - `Buffer.TimeSpanInNanoseconds`: The maximum time between timestamps in nanoseconds. When the difference between the earliest and latest buffered timestamp surpasses this number, the packet of data is released.    
      - `Buffer.TimeSpanInMilliseconds`: The maximum time between timestamps in milliseconds. When the difference between the earliest and latest buffered timestamp surpasses this number, the packet of data is released. Note: This is a millisecond converter on top of `TimeSpanInNanoseconds`. They both work with the same underlying value.    
      - `Buffer.CustomTriggerBeforeEnqueue`: A custom function which is invoked **before** adding a new timestamp to the buffer. If it returns true, the packet of data is released before adding the timestamp to it.    
      - `Buffer.CustomTrigger`: A custom function which is invoked **after** adding a new timestamp to the buffer. If it returns true, the packet of data is released with the entire buffer content.    
      - `Buffer.Filter`: A custom function to filter the incoming data before adding it to the buffer. If it returns true, data is added, otherwise it isn’t.

#### Examples

The following buffer configuration will send data every 100ms or, if no data is buffered in the 1 second timeout period, it will empty the buffer and send the pending data anyway:

=== "Python"
    
    ``` python
    stream.timeseries.buffer.packet_size = 100
    stream.timeseries.buffer.buffer_timeout = 1000
    ```

=== "C\#"
    
    ``` cs
    stream.Timeseries.Buffer.PacketSize = 100;
    stream.Timeseries.Buffer.BufferTimeout = 1000;
    ```

The following buffer configuration will send data every 100ms window or if critical data arrives:

=== "Python"
    
    ``` python
    buffer.time_span_in_milliseconds = 100
    buffer.custom_trigger = lambda data: data.timestamps[0].tags["is_critical"] == 'True'
    ```

=== "C\#"
    
    ``` cs
    stream.Timeseries.Buffer.TimeSpanInMilliseconds = 100;
    stream.Timeseries.Buffer.CustomTrigger = data => data.Timestamps[0].Tags["is_critical"] == "True";
    ```
    
## Subscribing to events

`EventData` is the formal class in Quix Streams which represents an Event data packet in memory. `EventData` is meant to be used when the data is intended to be consumed only as single unit, such as json payload where properties can't be converted to individual parameters. EventData can also be better for non-standard changes such as a machine shutting down sending event named ShutDown.

!!! tip

	If your data source generates data at regular time intervals, or the information can be organized in a fixed list of Parameters, the [TimeseriesData](#timeseriesdata-format) format is a better fit for your time-series data.

### EventData format

`EventData` consists of a record with a `Timestamp`, an `EventId` and an `EventValue`.

You should imagine a list of `EventData` instances as a simple table of three columns where the `Timestamp` is the first column of that table and the `EventId` and `EventValue` are the second and third columns, as shown in the following table:

| Timestamp | EventId     | EventValue                 |
| --------- | ----------- | -------------------------- |
| 1         | failure23   | Gearbox has a failure      |
| 2         | box-event2  | Car has entered to the box |
| 3         | motor-off   | Motor has stopped          |
| 6         | race-event3 | Race has finished          |

Reading events from a stream is as easy as reading timeseries data. In this case, the library does not use a buffer, but the way we read Event Data from a stream is similar.

=== "Python"
    from quixstreams import TopicConsumer, StreamConsumer, EventData

    ``` python
    def on_event_data_received_handler(stream: StreamConsumer, data: EventData):
        with data:
            print("Event read for stream. Event Id: " + data.Id)
    
    stream_received.events.on_data_received = on_event_data_received_handler
    ```

=== "C\#"
    
    ``` cs
    newStream.Events.OnDataReceived += (stream, args) =>
    {
        Console.WriteLine($"Event received for stream. Event Id: {args.Data.Id}");
    };
    ```

output:

``` console
Event read for stream. Event Id: failure23
Event read for stream. Event Id: box-event2
Event read for stream. Event Id: motor-off
Event read for stream. Event Id: race-event3
```

## Committing / checkpointing

It is important to be aware of the commit concept when working with a broker. Committing allows you to mark how far data has been processed, also known as creating a [checkpoint](kafka.md#checkpointing). In the event of a restart or rebalance, the client only processes messages from the last committed position. Commits are done for each consumer group, so if you have several consumer groups in use, they do not affect each another when committing.

!!! tip

	Commits are done at a partition level when you use Kafka as a message broker. Streams that belong to the same partition are committed at the same time using the same position. Quix Streams currently does not expose the option to subscribe to only specific partitions of a topic, but commits will only ever affect partitions that are currently assigned to your client.

	Partitions and the Kafka rebalancing protocol are internal details of the Kafka implementation of Quix Streams. You can read more about the Streaming Context feature of the library [here](features/streaming-context.md).

### Automatic committing

By default, Quix Streams automatically commits processed messages at a regular default interval, which is every 5 seconds or 5,000 messages, whichever happens sooner. However this is subject to change.

If you wish to use different automatic commit intervals, use the following code:

=== "Python"
    
    ``` python
    from quixstreams import CommitOptions
    
    commit_settings = CommitOptions()
    commit_settings.commit_every = 100 # note, you can set this to None
    commit_settings.commit_interval = 500 # note, you can set this to None
    commit_settings.auto_commit_enabled = True
    topic_consumer = client.get_topic_consumer('yourtopic', commit_settings=commit_settings)
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.GetTopicConsumer(topic, consumerGroup, new CommitOptions()
    {
            CommitEvery = 100,
            CommitInterval = 500,
            AutoCommitEnabled = true // optional, defaults to true
    });
    ```

The code above will commit every 100 processed messages or 500 ms, whichever is sooner.

### Manual committing

Some use cases need manual committing to mark completion of work, for example when you wish to batch process data, so the frequency of commit depends on the data. This can be achieved by first enabling manual commit for the topic:

=== "Python"
    
    ``` python
    from quixstreams import CommitMode
    
    topic_consumer = client.get_topic_consumer('yourtopic', commit_settings=CommitMode.Manual)
    ```

=== "C\#"
    
    ``` cs
    client.GetTopicConsumer(topic, consumerGroup, CommitMode.Manual);
    ```

Then, whenever your commit condition fulfils, call:

=== "Python"
    
    ``` python
    topic_consumer.commit()
    ```

=== "C\#"
    
    ``` cs
    topicConsumer.Commit();
    ```

The piece of code above will commit anything – like parameter, event or metadata - read and served to you from the topic you subscribed to up to this point.

### Commit callback

=== "Python"
Whenever a commit occurs, a callback is raised to let you know. This callback is invoked for both manual and automatic commits. You can set the callback using the following code:
    
    ``` python
    from quixstreams import TopicConsumer

    def on_committed_handler(topic_consumer: TopicConsumer):
        # your code doing something when committed to broker
    
    topic_consumer.on_committed = on_committed_handler
    ```

=== "C\#"
Whenever a commit occurs, an event is raised to let you know. This event is raised for both manual and automatic commits. You can subscribe to this event using the following code:
    
    ``` cs
    topicConsumer.OnCommitted += (sender, args) =>
    {
        //... your code …
    };
    ```

### Auto offset reset

You can control the offset that data is read from by optionally specifying `AutoOffsetReset` when you open the topic.

When setting the `AutoOffsetReset` you can specify one of three options:

| Option   | Description                                                      |
| -------- | ---------------------------------------------------------------- |
| Latest   | Read only the latest data as it arrives, dont include older data |
| Earliest | Read from the beginning, i.e. as much as possible                |
| Error    | Throws exception if no previous offset is found                  |

By default, Latest is used.

=== "Python"
    
    ``` python
    topic_consumer = client.get_topic_consumer(test_topic, auto_offset_reset=AutoOffsetReset.Latest)
    or
    topic_consumer = client.get_topic_consumer(test_topic, auto_offset_reset=AutoOffsetReset.Earliest)
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.GetTopicConsumer("MyTopic", autoOffset: AutoOffsetReset.Latest);
    or
    var topicConsumer = client.GetTopicConsumer("MyTopic", autoOffset: AutoOffsetReset.Earliest);
    ```

## Revocation

When working with a broker, you have a certain number of topic streams assigned to your consumer. Over the course of the client’s lifetime, there may be several events causing a stream to be revoked, like another client joining or leaving the consumer group, so your application should be prepared to handle these scenarios in order to avoid data loss and/or avoidable reprocessing of messages.

!!! tip

	Kafka revokes entire partitions, but Quix Streams makes it easy to determine which streams are affected by providing two events you can listen to.

	Partitions and the Kafka rebalancing protocol are internal details of the Kafka implementation of Quix Streams. You mainly don’t even need to worry about it because everything is abstracted within the [Streaming Context](features/streaming-context.md) feature of the library.

### Streams revoking

One or more streams are about to be revoked from your client, but you have a limited time frame – according to your broker configuration – to react to this and optionally commit to the broker:

=== "Python"
    
    ``` python
    def on_revoking_handler(topic_consumer: TopicConsumer):
        # your code
    
    topic_consumer.on_revoking = on_revoking_handler
    ```

=== "C\#"
    
    ``` cs
    topicConsumer.OnRevoking += (sender, args) =>
        {
            // ... your code ...
        };
    ```

### Streams revoked

One or more streams are revoked from your client. You can no longer commit to these streams, you can only handle the revocation in your client.

=== "Python"
    
    ``` python
    from quixstreams import StreamConsumer
    
    def on_streams_revoked_handler(topic_consumer: TopicConsumer, streams: [StreamConsumer]):
        for stream in streams:
            print("Stream " + stream.stream_id + " got revoked")
    
    topic_consumer.on_streams_revoked = on_streams_revoked_handler
    ```

=== "C\#"
    
    ``` cs
    topicConsumer.OnStreamsRevoked += (sender, revokedStreams) =>
        {
            // revoked streams are provided to the handler
        };
    ```

## Stream closure

=== "Python"
You can detect stream closure with the `on_stream_closed` callback which has the stream and the StreamEndType to help determine the closure reason if required.
    
    ``` python
    def on_stream_closed_handler(stream: StreamConsumer, end_type: StreamEndType):
            print("Stream closed with {}".format(end_type))
    
    stream_received.on_stream_closed = on_stream_closed_handler
    ```

=== "C\#"
You can detect stream closure with the stream closed event which has the sender and the StreamEndType to help determine the closure reason if required.
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, streamConsumer) =>
    {
            streamConsumer.OnStreamClosed += (reader, args) =>
            {
                    Console.WriteLine("Stream closed with {0}", args.EndType);
            };
    };
    ```

The `StreamEndType` can be one of:

| StreamEndType | Description                                                         |
| ------------- | ------------------------------------------------------------------- |
| Closed        | The stream was closed normally                                      |
| Aborted       | The stream was aborted by your code for your own reasons            |
| Terminated    | The stream was terminated unexpectedly while data was being written |

## Minimal example

This is a minimal code example you can use to read data from a topic using Quix Streams:

=== "Python"
    
    ``` python
    from quixstreams import *
    
    client = KafkaStreamingClient('127.0.0.1:9092')
    
    topic_consumer = client.get_topic_consumer(TOPIC_ID)
    
    # read streams
    def on_stream_received_handler(stream_received: StreamConsumer):    
        buffer = stream_received.timeseries.create_buffer()
        buffer.on_data_released = on_data_released_handler
    
    def on_data_released_handler(stream: StreamConsumer, data: TimeseriesData):
        with data:
            df = data.to_dataframe()
            print(df.to_string())    
    
    # Hook up events before initiating read to avoid losing out on any data
    topic_consumer.on_stream_received = on_stream_received_handler
    
    # Hook up to termination signal (for docker image) and CTRL-C
    print("Listening to streams. Press CTRL-C to exit.")
    
    # Handle graceful exit
    App.run()
    ```

    Find out more about [App.run()](app-management.md)

=== "C\#"
    
    ``` cs
    using System;
    using System.Linq;
    using System.Threading;
    using Quix.Streams.Streaming;
    using Quix.Streams.Streaming.Configuration;
    using Quix.Streams.Streaming.Models;
    
    
    namespace ReadHelloWorld
    {
        class Program
        {
            /// <summary>
            /// Main will be invoked when you run the application
            /// </summary>
            static void Main()
            {
                // Create a client which holds generic details for creating input and output topics
                var client = new KafkaStreamingClient("127.0.0.1:9092")
    
                using var topicConsumer = client.GetTopicConsumer(TOPIC_ID);
    
                // Hook up events before initiating read to avoid losing out on any data
                topicConsumer.OnStreamReceived += (topic, streamConsumer) =>
                {
                    Console.WriteLine($"New stream read: {streamConsumer.StreamId}");
    
                    var buffer = streamConsumer.Timeseries.CreateBuffer();
    
                    buffer.OnDataReleased += (sender, args) =>
                    {
                        Console.WriteLine($"ParameterA - {ags.Data.Timestamps[0].Timestamp}: {ags.Data.Timestamps.Average(a => a.Parameters["ParameterA"].NumericValue)}");
                    };
                };
    
                Console.WriteLine("Listening for streams");
    
                // Hook up to termination signal (for docker image) and CTRL-C and open streams
                App.Run();
    
                Console.WriteLine("Exiting");
            }
        }
    }
    ```

## Read raw Kafka messages

Quix Streams uses an internal protocol which is both data and speed optimized so we do encourage you to use it, but you need to use Quix Streams on both producer and consumer sides as of today. We have plans to support most common formats in near future, but custom formats will always need to be handled manually.

For this, we have created a way to [publish](publish.md#write-raw-kafka-messages) and [subscribe](subscribe.md#read-raw-kafka-messages) to the raw, unformatted messages and work with them as bytes. This gives you the ability to implement the protocol as needed and convert between formats.

=== "Python"
    
    ``` python
    from quixstreams import RawTopicConsumer, RawMessage

    raw_consumer = client.create_raw_topic_consumer(TOPIC_ID)
    
    def on_message_received_handler(topic: RawTopicConsumer, msg: RawMessage):
        #bytearray containing bytes received from kafka
        data = msg.value
    
        #broker metadata as dict
        meta = msg.metadata
    
    raw_consumer.on_message_received = on_message_received_handler
    raw_consumer.subscribe()  # or use App.run()
    ```

=== "C\#"
    
    ``` cs
    var rawConsumer = client.CreateRawTopicConsumer(TOPIC_ID)
    
    rawConsumer.OnMessageRead += (sender, message) =>
    {
        var data = (byte[])message.Value;
    };
    
    rawConsumer.Subscribe()  // or use App.Run()
    ```
