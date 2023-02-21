# Reading data

The Quix SDK allows you to read data in real time from the existing streams of your Topics.

All the necessary code to read data from your Quix Workspace is auto-generated when you create a project using the existing templates. In this section, we explain more in-depth how to read data using the Quix SDK.

!!! tip

	The [Quix Portal](https://portal.platform.quix.ai){target=_blank} offers you easy-to-use, auto-generated examples for reading, writing, and processing data. These examples work directly with your workspace Topics. You can deploy these examples in our serverless environment with just a few clicks. For a quick test of the capabilities of the SDK, we recommend starting with those auto-generated examples.

## Connect to Quix

In order to start reading data from Quix you need an instance of the Quix client, `QuixStreamingClient`. This is the central point where you interact with the main SDK operations.

You can create an instance of `QuixStreamingClient` using the proper constructor of the SDK, as shown here:

=== "Python"
    
    ``` python
    client = QuixStreamingClient()
    ```

=== "C\#"
    
    ``` cs
    var client = new Quix.Sdk.Streaming.QuixStreamingClient();
    ```

You can find more advanced information on how to connect to Quix in the [Connect to Quix](/sdk/connect) section.

## Open a topic for reading

Topics are the default environment for input/output operations on Quix.

In order to access that topic for reading you need an instance of `TopicConsumer`. This instance allow you to read all the incoming streams on the specified Topic. You can create an instance of `TopicConsumer` using the client’s `create_topic_consumer` method, passing the `TOPIC_ID` or the `TOPIC_NAME` as a parameter.

=== "Python"
    
    ``` python
    topic_consumer = client.create_topic_consumer(TOPIC_ID)
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.CreateTopicConsumer(TOPIC_ID);
    ```

### Consumer group

The **Consumer group** is a concept used when you want to [scale horizontally](/sdk/features/horizontal-scaling). Each consumer group is identified using an ID, which you set optionally when opening a connection to the topic for reading:

=== "Python"
    
    ``` python
    topic_consumer = client.create_topic_consumer("{topic}","{your-consumer-group-id}")
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.CreateTopicConsumer("{topic}","{your-consumer-group-id}");
    ```

When you want to enable [horizontal scalability](/sdk/features/horizontal-scaling), all the replicas of your process should use the same `ConsumerId`. This is how the message broker knows that all the replicas of your process want to share the load of the incoming streams between replicas. Each replica will receive only a subset of the streams incoming to the Input Topic.

!!! warning

	If you want to consume data from the topic locally for debugging purposes, and the model is also deployed in the Quix serverless environment, make sure that you change the consumer group ID to prevent clashing with the cloud deployment. If the clash happens, only one instance will be able to read data of the Stream at a time, and you will probably notice that your code is not receiving data at some point, either locally or in the cloud environment.

## Reading streams

=== "Python"  
    Once you have the `TopicConsumer` instance you can start reading streams. For each stream received to the specified topic, `TopicConsumer` will execute the callback `on_stream_received`. This callback will be invoked every time you receive a new Stream. For example, the following code prints the StreamId for each `newStream` received on that Topic:
    
    ``` python
    def read_stream(topic_consumer: TopicConsumer, new_stream: StreamConsumer):
        print("New stream read:" + new_stream.stream_id)
    
    topic_consumer.on_stream_received = read_stream
    topic_consumer.subscribe()
    ```

    !!! note
        `subscribe()` starts reading from the topic however, `App.run()` can also be used for this and provides other benefits.

        Find out more about [App.run()](app-management.md)


=== "C\#"
    Once you have the `TopicConsumer` instance you can start reading streams. For each stream received to the specified topic, `TopicConsumer` will execute the event `OnStreamReceived`. You can attach a callback to this event to execute code that reacts when you receive a new Stream. For example the following code prints the StreamId for each `newStream` received on that Topic:
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, newStream) =>
    {
        Console.WriteLine($"New stream read: {newStream.StreamId}");
    };
    
    topicConsumer.Subscribe();
    ```

!!! tip

	The `Subscribe` method indicates to the SDK the moment to start reading streams and data from your Topic. This should normally happen after you’ve registered callbacks for all the events you want to listen to.

## Reading time-series data

You can read real-time data from Streams using the `on_read` event of the `StreamConsumer` instance received in the previous callback when you receive a new stream in your Topic.

For instance, in the following example we read and print the first timestamp and value of the parameter `ParameterA` received in the [TimeseriesData](#timeseriesdata-format) packet:

=== "Python"
    
    ``` python
    def on_stream_received_handler(topic_consumer: TopicConsumer, new_stream: StreamConsumer):
        new_stream.on_read = on_parameter_data_handler
    
    def on_parameter_data_handler(topic_consumer: TopicConsumer, stream: StreamConsumer, data: TimeseriesData):
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
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, streamConsumer) =>
    {
        streamConsumer.Parameters.OnRead += (sender, args) =>
        {
            var timestamp = args.Data.Timestamps[0].Timestamp;
            var numValue = args.Data.Timestamps[0].Parameters["ParameterA"].NumericValue;
            Console.WriteLine($"ParameterA - {timestamp}: {numValue}");
        };
    };
    
    topicConsumer.Subscribe();
    ```

We use [TimeseriesData](#timeseriesdata-format) packages to read data from the stream. This class handles reading and writing of time series data. The Quix SDK provides multiple helpers for reading and writing data using [TimeseriesData](#timeseriesdata-format).

!!! tip

	If you’re using Python you can convert [TimeseriesData](#timeseriesdata-format) to a [Pandas DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe){target=_blank} or read them directly from the SDK. Refer to [Using Data Frames](#using-data-frames){target=_blank} for more information.

### TimeseriesData format

[TimeseriesData](#timeseriesdata-format) is the formal class in the SDK which represents a time series data packet in memory.

[TimeseriesData](#timeseriesdata-format) consists of a list of Timestamps with their corresponding Parameter Names and Values for each timestamp.

You should imagine a Timeseries Data as a table where the Timestamp is the first column of that table and where the Parameters are the columns for the Values of that table.

The following table shows an example of Timeseries Data:

| Timestamp | Speed | Gear |
| --------- | ----- | ---- |
| 1         | 120   | 3    |
| 2         | 123   | 3    |
| 3         | 125   | 3    |
| 6         | 110   | 2    |

You can use the `timestamps` property of a `TimeseriesData` instance to access each row of that table, and the `parameters` property to access the values of that timestamp.

The Quix SDK supports Numeric, String, and Binary values and you should use the proper property depending of the value type of your Parameter:

=== "Python"
    
      - `numeric_value`: Returns the Numeric value of the Parameter, represented as a `float` type.
    
      - `string_value`: Returns the String value of the Parameter, represented as a `string` type.
    
      - `binary_value`: Returns the Binary value of the Parameter, represented as a `bytearray` type.

=== "C\#"
    
      - `NumericValue`: Returns the Numeric value of the Parameter, represented as a `double` type.
    
      - `StringValue`: Returns the String value of the Parameter, represented as a `string` type.
    
      - `BinaryValue`: Returns the Binary value of the Parameter, represented as an array of `byte`.

This is a simple example showing how to read Speed values of the `TimeseriesData` used in the previous example:

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

### Buffer

The Quix SDK provides you with a programmable buffer which you can tailor to your needs. Using buffers to read data enhances the throughput of your application. This helps you to develop Models with a high-performance throughput.

You can use the `buffer` property embedded in the `Parameters` property of your `stream`, or create a separate instance of that buffer using the `create_buffer` method:

=== "Python"
    
    ``` python
    buffer = newStream.parameters.create_buffer()
    ```

=== "C\#"
    
    ``` cs
    var buffer = newStream.Parameters.CreateBuffer();
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

Reading data from that buffer is as simple as using its `OnRead` event. For each [TimeseriesData](#timeseriesdata-format) packet released from the buffer, the SDK will execute the `OnRead` event with the timeseries data as a given parameter. For example, the following code prints the ParameterA value of the first timestamp of each packet released from the buffer:

=== "Python"
    
    ``` python
    def on_parameter_data_handler(topic: TopicConsumer, stream: StreamConsumer, data: TimeseriesData):
        with data:
            timestamp = data.timestamps[0].timestamp
            num_value = data.timestamps[0].parameters['ParameterA'].numeric_value
            print("ParameterA - " + str(timestamp) + ": " + str(num_value))
    
    buffer.on_read = on_parameter_data_handler
    ```

=== "C\#"
    
    ``` cs
    buffer.OnRead += (sender, args) =>
    {
        var timestamp = ags.Data.Timestamps[0].Timestamp;
        var numValue = ags.Data.Timestamps[0].Parameters["ParameterA"].NumericValue;
        Console.WriteLine($"ParameterA - {timestamp}: {numValue}");
    };
    ```

You can configure multiple conditions to determine when the Buffer has to release data, if any of these conditions become true, the buffer will release a new packet of data and that data is cleared from the buffer:

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
    
      - `Buffer.TimeSpanInMilliseconds`: The maximum time between timestamps in milliseconds. When the difference between the earliest and latest buffered timestamp surpasses this number, the packet of data is released. Note: This is a millisecond converter on top of `time_span_in_nanoseconds`. They both work with the same underlying value.
    
      - `Buffer.CustomTriggerBeforeEnqueue`: A custom function which is invoked **before** adding a new timestamp to the buffer. If it returns true, the packet of data is released before adding the timestamp to it.
    
      - `Buffer.CustomTrigger`: A custom function which is invoked **after** adding a new timestamp to the buffer. If it returns true, the packet of data is released with the entire buffer content.
    
      - `Buffer.Filter`: A custom function to filter the incoming data before adding it to the buffer. If it returns true, data is added, otherwise it isn’t.

#### Examples

The following buffer configuration will send data every 100ms or, if no data is buffered in the 1 second timeout period, it will empty the buffer and send the pending data anyway:

=== "Python"
    
    ``` python
    stream.parameters.buffer.packet_size = 100
    stream.parameters.buffer.buffer_timeout = 1000
    ```

=== "C\#"
    
    ``` cs
    stream.Parameters.Buffer.PacketSize = 100;
    stream.Parameters.Buffer.BufferTimeout = 1000;
    ```

The following buffer configuration will send data every 100ms window or if critical data arrives:

=== "Python"
    
    ``` python
    buffer.time_span_in_milliseconds = 100
    buffer.custom_trigger = lambda data: data.timestamps[0].tags["is_critical"] == 'True'
    ```

=== "C\#"
    
    ``` cs
    stream.Parameters.Buffer.TimeSpanInMilliseconds = 100;
    stream.Parameters.Buffer.CustomTrigger = data => data.Timestamps[0].Tags
    ["is_critical"] == "True";
    ```

### Using Data Frames

If you use the Python version of the SDK you can use [Pandas DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe){target=_blank} for reading and writing `TimeseriesData` to Quix. 

The Pandas DataFrames format is just a representation of [TimeseriesData](#timeseriesdata-format) format, where the Timestamp is mapped to a column named `time` and the rest of the parameters are mapped as columns named as the ParameterId of the parameter. Tags are mapped as columns with the prefix `TAG__` and the TagId of the tag.

For example, the following [TimeseriesData](#timeseriesdata-format):

| Timestamp | CarId (tag) | Speed | Gear |
| --------- | ----------- | ----- | ---- |
| 1         | car-1       | 120   | 3    |
| 2         | car-2       | 123   | 3    |
| 3         | car-1       | 125   | 3    |
| 6         | car-2       | 110   | 2    |

Is represented as the following Pandas DataFrame:

| time | TAG\_\_CarId | Speed | Gear |
| ---- | ------------ | ----- | ---- |
| 1    | car-1        | 120   | 3    |
| 2    | car-2        | 123   | 3    |
| 3    | car-1        | 125   | 3    |
| 6    | car-2        | 110   | 2    |

One simple way to read data from Quix using [Pandas DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe){target=_blank} is using the event `on_read_dataframe` instead of the common callback `on_read` when reading from a `stream`, or when reading data from a buffer:

``` python
def read_stream(topic_consumer: TopicConsumer, new_stream: StreamConsumer):

    buffer = new_stream.parameters.create_buffer()
    buffer.on_read_dataframe = on_dataframe_handler

def on_dataframe_handler(topic_consumer: TopicConsumer, stream: StreamConsumer, df: pd.DataFrame):
    print(df.to_string())

topic_consumer.on_stream_received = read_stream
topic_consumer.subscribe()
```
    
Alternatively, you can always convert a [TimeseriesData](#timeseriesdata-format) to a Pandas DataFrame using the method `to_panda_dataframe`:

``` python
def read_stream(topic_consumer: TopicConsumer, new_stream: StreamConsumer):

    buffer = new_stream.parameters.create_buffer()
    buffer.on_read = on_parameter_data_handler

def on_parameter_data_handler(topic_consumer: TopicConsumer, stream: StreamConsumer, data: TimeseriesData):
    with data:
        # read from input stream
        df = data.to_panda_dataframe()
        print(df.to_string())

topic_consumer.on_stream_received = read_stream
topic_consumer.subscribe()
```

!!! tip

	The conversions from [TimeseriesData](#timeseriesdata-format) to Pandas DataFrames have an intrinsic cost overhead. For high-performance models using Pandas DataFrames, you should use the `on_read_dataframe` callback provided by the SDK, which is optimized for doing as few conversions as possible.

!!! note
    `subscribe()` starts reading from the topic however, `App.run()` can also be used for this and provides other benefits.

    Find out more about [App.run()](app-management.md)
    
## Reading events

`EventData` is the formal class in the SDK which represents an Event data packet in memory. `EventData` is meant to be used for time-series data coming from sources that generate data at irregular intervals or without a defined structure.

### EventData format

`EventData` consists of a record with a `Timestamp`, an `EventId` and an `EventValue`.

You should imagine a list of `EventData` instances as a simple table of three columns where the `Timestamp` is the first column of that table and the `EventId` and `EventValue` are the second and third columns, as shown in the following table:

| Timestamp | EventId     | EventValue                 |
| --------- | ----------- | -------------------------- |
| 1         | failure23   | Gearbox has a failure      |
| 2         | box-event2  | Car has entered to the box |
| 3         | motor-off   | Motor has stopped          |
| 6         | race-event3 | Race has finished          |

Reading events from a stream is as easy as reading timeseries data. In this case, the SDK does not use a Buffer because we don’t need high-performance throughput, but the way we read Event Data from a `Stream` is identical.

=== "Python"
    
    ``` python
    def on_event_data_handler(topic_consumer: TopicConsumer, stream: StreamConsumer, data: EventData):
        with data:
            print("Event read for stream. Event Id: " + data.Id)
    
    new_stream.events.on_read = on_event_data_handler
    ```

=== "C\#"
    
    ``` cs
    newStream.Events.OnRead += (stream, args) =>
    {
        Console.WriteLine($"Event read for stream. Event Id: {args.Data.Id}");
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

It is important to be aware of the commit concept when working with a broker. Committing allows you to mark how far data has been processed, also known as creating a checkpoint. In the event of a restart or rebalance, the client only processes messages from the last commit position. In Kafka this is equivalent to commits for a [consumer group](/sdk/read/#consumer-groups).

Commits are done for each consumer group, so if you have several consumer groups in use, they do not affect each another when committing to one of them.

!!! tip

	Commits are done at a partition level when you use Kafka as a Message Broker, which means that streams that belong to the same partition are committed using the same position. The SDK currently does not expose the option to subscribe to only specific partitions of a topic, but commits will only ever affect partitions that are currently assigned to your client.

	Partitions and the Kafka rebalancing protocol are internal details of the Kafka implementation of the Quix SDK. You mainly don’t even need to worry about it because everything is abstracted within the [Streaming Context](/sdk/features/streaming-context) feature of the SDK.

### Automatic committing

By default, the SDK automatically commits messages for which all handlers returned at a regular default interval, which is every 5 seconds or 5,000 messages, whichever happens sooner. However this is subject to change.

If you wish to use different automatic commit intervals, use the following code:

=== "Python"
    
    ``` python
    from quixstreams import CommitOptions
    
    commit_settings = CommitOptions()
    commit_settings.commit_every = 100 # note, you can set this to none
    commit_settings.commit_interval = 500 # note, you can set this to none
    commit_settings.auto_commit_enabled = True
    topic_consumer = client.create_topic_consumer('yourtopic', commit_settings=commit_settings)
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.CreateTopicConsumer(topic, consumerGroup, new CommitOptions()
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
    
    topic_consumer = client.create_topic_consumer('yourtopic', commit_settings=CommitMode.Manual)
    ```

=== "C\#"
    
    ``` cs
    client.CreateTopicConsumer(topic, consumerGroup, CommitMode.Manual);
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

The piece of code above will commit anything – like parameter, event or metadata - read and served to you from the input topic up to this point.

### Commit callback

=== "Python"
Whenever a commit occurs, a callback is raised to let you know. This callback is invoked for both manual and automatic commits. You can set the callback using the following code:
    
    ``` python
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

=== "Python"
    
    ``` python
    topic_consumer = client.create_topic_consumer(test_topic, auto_offset_reset=AutoOffsetReset.Latest)
    or
    topic_consumer = client.create_topic_consumer(test_topic, auto_offset_reset=AutoOffsetReset.Earliest)
    ```

=== "C\#"
    
    ``` cs
    var topicConsumer = client.CreateTopicConsumer("MyTopic", autoOffset: AutoOffsetReset.Latest);
    or
    var topicConsumer = client.CreateTopicConsumer("MyTopic", autoOffset: AutoOffsetReset.Earliest);
    ```

## Revocation

When working with a broker, you have a certain number of topic streams assigned to your consumer. Over the course of the client’s lifetime, there may be several events causing a stream to be revoked, like another client joining or leaving the consumer group, so your application should be prepared to handle these scenarios in order to avoid data loss and/or avoidable reprocessing of messages.

!!! tip

	Kafka revokes entire partitions, but the SDK makes it easy to determine which streams are affected by providing two events you can listen to.

	Partitions and the Kafka rebalancing protocol are internal details of the Kafka implementation of the Quix SDK. You mainly don’t even need to worry about it because everything is abstracted within the [Streaming Context](/sdk/features/streaming-context) feature of the SDK.

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
    def on_stream_closed_handler(topic_consumer: TopicConsumer, stream: StreamConsumer, end_type: StreamEndType):
            print("Stream closed with {}".format(end_type))
    
    new_stream.on_stream_closed = on_stream_closed_handler
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

This is a minimal code example you can use to read data from a topic using the Quix SDK:

=== "Python"
    
    ``` python
    from quixstreams import *
    from quixstreams.app import App
    
    # Quix injects credentials automatically to the client. Alternatively, you can always pass an SDK token manually as an argument.
    client = QuixStreamingClient()
    
    topic_consumer = client.create_topic_consumer(TOPIC_ID)
    
    # read streams
    def read_stream(topic_consumer: TopicConsumer, new_stream: StreamConsumer):
    
        buffer = new_stream.parameters.create_buffer()
        buffer.on_read = on_parameter_data_handler
    
    def on_parameter_data_handler(topic_consumer: TopicConsumer, stream: StreamConsumer, data: TimeseriesData):
        with data:
            df = data.to_panda_dataframe()
            print(df.to_string())    
    
    # Hook up events before initiating read to avoid losing out on any data
    topic_consumer.on_stream_received = read_stream
    
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
    using Quix.Sdk.Streaming;
    using Quix.Sdk.Streaming.Configuration;
    using Quix.Sdk.Streaming.Models;
    
    
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
                var client = new Quix.Sdk.Streaming.QuixStreamingClient();
    
                using var topicConsumer = client.CreateTopicConsumer(TOPIC_ID);
    
                // Hook up events before initiating read to avoid losing out on any data
                topicConsumer.OnStreamReceived += (topic, streamConsumer) =>
                {
                    Console.WriteLine($"New stream read: {streamConsumer.StreamId}");
    
                    var buffer = streamConsumer.Parameters.CreateBuffer();
    
                    buffer.OnRead += (sender, args) =>
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

The Quix SDK uses the message brokers' internal protocol for data transmission. This protocol is both data and speed optimized so we do encourage you to use it. For that you need to use the SDK on both producer (writer) and consumer (reader) sides.

However, in some cases, you simply do not have the ability to run the Quix SDK on both sides and you need to have the ability to connect to the data in different ways.

To cater for these cases we added the ability to read the raw, unformatted, messages. Using this feature you have the ability to access the raw, unmodified content of each Kafka message from the topic. The data is a byte array, giving you the freedom to implement the protocol as needed, such as JSON, or comma-separated rows.

=== "Python"
    
    ``` python
    inp = client.create_raw_topic_consumer(TOPIC_ID)
    
    def on_raw_message(topic: RawTopicConsumer, msg: RawMessage):
        #bytearray containing bytes received from kafka
        data = msg.value
    
        #broker metadata as dict
        meta = msg.metadata
    
    inp.on_message_read = on_raw_message
    inp.subscribe()
    ```

=== "C\#"
    
    ``` cs
    var inp = client.CreateRawTopicConsumer(TOPIC_ID)
    
    inp.OnMessageRead += (sender, message) =>
    {
        var data = (byte[])message.Value;
    };
    
    inp.Subscribe()
    ```
