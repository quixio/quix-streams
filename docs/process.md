# Processing data

With Quix Streams, the main operations you need to learn are how to [subscribe](subscribe.md) to topics and how to [publish](publish.md) data.

The typical pattern for creating a service is to [subscribe](subscribe.md) to data in a topic, process it, and then [publish](publish.md) it to a topic. A series of such services can be connected together into a stream processing pipeline.

If using Quix Streams with Python, you also have the option of using Pandas data frames, which enables a familiar approach to processing time-series data. You can also process time-series data in other formats, and it is also possible to process event data. 

The following examples show how to process data in the Pandas data frame format, the format defined by the `TimeseriesData` class, and the event data format:

=== "Python - Data Frame"
    
    ``` python
    import quixstreams as qx
    using pandas as pd

    client = qx.QuixStreamingClient()

    topic_consumer = client.get_topic_consumer("input-topic")
    topic_producer = client.get_topic_producer("output-topic")

    # Callback triggered for each new data frame
    def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
        output_df = pd.DataFrame()
        output_df["time"] = df["timestamp"]
        output_df["TAG__LapNumber"] = df["TAG__LapNumber"]
    
        # If braking force applied is more than 50%, mark HardBraking with True
        output_df["HardBraking"] = df.apply(lambda row: "True" if row.Brake > 0.5 else "False", axis=1)
    
        topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries.publish(output_df)  # Send data to the output stream

    def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
        stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler

    # subscribe to new streams being received
    topic_consumer.on_stream_received = on_stream_received_handler

    print("Listening to streams. Press CTRL-C to exit.")

    # Handle termination signals and provide a graceful exit
    qx.App.run()
    ```

=== "Python - Plain"
    
    ``` python
    import quixstreams as qx

    client = qx.QuixStreamingClient()

    topic_consumer = client.get_topic_consumer("input-topic")
    topic_producer = client.get_topic_producer("output-topic")

    # Callback triggered for each new data
    def on_data_received_handler(stream_consumer: qx.StreamConsumer, data: qx.TimeseriesData):
        with data:
            for row in data.timestamps:
                # If braking force applied is more than 50%, mark HardBraking with True
                hard_braking = row.parameters["Brake"].numeric_value > 0.5
        
                topic_producer.get_or_create_stream(stream_consumer.stream_id).timeseries \
                    .add_timestamp(row.timestamp) \
                    .add_tag("LapNumber", row.tags["LapNumber"]) \
                    .add_value("HardBraking", hard_braking) \
                    .publish()
 
     def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
        stream_consumer.timeseries.on_data_received = on_data_received_handler

    # subscribe to new streams being received
    topic_consumer.on_stream_received = on_stream_received_handler

    print("Listening to streams. Press CTRL-C to exit.")

    # Handle termination signals and provide a graceful exit
    qx.App.run()
   ```

=== "Python - Event data"

    ``` python
    import quixstreams as qx

    client = qx.QuixStreamingClient()

    topic_consumer = client.get_topic_consumer("input-topic")
    topic_producer = client.get_topic_producer("output-topic")

    # Callback triggered for each new event data
    def on_event_data_received_handler(stream: qx.StreamConsumer, data: qx.EventData):
        with data:
            # process as required and then write out to the output topic
            topic_producer.get_or_create_stream(stream_consumer.stream_id).events.publish(data)

    def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
        stream_consumer.event.on_data_received = on_event_data_received_handler

    # subscribe to new streams being received
    topic_consumer.on_stream_received = on_stream_received_handler

    print("Listening to streams. Press CTRL-C to exit.")

    # Handle termination signals and provide a graceful exit
    qx.App.run()
    ```

=== "C\#"
    
    ``` cs
    streamConsumer.timeseries.OnDataReceived += (stream, args) =>
    {
        var outputData = new TimeseriesData();
    
        // Calculate mean value for each second of data to effectively down-sample source topic to 1Hz.
        outputData.AddTimestamp(args.Data.Timestamps.First().Timestamp)
            .AddValue("ParameterA 10Hz", args.Data.Timestamps.Average(s => s.Parameters["ParameterA"].NumericValue.GetValueOrDefault()))
            .AddValue("ParameterA source frequency", args.Data.Timestamps.Count);
    
        // Send data back to the stream
        streamProducer.Timeseries.Publish(outputData);
    };
    ```

Your code can use any convenient libraries. If working in the Quix Platform, your code can include these libraries by adding them to the `requirements.txt` file for Python, or `nuget.config` for C#.

!!! tip

	The [Quix Portal](https://portal.platform.quix.ai/self-sign-up) provides easy-to-use [open source samples](https://github.com/quixio/quix-samples) for reading, writing, and processing data. These samples work directly with your workspace topics. You can configure and deploy these samples in the Quix serverless environment using the Quix Portal UI. While the samples provide ready-made connectors and transforms you can use in your pipeline, you can also explore their code to see how they work, and adapt them to make your own custom connectors and transforms.
