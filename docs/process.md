# Processing data

Quix Streams is specifically designed to make real-time data processing easy. We provide high-performance technology, powered by our experience in F1, in a way that anybody with basic development skills can understand and use it very quickly.

!!! tip

	The [Quix Portal](https://portal.platform.quix.ai) offers you easy-to-use, auto-generated examples for reading, writing, and processing data based on our [open source library](https://github.com/quixio/quix-library). These examples work directly with your workspace topics. You can deploy these examples in our serverless environment with just a few clicks. For a quick test of the capabilities of the library, we recommend starting with those auto-generated examples.

Other streaming platforms are tied to a narrow set of functions, queries, or syntax to set up a data processor or model. This limited approach is often only suitable for some use cases, and tends to have a steep learning curve. The feature limitations and time investment required form a barrier to entry for inexperienced developers and data scientists alike.

Our approach is simpler and far more powerful than other streaming solutions. So much so that we can’t show you any Quix Streams related functions here because you literally don’t need them if you use Quix Streams.

With Quix Streams, you are not tied to complicated functions, lambdas, maps or query libraries to be able to deploy and process data in real time. You just need to know how to [subscribe](subscribe.md) and [publish](publish.md) data with Quix Streams — that’s it, the rest is up to you and your imagination.

Let’s see some examples of how to subscribe to and publish data using Quix Streams. We just [subscribe](subscribe.md) to data from the message broker, process it, and [publish](publish.md) it back to the message broker.

=== "Python - Data Frame"
    
    ``` python
    from quixstreams import TopicConsumer, StreamConsumer
    using pandas as pd

    # Callback triggered for each new data frame
    def on_dataframe_received_handler(stream: StreamConsumer, df: pd.DataFrame):
        output_df = pd.DataFrame()
        # you can use 'time', 'timestamp', 'datetime' for your own dataframes, but
        # when you are given a dataframe by this call or data.to_dataframe
        # 'timestamp' will be used as the column name
        output_df["time"] = df["timestamp"]
        output_df["TAG__LapNumber"] = df["TAG__LapNumber"]
    
        # If braking force applied is more than 50%, we mark HardBraking with True
        output_df["HardBraking"] = df.apply(lambda row: "True" if row.Brake > 0.5 else "False", axis=1)
    
        stream_producer.timeseries.publish(output_df)  # Send data to the output stream

    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler
    ```

=== "Python - Plain"
    
    ``` python
    from quixstreams import TopicConsumer, StreamConsumer, TimeseriesData

    # Callback triggered for each new data frame
    def on_data_received_handler(stream: StreamConsumer, data: TimeseriesData):
        with data:
            for row in data.timestamps:
                # If braking force applied is more than 50%, we mark HardBraking with True
                hard_braking = row.parameters["Brake"].numeric_value > 0.5
        
                stream_producer.timeseries \
                    .add_timestamp(row.timestamp) \
                    .add_tag("LapNumber", row.tags["LapNumber"]) \
                    .add_value("HardBraking", hard_braking) \
                    .publish()

    stream_consumer.timeseries.on_data_received = on_dataframe_received_handler
    ```

=== "C\#"
    
    ``` cs
    streamConsumer.timeseries.OnDataReceived += (stream, args) =>
    {
        var outputData = new TimeseriesData();
    
        // We calculate mean value for each second of data to effectively down-sample source topic to 1Hz.
        outputData.AddTimestamp(args.Data.Timestamps.First().Timestamp)
            .AddValue("ParameterA 10Hz", args.Data.Timestamps.Average(s => s.Parameters["ParameterA"].NumericValue.GetValueOrDefault()))
            .AddValue("ParameterA source frequency", args.Data.Timestamps.Count);
    
        // Send data back to the stream
        streamProducer.Timeseries.Publish(outputData);
    };
    ```

So, because you are not tied to any narrow processing architecture, you can use any methods, classes or libraries that you are already familiar with to implement your model or data processor.

Check out more samples using our [open source library samples](https://github.com/quixio/quix-library).
