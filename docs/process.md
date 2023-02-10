# Processing data

The Quix SDK is specifically designed to make real-time data processing very easy. We provide high-performance technology, inherited from F1, in a way that anybody with basic development skills can understand and use it very quickly.

!!! tip

	The [Quix Portal](https://portal.platform.quix.ai){target=_blank} offers you easy-to-use, auto-generated examples for reading, writing, and processing data. These examples work directly with your workspace Topics. You can deploy these examples in our serverless environment with just a few clicks. For a quick test of the capabilities of the SDK, we recommend starting with those auto-generated examples.

Other streaming platforms are tied to a narrow set of functions, queries, or syntax to set up a data processor or Model. This limited approach is often only suitable for some use cases, and tends to have a steep learning curve. The feature limitations and time investment required form a barrier to entry for inexperienced developers and Data scientists alike.

Our approach is simpler and far more powerful than other streaming solutions. So much so that we can’t show you any SDK related functions here because you literally don’t need them if you use the Quix SDK.

With the Quix SDK, you are not tied to complicated Functions, Lambdas, Maps or Query libraries to be able to deploy and process data in real time. You just need to know how to [read](/sdk/read) and [write](/sdk/write) data with the SDK — that’s it, the rest is up to you and your imagination.

Let’s see some examples of how to read and write data in a Data processor using the Quix SDK. We just [read](/sdk/read) data from the message broker, process it, and [write](/sdk/write) it back to the stream.

=== "Python - Data Frame"
    
    ``` python
    # Callback triggered for each new data frame
    def on_parameter_data_handler(data: ParameterData):
        with data:
    
            df = data.to_panda_dataframe()  # Input data frame
            output_df = pd.DataFrame()
            output_df["time"] = df["time"]
            output_df["TAG__LapNumber"] = df["TAG__LapNumber"]
        
            # If braking force applied is more than 50%, we mark HardBraking with True
            output_df["HardBraking"] = df.apply(lambda row: "True" if row.Brake > 0.5 else "False", axis=1)
        
            stream_output.parameters.write(output_df)  # Send data back to the stream
    ```

=== "Python - Plain"
    
    ``` python
    # Callback triggered for each new data frame
    def on_parameter_data_handler(data: ParameterData):
        with data:
            for row in data.timestamps:
                # If braking force applied is more than 50%, we mark HardBraking with True
                hard_braking = row.parameters["Brake"].numeric_value > 0.5
        
                stream_output.parameters \
                    .add_timestamp(row.timestamp) \
                    .add_tag("LapNumber", row.tags["LapNumber"]) \
                    .add_value("HardBraking", hard_braking) \
                    .write()
    ```

=== "C\#"
    
    ``` cs
    buffer.OnRead += (data) =>
    {
        var outputData = new ParameterData();
    
        // We calculate mean value for each second of data to effectively down-sample source topic to 1Hz.
        outputData.AddTimestamp(data.Timestamps.First().Timestamp)
            .AddValue("ParameterA 10Hz", data.Timestamps.Average(s => s.Parameters["ParameterA"].NumericValue.GetValueOrDefault()))
            .AddValue("ParameterA source frequency", data.Timestamps.Count);
    
        // Send data back to the stream
        streamOutput.Parameters.Write(outputData);
    };
    ```

So, because you are not tied to any narrow processing architecture, you can use any methods, classes or libraries that you are already familiar with to implement your model or data processor.

Check the complete [code example](https://github.com/quixai/car-data-model) in GitHub for further information.
