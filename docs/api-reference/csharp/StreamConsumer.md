#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## StreamConsumer Class

Handles reading data for the assigned stream from the protocol.

```csharp
internal class StreamConsumer : QuixStreams.Telemetry.StreamPipeline,
QuixStreams.Streaming.IStreamConsumer
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; [QuixStreams.Telemetry.StreamPipeline](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.StreamPipeline 'QuixStreams.Telemetry.StreamPipeline') &#129106; StreamConsumer

Implements [IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')

| Constructors | |
| :--- | :--- |
| [StreamConsumer()](StreamConsumer.StreamConsumer().md 'QuixStreams.Streaming.StreamConsumer.StreamConsumer()') | Exists for mocking purposes |
| [StreamConsumer(ITopicConsumer, string)](StreamConsumer.StreamConsumer(ITopicConsumer,string).md 'QuixStreams.Streaming.StreamConsumer.StreamConsumer(QuixStreams.Streaming.ITopicConsumer, string)') | Initializes a new instance of [StreamConsumer](StreamConsumer.md 'QuixStreams.Streaming.StreamConsumer')<br/>This constructor is called internally by the [QuixStreams.Telemetry.StreamPipelineFactory](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.StreamPipelineFactory 'QuixStreams.Telemetry.StreamPipelineFactory') |

| Properties | |
| :--- | :--- |
| [Events](StreamConsumer.Events.md 'QuixStreams.Streaming.StreamConsumer.Events') | Gets the consumer for accessing event related information of the stream such as event definitions and values |
| [Properties](StreamConsumer.Properties.md 'QuixStreams.Streaming.StreamConsumer.Properties') | Gets the consumer for accessing the properties and metadata of the stream |
| [Timeseries](StreamConsumer.Timeseries.md 'QuixStreams.Streaming.StreamConsumer.Timeseries') | Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values |

| Events | |
| :--- | :--- |
| [OnEventData](StreamConsumer.OnEventData.md 'QuixStreams.Streaming.StreamConsumer.OnEventData') | Event raised when a new package of [QuixStreams.Telemetry.Models.EventDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDataRaw 'QuixStreams.Telemetry.Models.EventDataRaw') values have been received. |
| [OnEventDefinitionsChanged](StreamConsumer.OnEventDefinitionsChanged.md 'QuixStreams.Streaming.StreamConsumer.OnEventDefinitionsChanged') | Event raised when the [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions') have been changed. |
| [OnPackageReceived](StreamConsumer.OnPackageReceived.md 'QuixStreams.Streaming.StreamConsumer.OnPackageReceived') | Event raised when a stream package has been received. |
| [OnParameterDefinitionsChanged](StreamConsumer.OnParameterDefinitionsChanged.md 'QuixStreams.Streaming.StreamConsumer.OnParameterDefinitionsChanged') | Event raised when the [QuixStreams.Telemetry.Models.ParameterDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.ParameterDefinitions 'QuixStreams.Telemetry.Models.ParameterDefinitions') have been changed. |
| [OnStreamClosed](StreamConsumer.OnStreamClosed.md 'QuixStreams.Streaming.StreamConsumer.OnStreamClosed') | Event raised when the stream has closed. |
| [OnStreamPropertiesChanged](StreamConsumer.OnStreamPropertiesChanged.md 'QuixStreams.Streaming.StreamConsumer.OnStreamPropertiesChanged') | Event raised when the Stream Properties have changed. |
| [OnTimeseriesData](StreamConsumer.OnTimeseriesData.md 'QuixStreams.Streaming.StreamConsumer.OnTimeseriesData') | Event raised when a new package of [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw') values have been received. |
