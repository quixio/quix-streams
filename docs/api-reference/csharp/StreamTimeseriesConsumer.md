#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer')

## StreamTimeseriesConsumer Class

Helper class for reader [QuixStreams.Telemetry.Models.ParameterDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.ParameterDefinitions 'QuixStreams.Telemetry.Models.ParameterDefinitions') and [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

```csharp
public class StreamTimeseriesConsumer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamTimeseriesConsumer

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [StreamTimeseriesConsumer(ITopicConsumer, IStreamConsumerInternal)](StreamTimeseriesConsumer.StreamTimeseriesConsumer(ITopicConsumer,IStreamConsumerInternal).md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer(QuixStreams.Streaming.ITopicConsumer, QuixStreams.Streaming.IStreamConsumerInternal)') | Initializes a new instance of [StreamTimeseriesConsumer](StreamTimeseriesConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer') |

| Properties | |
| :--- | :--- |
| [Buffers](StreamTimeseriesConsumer.Buffers.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.Buffers') | List of buffers created for this stream |
| [Definitions](StreamTimeseriesConsumer.Definitions.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.Definitions') | Gets the latest set of event definitions |

| Methods | |
| :--- | :--- |
| [CreateBuffer(TimeseriesBufferConfiguration, string[])](StreamTimeseriesConsumer.CreateBuffer(TimeseriesBufferConfiguration,string[]).md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.CreateBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration, string[])') | Create a new Parameters buffer for reading data |
| [CreateBuffer(string[])](StreamTimeseriesConsumer.CreateBuffer(string[]).md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.CreateBuffer(string[])') | Create a new Parameters buffer for reading data |

| Events | |
| :--- | :--- |
| [OnDataReceived](StreamTimeseriesConsumer.OnDataReceived.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.OnDataReceived') | Event raised when data is available to read (without buffering)<br/>This event does not use Buffers and data will be raised as they arrive without any processing. |
| [OnDefinitionsChanged](StreamTimeseriesConsumer.OnDefinitionsChanged.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.OnDefinitionsChanged') | Raised when the parameter definitions have changed for the stream.<br/>See [Definitions](StreamTimeseriesConsumer.Definitions.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.Definitions') for the latest set of parameter definitions |
| [OnRawReceived](StreamTimeseriesConsumer.OnRawReceived.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.OnRawReceived') | Event raised when data is received (without buffering) in raw transport format<br/>This event does not use Buffers and data will be raised as they arrive without any processing. |
