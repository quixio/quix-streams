#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## StreamTimeseriesProducer Class

Helper class for producing [ParameterDefinition](ParameterDefinition.md 'QuixStreams.Streaming.Models.ParameterDefinition') and [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

```csharp
public class StreamTimeseriesProducer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamTimeseriesProducer

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Properties | |
| :--- | :--- |
| [Buffer](StreamTimeseriesProducer.Buffer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.Buffer') | Gets the buffer for producing timeseries data |
| [DefaultLocation](StreamTimeseriesProducer.DefaultLocation.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.DefaultLocation') | Default Location of the parameters. Parameter definitions added with [AddDefinition(string, string, string)](StreamTimeseriesProducer.AddDefinition(string,string,string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.AddDefinition(string, string, string)') will be inserted at this location.<br/>See [AddLocation(string)](StreamTimeseriesProducer.AddLocation(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.AddLocation(string)') for adding definitions at a different location without changing default.<br/>Example: "/Group1/SubGroup2" |

| Methods | |
| :--- | :--- |
| [AddDefinition(string, string, string)](StreamTimeseriesProducer.AddDefinition(string,string,string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.AddDefinition(string, string, string)') | Adds a new parameter definition to the [StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer'). Configure it with the builder methods. |
| [AddDefinitions(List&lt;ParameterDefinition&gt;)](StreamTimeseriesProducer.AddDefinitions(List_ParameterDefinition_).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.AddDefinitions(System.Collections.Generic.List<QuixStreams.Streaming.Models.ParameterDefinition>)') | Adds a list of definitions to the [StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer'). Configure it with the builder methods. |
| [AddLocation(string)](StreamTimeseriesProducer.AddLocation(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.AddLocation(string)') | Adds a new location in the parameters groups hierarchy |
| [CreateLeadingEdgeBuffer(int)](StreamTimeseriesProducer.CreateLeadingEdgeBuffer(int).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.CreateLeadingEdgeBuffer(int)') | Creates a new [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') using this producer where tags form part of the row's key<br/>and can't be modified after initial values |
| [CreateLeadingEdgeTimeBuffer(int)](StreamTimeseriesProducer.CreateLeadingEdgeTimeBuffer(int).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.CreateLeadingEdgeTimeBuffer(int)') | Creates a new [LeadingEdgeTimeBuffer](LeadingEdgeTimeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer') using this producer where tags do not form part of the row's key<br/>and can be freely modified after initial values |
| [Dispose()](StreamTimeseriesProducer.Dispose().md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.Dispose()') | Flushes internal buffers and disposes |
| [Flush()](StreamTimeseriesProducer.Flush().md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.Flush()') | Immediately publish timeseries data and definitions from the buffer without waiting for buffer condition to fulfill for either |
| [Publish(TimeseriesData)](StreamTimeseriesProducer.Publish(TimeseriesData).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.Publish(QuixStreams.Streaming.Models.TimeseriesData)') | Publish data to stream without any buffering |
| [Publish(TimeseriesDataTimestamp)](StreamTimeseriesProducer.Publish(TimeseriesDataTimestamp).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.Publish(QuixStreams.Streaming.Models.TimeseriesDataTimestamp)') | Publish single timestamp to stream without any buffering |
| [Publish(TimeseriesDataRaw)](StreamTimeseriesProducer.Publish(TimeseriesDataRaw).md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.Publish(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Publish data in TimeseriesDataRaw format without any buffering |
