#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[LeadingEdgeTimeBuffer](LeadingEdgeTimeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer')

## LeadingEdgeTimeBuffer.OnPublish Event

Event raised when [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') condition is met and before data is published to the underlying [StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer').

```csharp
public event EventHandler<TimeseriesData> OnPublish;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')