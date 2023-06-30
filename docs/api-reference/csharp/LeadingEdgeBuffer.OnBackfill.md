#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer')

## LeadingEdgeBuffer.OnBackfill Event

Data arriving with a timestamp earlier then the latest released timestamp is discarded but released in this event for further processing or forwarding.

```csharp
public event EventHandler<TimeseriesData> OnBackfill;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')