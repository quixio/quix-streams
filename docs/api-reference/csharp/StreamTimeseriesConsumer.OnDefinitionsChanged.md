#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer').[StreamTimeseriesConsumer](StreamTimeseriesConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer')

## StreamTimeseriesConsumer.OnDefinitionsChanged Event

Raised when the parameter definitions have changed for the stream.  
See [Definitions](StreamTimeseriesConsumer.Definitions.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.Definitions') for the latest set of parameter definitions

```csharp
public event EventHandler<ParameterDefinitionsChangedEventArgs> OnDefinitionsChanged;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[QuixStreams.Streaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Streaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs 'QuixStreams.Streaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')