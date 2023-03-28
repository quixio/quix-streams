#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

## TimeseriesBufferProducer.AddTimestamp(TimeSpan) Method

Starts adding a new set of parameter values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder AddTimestamp(System.TimeSpan timeSpan);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestamp(System.TimeSpan).timeSpan'></a>

`timeSpan` [System.TimeSpan](https://docs.microsoft.com/en-us/dotnet/api/System.TimeSpan 'System.TimeSpan')

The time since the default [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') to add the parameter values at

#### Returns
[TimeseriesDataBuilder](TimeseriesDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder')  
Timeseries data builder to add parameter values at the provided time