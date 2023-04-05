#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

## TimeseriesBufferProducer.AddTimestampNanoseconds(long) Method

Starts adding a new set of parameter values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder AddTimestampNanoseconds(long timeNanoseconds);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestampNanoseconds(long).timeNanoseconds'></a>

`timeNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

The time in nanoseconds since the default [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') to add the parameter values at

#### Returns
[TimeseriesDataBuilder](TimeseriesDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder')  
Timeseries data builder to add parameter values at the provided time