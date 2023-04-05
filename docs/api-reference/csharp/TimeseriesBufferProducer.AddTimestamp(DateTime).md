#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

## TimeseriesBufferProducer.AddTimestamp(DateTime) Method

Starts adding a new set of parameter values at the given timestamp.  
Note, [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') is not used when invoking with a [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime')

```csharp
public QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder AddTimestamp(System.DateTime dateTime);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestamp(System.DateTime).dateTime'></a>

`dateTime` [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime')

The datetime to use for adding new parameter values

#### Returns
[TimeseriesDataBuilder](TimeseriesDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder')  
Timeseries data builder to add parameter values at the provided time