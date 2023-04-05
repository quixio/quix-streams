#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer').[StreamTimeseriesConsumer](StreamTimeseriesConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer')

## StreamTimeseriesConsumer.CreateBuffer(string[]) Method

Creates a new buffer for reading data

```csharp
public QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer CreateBuffer(params string[] parametersFilter);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.CreateBuffer(string[]).parametersFilter'></a>

`parametersFilter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

Zero or more parameter identifiers to filter as a whitelist. If provided, only those parameters will be available through this buffer

#### Returns
[TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')  
[TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer') which will raise OnDataReceived event when new data is consumed