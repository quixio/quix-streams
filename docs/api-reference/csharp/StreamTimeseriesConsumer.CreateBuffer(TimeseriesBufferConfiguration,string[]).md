#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer').[StreamTimeseriesConsumer](StreamTimeseriesConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer')

## StreamTimeseriesConsumer.CreateBuffer(TimeseriesBufferConfiguration, string[]) Method

Creates a new buffer for reading data

```csharp
public QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer CreateBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration bufferConfiguration=null, params string[] parametersFilter);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.CreateBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[]).bufferConfiguration'></a>

`bufferConfiguration` [TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

An optional TimeseriesBufferConfiguration

<a name='QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.CreateBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[]).parametersFilter'></a>

`parametersFilter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

Zero or more parameter identifiers to filter as a whitelist. If provided, only those parameters will be available through this buffer

#### Returns
[TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')  
[TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer') which will raise OnDataReceived event when new data is consumed