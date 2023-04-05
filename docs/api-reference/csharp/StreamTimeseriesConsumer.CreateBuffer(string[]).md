#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer').[StreamTimeseriesConsumer](StreamTimeseriesConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer')

## StreamTimeseriesConsumer.CreateBuffer(string[]) Method

Create a new Parameters buffer for reading data

```csharp
public QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer CreateBuffer(params string[] parametersFilter);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer.CreateBuffer(string[]).parametersFilter'></a>

`parametersFilter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

List of parameters to filter

#### Returns
[TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')  
Parameters reading buffer