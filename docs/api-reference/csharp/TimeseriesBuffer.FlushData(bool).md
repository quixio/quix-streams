#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.FlushData(bool) Method

Flush data from the buffer and release it to make it available for Read events subscribers

```csharp
internal void FlushData(bool force);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.FlushData(bool).force'></a>

`force` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

If true is flushing data even when disposed