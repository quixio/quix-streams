#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.WriteChunk(TimeseriesDataRaw) Method

Writes a chunck of data into the buffer

```csharp
protected internal void WriteChunk(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.WriteChunk(QuixStreams.Telemetry.Models.TimeseriesDataRaw).timeseriesDataRaw'></a>

`timeseriesDataRaw` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

Data in [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') format