#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.CopyTimeseriesDataRawIndex(TimeseriesDataRaw, int, int) Method

Copy one timestamp to another index of the buffer

```csharp
protected void CopyTimeseriesDataRawIndex(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw, int sourceIndex, int targetIndex);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.CopyTimeseriesDataRawIndex(QuixStreams.Telemetry.Models.TimeseriesDataRaw,int,int).timeseriesDataRaw'></a>

`timeseriesDataRaw` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

Buffer data

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.CopyTimeseriesDataRawIndex(QuixStreams.Telemetry.Models.TimeseriesDataRaw,int,int).sourceIndex'></a>

`sourceIndex` [System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')

Index of the timestamp to copy

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.CopyTimeseriesDataRawIndex(QuixStreams.Telemetry.Models.TimeseriesDataRaw,int,int).targetIndex'></a>

`targetIndex` [System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')

Target index where to copy timestamp