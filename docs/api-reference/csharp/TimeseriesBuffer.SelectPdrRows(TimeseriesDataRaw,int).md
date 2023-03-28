#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.SelectPdrRows(TimeseriesDataRaw, int) Method

Get row subset of the TimeseriesDataRaw starting from startIndex until the last timestamp available

```csharp
protected QuixStreams.Telemetry.Models.TimeseriesDataRaw SelectPdrRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw, int startIndex);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw,int).timeseriesDataRaw'></a>

`timeseriesDataRaw` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw,int).startIndex'></a>

`startIndex` [System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')

#### Returns
[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')