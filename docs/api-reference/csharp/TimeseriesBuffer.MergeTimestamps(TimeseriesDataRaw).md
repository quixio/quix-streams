#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.MergeTimestamps(TimeseriesDataRaw) Method

Merge existing timestamps with the same timestamp and tags

```csharp
protected QuixStreams.Telemetry.Models.TimeseriesDataRaw MergeTimestamps(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.MergeTimestamps(QuixStreams.Telemetry.Models.TimeseriesDataRaw).timeseriesDataRaw'></a>

`timeseriesDataRaw` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

Data to merge

#### Returns
[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')  
New object with the proper length containing merged values