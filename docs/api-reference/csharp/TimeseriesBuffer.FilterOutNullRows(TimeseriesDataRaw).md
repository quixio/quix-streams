#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.FilterOutNullRows(TimeseriesDataRaw) Method

Remove rows that only contain null values

```csharp
protected QuixStreams.Telemetry.Models.TimeseriesDataRaw FilterOutNullRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.FilterOutNullRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw).timeseriesDataRaw'></a>

`timeseriesDataRaw` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

Data to be cleaned

#### Returns
[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')  
Cleaned data without the rows containing only null values