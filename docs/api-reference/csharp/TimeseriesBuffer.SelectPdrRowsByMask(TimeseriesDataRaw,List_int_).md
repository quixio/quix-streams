#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.SelectPdrRowsByMask(TimeseriesDataRaw, List<int>) Method

Get row subset of the TimeseriesDataRaw from a list of selected indexes

```csharp
protected QuixStreams.Telemetry.Models.TimeseriesDataRaw SelectPdrRowsByMask(QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw, System.Collections.Generic.List<int> filteredRows);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRowsByMask(QuixStreams.Telemetry.Models.TimeseriesDataRaw,System.Collections.Generic.List_int_).timeseriesDataRaw'></a>

`timeseriesDataRaw` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

Original data

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRowsByMask(QuixStreams.Telemetry.Models.TimeseriesDataRaw,System.Collections.Generic.List_int_).filteredRows'></a>

`filteredRows` [System.Collections.Generic.List&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')[System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')

IEnumerable containing indexes of rows to select (e.g. [0,3,5,6])

#### Returns
[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')