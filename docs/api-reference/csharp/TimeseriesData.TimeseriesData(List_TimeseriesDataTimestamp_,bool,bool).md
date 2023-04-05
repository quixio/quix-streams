#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData(List<TimeseriesDataTimestamp>, bool, bool) Constructor

Creates a new instance of [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData') with the provided timestamps

```csharp
public TimeseriesData(System.Collections.Generic.List<QuixStreams.Streaming.Models.TimeseriesDataTimestamp> timestamps, bool merge=true, bool clean=true);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(System.Collections.Generic.List_QuixStreams.Streaming.Models.TimeseriesDataTimestamp_,bool,bool).timestamps'></a>

`timestamps` [System.Collections.Generic.List&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')[TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')

The timestamps with timeseries data

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(System.Collections.Generic.List_QuixStreams.Streaming.Models.TimeseriesDataTimestamp_,bool,bool).merge'></a>

`merge` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Merge duplicated timestamps

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(System.Collections.Generic.List_QuixStreams.Streaming.Models.TimeseriesDataTimestamp_,bool,bool).clean'></a>

`clean` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Clean timestamps without values