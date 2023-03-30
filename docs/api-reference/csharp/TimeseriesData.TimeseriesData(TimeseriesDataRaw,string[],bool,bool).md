#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData(TimeseriesDataRaw, string[], bool, bool) Constructor

Creates a new instance of [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData') based on a [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw') instance

```csharp
public TimeseriesData(QuixStreams.Telemetry.Models.TimeseriesDataRaw rawData, string[] parametersFilter=null, bool merge=true, bool clean=true);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(QuixStreams.Telemetry.Models.TimeseriesDataRaw,string[],bool,bool).rawData'></a>

`rawData` [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')

Timeseries Data Raw instance from where lookup the data

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(QuixStreams.Telemetry.Models.TimeseriesDataRaw,string[],bool,bool).parametersFilter'></a>

`parametersFilter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

List of parameters to filter

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(QuixStreams.Telemetry.Models.TimeseriesDataRaw,string[],bool,bool).merge'></a>

`merge` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Merge duplicated timestamps

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(QuixStreams.Telemetry.Models.TimeseriesDataRaw,string[],bool,bool).clean'></a>

`clean` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Clean timestamps without values