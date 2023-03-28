#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData.AddTimestampNanoseconds(long, bool) Method

Starts adding a new set of parameter values at the given timestamp.

```csharp
internal QuixStreams.Streaming.Models.TimeseriesDataTimestamp AddTimestampNanoseconds(long timeNanoseconds, bool epochIncluded);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.AddTimestampNanoseconds(long,bool).timeNanoseconds'></a>

`timeNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

The time in nanoseconds since the   to add the parameter values at

<a name='QuixStreams.Streaming.Models.TimeseriesData.AddTimestampNanoseconds(long,bool).epochIncluded'></a>

`epochIncluded` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Epoch offset is included in the timestamp

#### Returns
[TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp')  
Timeseries data to add parameter values at the provided time