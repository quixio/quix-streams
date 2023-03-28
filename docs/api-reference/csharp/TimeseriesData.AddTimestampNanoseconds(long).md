#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData.AddTimestampNanoseconds(long) Method

Starts adding a new set of parameter values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.TimeseriesDataTimestamp AddTimestampNanoseconds(long timeNanoseconds);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.AddTimestampNanoseconds(long).timeNanoseconds'></a>

`timeNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

The time in nanoseconds since the   to add the parameter values at

#### Returns
[TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp')  
Timeseries data to add parameter values at the provided time