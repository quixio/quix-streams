#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData.AddTimestamp(TimeSpan) Method

Starts adding a new set of parameter values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.TimeseriesDataTimestamp AddTimestamp(System.TimeSpan timeSpan);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.AddTimestamp(System.TimeSpan).timeSpan'></a>

`timeSpan` [System.TimeSpan](https://docs.microsoft.com/en-us/dotnet/api/System.TimeSpan 'System.TimeSpan')

The time since the  to add the parameter values at

#### Returns
[TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp')  
Timeseries data to add parameter values at the provided time