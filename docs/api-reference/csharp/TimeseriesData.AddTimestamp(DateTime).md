#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData.AddTimestamp(DateTime) Method

Starts adding a new set of parameter values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.TimeseriesDataTimestamp AddTimestamp(System.DateTime dateTime);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.AddTimestamp(System.DateTime).dateTime'></a>

`dateTime` [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime')

The datetime to use for adding new parameter values

#### Returns
[TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp')  
Timeseries data to add parameter values at the provided time