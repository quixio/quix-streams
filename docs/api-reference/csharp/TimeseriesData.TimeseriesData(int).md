#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')

## TimeseriesData(int) Constructor

Create a new empty Timeseries Data instance to allow create new timestamps and parameters values from scratch

```csharp
public TimeseriesData(int capacity=10);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(int).capacity'></a>

`capacity` [System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')

The number of timestamps that the new Timeseries Data initially store.   
            Using this parameter when you know the number of Timestamps you need to store will increase the performance of the writing.