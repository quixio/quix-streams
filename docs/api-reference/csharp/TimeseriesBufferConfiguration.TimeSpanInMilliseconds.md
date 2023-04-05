#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

## TimeseriesBufferConfiguration.TimeSpanInMilliseconds Property

Gets or sets the maximum time between timestamps for the buffer in milliseconds. When the difference between the  
earliest and latest buffered timestamp surpasses this number the [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event  
is invoked and the data is cleared from the buffer.  
Defaults to null (disabled).  
Note: This is a millisecond converter on top of [TimeSpanInNanoseconds](TimeseriesBufferConfiguration.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInNanoseconds'). They both work with same underlying value.

```csharp
public System.Nullable<long> TimeSpanInMilliseconds { get; set; }
```

#### Property Value
[System.Nullable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')[System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')