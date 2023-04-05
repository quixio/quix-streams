#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

## TimeseriesBufferConfiguration.BufferTimeout Property

Gets or sets the maximum duration in milliseconds for which the buffer will be held before triggering [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event.   
[OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event is triggered when the configured [BufferTimeout](TimeseriesBufferConfiguration.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.BufferTimeout') has elapsed from the last data received by the buffer.  
Defaults to null (disabled).

```csharp
public System.Nullable<int> BufferTimeout { get; set; }
```

#### Property Value
[System.Nullable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')[System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')