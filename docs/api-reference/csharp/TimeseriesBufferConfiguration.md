#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## TimeseriesBufferConfiguration Class

Describes the configuration for timeseries buffers

```csharp
public class TimeseriesBufferConfiguration
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TimeseriesBufferConfiguration

| Properties | |
| :--- | :--- |
| [BufferTimeout](TimeseriesBufferConfiguration.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.BufferTimeout') | Gets or sets the maximum duration in milliseconds for which the buffer will be held before triggering [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event. <br/>[OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event is triggered when the configured [BufferTimeout](TimeseriesBufferConfiguration.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.BufferTimeout') has elapsed from the last data received by the buffer.<br/>Defaults to null (disabled). |
| [CustomTrigger](TimeseriesBufferConfiguration.CustomTrigger.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.CustomTrigger') | Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked with the entire buffer content<br/>Defaults to null (disabled). |
| [CustomTriggerBeforeEnqueue](TimeseriesBufferConfiguration.CustomTriggerBeforeEnqueue.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.CustomTriggerBeforeEnqueue') | Gets or set the custom function which is invoked before adding the timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked before adding the timestamp to it.<br/>Defaults to null (disabled). |
| [Filter](TimeseriesBufferConfiguration.Filter.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.Filter') | Gets or sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not. <br/>Defaults to null (disabled). |
| [PacketSize](TimeseriesBufferConfiguration.PacketSize.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.PacketSize') | Gets or sets the max packet size in terms of values for the buffer. Each time the buffer has this amount<br/>of data the [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event is invoked and the data is cleared from the buffer.<br/>Defaults to null (disabled). |
| [TimeSpanInMilliseconds](TimeseriesBufferConfiguration.TimeSpanInMilliseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInMilliseconds') | Gets or sets the maximum time between timestamps for the buffer in milliseconds. When the difference between the<br/>earliest and latest buffered timestamp surpasses this number the [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event<br/>is invoked and the data is cleared from the buffer.<br/>Defaults to null (disabled).<br/>Note: This is a millisecond converter on top of [TimeSpanInNanoseconds](TimeseriesBufferConfiguration.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInNanoseconds'). They both work with same underlying value. |
| [TimeSpanInNanoseconds](TimeseriesBufferConfiguration.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInNanoseconds') | Gets or sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the<br/>earliest and latest buffered timestamp surpasses this number the [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') event<br/>is invoked and the data is cleared from the buffer.<br/>Defaults to None (disabled). |
