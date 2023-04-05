#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## TimeseriesBuffer Class

Represents a class used to consume and produce stream messages in a buffered manner.

```csharp
public class TimeseriesBuffer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TimeseriesBuffer

Derived  
&#8627; [TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')  
&#8627; [TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Properties | |
| :--- | :--- |
| [BufferTimeout](TimeseriesBuffer.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.BufferTimeout') | Timeout configuration. [BufferTimeout](TimeseriesBufferConfiguration.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.BufferTimeout') |
| [CustomTrigger](TimeseriesBuffer.CustomTrigger.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.CustomTrigger') | Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked with the entire buffer content<br/>Defaults to null (disabled). |
| [CustomTriggerBeforeEnqueue](TimeseriesBuffer.CustomTriggerBeforeEnqueue.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.CustomTriggerBeforeEnqueue') | Gets or set the custom function which is invoked before adding the timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked before adding the timestamp to it.<br/>Defaults to null (disabled). |
| [Filter](TimeseriesBuffer.Filter.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.Filter') | Filter configuration. [Filter](TimeseriesBufferConfiguration.Filter.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.Filter') |
| [PacketSize](TimeseriesBuffer.PacketSize.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.PacketSize') | Packet Size configuration. [PacketSize](TimeseriesBufferConfiguration.PacketSize.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.PacketSize') |
| [TimeSpanInMilliseconds](TimeseriesBuffer.TimeSpanInMilliseconds.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.TimeSpanInMilliseconds') | TimeSpan configuration in Milliseconds. [TimeSpanInMilliseconds](TimeseriesBufferConfiguration.TimeSpanInMilliseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInMilliseconds') |
| [TimeSpanInNanoseconds](TimeseriesBuffer.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.TimeSpanInNanoseconds') | TimeSpan configuration in Nanoseconds. [TimeSpanInNanoseconds](TimeseriesBufferConfiguration.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInNanoseconds') |

| Methods | |
| :--- | :--- |
| [Dispose()](TimeseriesBuffer.Dispose().md 'QuixStreams.Streaming.Models.TimeseriesBuffer.Dispose()') | Dispose the buffer. It releases data out before the actual disposal. |

| Events | |
| :--- | :--- |
| [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') | Event invoked when TimeseriesData is received from the buffer |
| [OnRawReleased](TimeseriesBuffer.OnRawReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnRawReleased') | Event invoked when TimeseriesDataRaw is received from the buffer |
