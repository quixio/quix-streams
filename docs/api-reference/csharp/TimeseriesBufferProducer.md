#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## TimeseriesBufferProducer Class

A class for producing timeseries data to an [IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer') in a buffered manner.

```csharp
public class TimeseriesBufferProducer : QuixStreams.Streaming.Models.TimeseriesBuffer
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; [TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer') &#129106; TimeseriesBufferProducer

| Properties | |
| :--- | :--- |
| [DefaultTags](TimeseriesBufferProducer.DefaultTags.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.DefaultTags') | Default tags injected for all parameters values sent by this buffer. |
| [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') | Default Epoch used for Timestamp parameter values. Datetime added on top of all the Timestamps. |

| Methods | |
| :--- | :--- |
| [AddTimestamp(DateTime)](TimeseriesBufferProducer.AddTimestamp(DateTime).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestamp(System.DateTime)') | Starts adding a new set of parameter values at the given timestamp.<br/>Note, [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') is not used when invoking with a [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime') |
| [AddTimestamp(TimeSpan)](TimeseriesBufferProducer.AddTimestamp(TimeSpan).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestamp(System.TimeSpan)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestampMilliseconds(long)](TimeseriesBufferProducer.AddTimestampMilliseconds(long).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestampMilliseconds(long)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestampNanoseconds(long)](TimeseriesBufferProducer.AddTimestampNanoseconds(long).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestampNanoseconds(long)') | Starts adding a new set of parameter values at the given timestamp. |
| [Dispose()](TimeseriesBufferProducer.Dispose().md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Dispose()') | Flushes the internal buffers and disposes the object. |
| [Flush()](TimeseriesBufferProducer.Flush().md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Flush()') | Immediately publishes the data from the buffer without waiting for the buffer condition to be fulfilled. |
| [Publish(TimeseriesData)](TimeseriesBufferProducer.Publish(TimeseriesData).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Publish(QuixStreams.Streaming.Models.TimeseriesData)') | Publish timeseries data to the buffer. |
| [Publish(TimeseriesDataTimestamp)](TimeseriesBufferProducer.Publish(TimeseriesDataTimestamp).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Publish(QuixStreams.Streaming.Models.TimeseriesDataTimestamp)') | Publish single timestamp to the buffer. |
| [Publish(TimeseriesDataRaw)](TimeseriesBufferProducer.Publish(TimeseriesDataRaw).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Publish(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Publish timeseries data raw to the buffer. |
