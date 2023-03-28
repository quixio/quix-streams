#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## TimeseriesBufferProducer Class

Class used to write time-series to [IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer') in a buffered manner

```csharp
public class TimeseriesBufferProducer : QuixStreams.Streaming.Models.TimeseriesBuffer
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; [TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer') &#129106; TimeseriesBufferProducer

| Constructors | |
| :--- | :--- |
| [TimeseriesBufferProducer(ITopicProducer, IStreamProducerInternal, TimeseriesBufferConfiguration)](TimeseriesBufferProducer.TimeseriesBufferProducer(ITopicProducer,IStreamProducerInternal,TimeseriesBufferConfiguration).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer(QuixStreams.Streaming.ITopicProducer, QuixStreams.Streaming.IStreamProducerInternal, QuixStreams.Streaming.Models.TimeseriesBufferConfiguration)') | Initializes a new instance of [TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer') |

| Properties | |
| :--- | :--- |
| [DefaultTags](TimeseriesBufferProducer.DefaultTags.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.DefaultTags') | Default tags injected for all parameters values sent by this buffer. |
| [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') | Default Epoch used for Timestamp parameter values. Datetime added on top of all the Timestamps. |

| Methods | |
| :--- | :--- |
| [AddTimestamp(DateTime)](TimeseriesBufferProducer.AddTimestamp(DateTime).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestamp(System.DateTime)') | Starts adding a new set of parameter values at the given timestamp.<br/>Note, [Epoch](TimeseriesBufferProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Epoch') is not used when invoking with [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime') |
| [AddTimestamp(TimeSpan)](TimeseriesBufferProducer.AddTimestamp(TimeSpan).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestamp(System.TimeSpan)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestampMilliseconds(long)](TimeseriesBufferProducer.AddTimestampMilliseconds(long).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestampMilliseconds(long)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestampNanoseconds(long)](TimeseriesBufferProducer.AddTimestampNanoseconds(long).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.AddTimestampNanoseconds(long)') | Starts adding a new set of parameter values at the given timestamp. |
| [Dispose()](TimeseriesBufferProducer.Dispose().md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Dispose()') | Flushes internal buffers and disposes |
| [Flush()](TimeseriesBufferProducer.Flush().md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Flush()') | Immediately writes the data from the buffer without waiting for buffer condition to fulfill |
| [Publish(TimeseriesData)](TimeseriesBufferProducer.Publish(TimeseriesData).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.Publish(QuixStreams.Streaming.Models.TimeseriesData)') | Write timeseries data to the buffer |
