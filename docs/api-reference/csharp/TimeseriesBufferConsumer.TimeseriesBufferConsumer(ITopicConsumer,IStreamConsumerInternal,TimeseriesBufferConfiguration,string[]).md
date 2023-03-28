#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer').[TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')

## TimeseriesBufferConsumer(ITopicConsumer, IStreamConsumerInternal, TimeseriesBufferConfiguration, string[]) Constructor

Initializes a new instance of [TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')

```csharp
internal TimeseriesBufferConsumer(QuixStreams.Streaming.ITopicConsumer topicConsumer, QuixStreams.Streaming.IStreamConsumerInternal streamConsumer, QuixStreams.Streaming.Models.TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[]).topicConsumer'></a>

`topicConsumer` [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

The topic it belongs to

<a name='QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[]).streamConsumer'></a>

`streamConsumer` [IStreamConsumerInternal](IStreamConsumerInternal.md 'QuixStreams.Streaming.IStreamConsumerInternal')

Stream reader owner

<a name='QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[]).bufferConfiguration'></a>

`bufferConfiguration` [TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

Configuration of the buffer

<a name='QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[]).parametersFilter'></a>

`parametersFilter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

List of parameters to filter