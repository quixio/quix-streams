#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

## TimeseriesBufferProducer(ITopicProducer, IStreamProducerInternal, TimeseriesBufferConfiguration) Constructor

Initializes a new instance of [TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

```csharp
internal TimeseriesBufferProducer(QuixStreams.Streaming.ITopicProducer topicProducer, QuixStreams.Streaming.IStreamProducerInternal streamProducer, QuixStreams.Streaming.Models.TimeseriesBufferConfiguration bufferConfiguration);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer(QuixStreams.Streaming.ITopicProducer,QuixStreams.Streaming.IStreamProducerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration).topicProducer'></a>

`topicProducer` [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')

The topic producer to publish with

<a name='QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer(QuixStreams.Streaming.ITopicProducer,QuixStreams.Streaming.IStreamProducerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration).streamProducer'></a>

`streamProducer` [IStreamProducerInternal](IStreamProducerInternal.md 'QuixStreams.Streaming.IStreamProducerInternal')

Stream writer owner

<a name='QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer(QuixStreams.Streaming.ITopicProducer,QuixStreams.Streaming.IStreamProducerInternal,QuixStreams.Streaming.Models.TimeseriesBufferConfiguration).bufferConfiguration'></a>

`bufferConfiguration` [TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

Configuration of the buffer