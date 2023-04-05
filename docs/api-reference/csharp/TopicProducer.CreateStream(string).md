#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer')

## TopicProducer.CreateStream(string) Method

Creates a new stream and returns the related stream producer to operate it.

```csharp
public QuixStreams.Streaming.IStreamProducer CreateStream(string streamId);
```
#### Parameters

<a name='QuixStreams.Streaming.TopicProducer.CreateStream(string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Stream Id of the created stream

Implements [CreateStream(string)](ITopicProducer.CreateStream(string).md 'QuixStreams.Streaming.ITopicProducer.CreateStream(string)')

#### Returns
[IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer')  
Stream producer to allow the stream to push data to the platform