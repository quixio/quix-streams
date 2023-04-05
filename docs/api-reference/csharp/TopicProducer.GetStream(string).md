#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer')

## TopicProducer.GetStream(string) Method

Retrieves a stream that was previously created by this instance, if the stream is not closed.

```csharp
public QuixStreams.Streaming.IStreamProducer GetStream(string streamId);
```
#### Parameters

<a name='QuixStreams.Streaming.TopicProducer.GetStream(string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The Id of the stream

Implements [GetStream(string)](ITopicProducer.GetStream(string).md 'QuixStreams.Streaming.ITopicProducer.GetStream(string)')

#### Returns
[IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer')  
Stream producer to allow the stream to push data to the platform or null if not found.