#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')

## ITopicProducer.GetOrCreateStream(string, Action<IStreamProducer>) Method

Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream.

```csharp
QuixStreams.Streaming.IStreamProducer GetOrCreateStream(string streamId, System.Action<QuixStreams.Streaming.IStreamProducer> onStreamCreated=null);
```
#### Parameters

<a name='QuixStreams.Streaming.ITopicProducer.GetOrCreateStream(string,System.Action_QuixStreams.Streaming.IStreamProducer_).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The Id of the stream you want to get or create

<a name='QuixStreams.Streaming.ITopicProducer.GetOrCreateStream(string,System.Action_QuixStreams.Streaming.IStreamProducer_).onStreamCreated'></a>

`onStreamCreated` [System.Action&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-1 'System.Action`1')[IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-1 'System.Action`1')

Callback executed when a new Stream is created in the topic producer because it doesn't exist.

#### Returns
[IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer')  
Stream producer to allow the stream to push data to the platform.