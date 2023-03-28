#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## ITopicProducer Interface

Interface to write outgoing streams

```csharp
public interface ITopicProducer :
System.IDisposable
```

Derived  
&#8627; [ITopicProducerInternal](ITopicProducerInternal.md 'QuixStreams.Streaming.ITopicProducerInternal')  
&#8627; [TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer')

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Methods | |
| :--- | :--- |
| [CreateStream()](ITopicProducer.CreateStream().md 'QuixStreams.Streaming.ITopicProducer.CreateStream()') | Creates a new stream and returns the related stream writer to operate it. |
| [CreateStream(string)](ITopicProducer.CreateStream(string).md 'QuixStreams.Streaming.ITopicProducer.CreateStream(string)') | Creates a new stream and returns the related stream writer to operate it. |
| [GetOrCreateStream(string, Action&lt;IStreamProducer&gt;)](ITopicProducer.GetOrCreateStream(string,Action_IStreamProducer_).md 'QuixStreams.Streaming.ITopicProducer.GetOrCreateStream(string, System.Action<QuixStreams.Streaming.IStreamProducer>)') | Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream. |
| [GetStream(string)](ITopicProducer.GetStream(string).md 'QuixStreams.Streaming.ITopicProducer.GetStream(string)') | Retrieves a stream that was previously created by this instance, if the stream is not closed. |

| Events | |
| :--- | :--- |
| [OnDisposed](ITopicProducer.OnDisposed.md 'QuixStreams.Streaming.ITopicProducer.OnDisposed') | Raised when the resource finished disposing |
