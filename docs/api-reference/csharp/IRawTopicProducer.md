#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw')

## IRawTopicProducer Interface

Interface to publish raw messages into a topic (capable to write non-quixstreams messages)

```csharp
public interface IRawTopicProducer :
System.IDisposable
```

Derived  
&#8627; [RawTopicProducer](RawTopicProducer.md 'QuixStreams.Streaming.Raw.RawTopicProducer')

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Methods | |
| :--- | :--- |
| [Publish(RawMessage)](IRawTopicProducer.Publish(RawMessage).md 'QuixStreams.Streaming.Raw.IRawTopicProducer.Publish(QuixStreams.Streaming.Raw.RawMessage)') | Publish data to the topic |

| Events | |
| :--- | :--- |
| [OnDisposed](IRawTopicProducer.OnDisposed.md 'QuixStreams.Streaming.Raw.IRawTopicProducer.OnDisposed') | Raised when the resource is disposed |
