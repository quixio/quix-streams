#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw')

## IRawTopicConsumer Interface

Interface to subscribe to incoming raw messages (capable to read non-quixstreams messages)

```csharp
public interface IRawTopicConsumer :
System.IDisposable
```

Derived  
&#8627; [RawTopicConsumer](RawTopicConsumer.md 'QuixStreams.Streaming.Raw.RawTopicConsumer')

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Methods | |
| :--- | :--- |
| [Subscribe()](IRawTopicConsumer.Subscribe().md 'QuixStreams.Streaming.Raw.IRawTopicConsumer.Subscribe()') | Start reading data from the topic.<br/>Use 'OnMessageReceived' event to read messages after executing this method |
| [Unsubscribe()](IRawTopicConsumer.Unsubscribe().md 'QuixStreams.Streaming.Raw.IRawTopicConsumer.Unsubscribe()') | Stops reading data from the topic. |

| Events | |
| :--- | :--- |
| [OnDisposed](IRawTopicConsumer.OnDisposed.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer.OnDisposed') | Raised when the resource is disposed |
| [OnErrorOccurred](IRawTopicConsumer.OnErrorOccurred.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer.OnErrorOccurred') | Event raised when a new error occurs |
| [OnMessageReceived](IRawTopicConsumer.OnMessageReceived.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer.OnMessageReceived') | Event raised when a message is received from the topic |
