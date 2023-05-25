#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## ITopicConsumer Interface

Interface to subscribe to incoming streams

```csharp
public interface ITopicConsumer :
System.IDisposable
```

Derived  
&#8627; [TopicConsumer](TopicConsumer.md 'QuixStreams.Streaming.TopicConsumer')

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Methods | |
| :--- | :--- |
| [Commit()](ITopicConsumer.Commit().md 'QuixStreams.Streaming.ITopicConsumer.Commit()') | Commit packages read up until now |
| [GetStateManager()](ITopicConsumer.GetStateManager().md 'QuixStreams.Streaming.ITopicConsumer.GetStateManager()') | Gets the manager for the topic states |
| [Subscribe()](ITopicConsumer.Subscribe().md 'QuixStreams.Streaming.ITopicConsumer.Subscribe()') | Start subscribing to streams.<br/>Use 'OnStreamReceived' event to read stream after executing this method |

| Events | |
| :--- | :--- |
| [OnCommitted](ITopicConsumer.OnCommitted.md 'QuixStreams.Streaming.ITopicConsumer.OnCommitted') | Raised when underlying source committed data read up to this point |
| [OnCommitting](ITopicConsumer.OnCommitting.md 'QuixStreams.Streaming.ITopicConsumer.OnCommitting') | Raised when underlying source is about to commit data read up to this point |
| [OnDisposed](ITopicConsumer.OnDisposed.md 'QuixStreams.Streaming.ITopicConsumer.OnDisposed') | Raised when the resource is disposed |
| [OnRevoking](ITopicConsumer.OnRevoking.md 'QuixStreams.Streaming.ITopicConsumer.OnRevoking') | Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point |
| [OnStreamReceived](ITopicConsumer.OnStreamReceived.md 'QuixStreams.Streaming.ITopicConsumer.OnStreamReceived') | Event raised when a new stream has been received for reading.<br/>Use the Stream Reader interface received to read data from the stream.<br/>You must execute 'Subscribe' method before starting to receive streams from this event |
| [OnStreamsRevoked](ITopicConsumer.OnStreamsRevoked.md 'QuixStreams.Streaming.ITopicConsumer.OnStreamsRevoked') | Raised when the underlying source of data became unavailable for the streams affected by it |
