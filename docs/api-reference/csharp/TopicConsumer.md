#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## TopicConsumer Class

Implementation of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer') to read incoming streams

```csharp
public class TopicConsumer :
QuixStreams.Streaming.ITopicConsumer,
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TopicConsumer

Implements [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer'), [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [TopicConsumer(TelemetryKafkaConsumer)](TopicConsumer.TopicConsumer(TelemetryKafkaConsumer).md 'QuixStreams.Streaming.TopicConsumer.TopicConsumer(QuixStreams.Telemetry.Kafka.TelemetryKafkaConsumer)') | Initializes a new instance of [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient') |

| Methods | |
| :--- | :--- |
| [Commit()](TopicConsumer.Commit().md 'QuixStreams.Streaming.TopicConsumer.Commit()') | Commit packages read up until now |
| [Subscribe()](TopicConsumer.Subscribe().md 'QuixStreams.Streaming.TopicConsumer.Subscribe()') | Start subscribing to streams.<br/>Use 'OnStreamReceived' event to read stream after executing this method |

| Events | |
| :--- | :--- |
| [OnCommitted](TopicConsumer.OnCommitted.md 'QuixStreams.Streaming.TopicConsumer.OnCommitted') | Raised when underlying source committed data read up to this point |
| [OnCommitting](TopicConsumer.OnCommitting.md 'QuixStreams.Streaming.TopicConsumer.OnCommitting') | Raised when underlying source is about to commit data read up to this point |
| [OnDisposed](TopicConsumer.OnDisposed.md 'QuixStreams.Streaming.TopicConsumer.OnDisposed') | Raised when the resource is disposed |
| [OnRevoking](TopicConsumer.OnRevoking.md 'QuixStreams.Streaming.TopicConsumer.OnRevoking') | Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point |
| [OnStreamReceived](TopicConsumer.OnStreamReceived.md 'QuixStreams.Streaming.TopicConsumer.OnStreamReceived') | Event raised when a new stream has been received for reading.<br/>Use the Stream Reader interface received to read data from the stream.<br/>You must execute 'Subscribe' method before starting to receive streams from this event |
| [OnStreamsRevoked](TopicConsumer.OnStreamsRevoked.md 'QuixStreams.Streaming.TopicConsumer.OnStreamsRevoked') | Raised when the underlying source of data became unavailable for the streams affected by it |
