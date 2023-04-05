#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[TopicConsumer](TopicConsumer.md 'QuixStreams.Streaming.TopicConsumer')

## TopicConsumer.OnStreamsRevoked Event

Raised when the underlying source of data became unavailable for the streams affected by it

```csharp
public event EventHandler<IStreamConsumer[]> OnStreamsRevoked;
```

Implements [OnStreamsRevoked](ITopicConsumer.OnStreamsRevoked.md 'QuixStreams.Streaming.ITopicConsumer.OnStreamsRevoked')

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')