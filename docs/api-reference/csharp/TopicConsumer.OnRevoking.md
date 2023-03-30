#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[TopicConsumer](TopicConsumer.md 'QuixStreams.Streaming.TopicConsumer')

## TopicConsumer.OnRevoking Event

Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point

```csharp
public event EventHandler OnRevoking;
```

Implements [OnRevoking](ITopicConsumer.OnRevoking.md 'QuixStreams.Streaming.ITopicConsumer.OnRevoking')

#### Event Type
[System.EventHandler](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler 'System.EventHandler')