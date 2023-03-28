#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

## ITopicConsumer.OnRevoking Event

Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point

```csharp
event EventHandler OnRevoking;
```

#### Event Type
[System.EventHandler](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler 'System.EventHandler')