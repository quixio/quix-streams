#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

## ITopicConsumer.OnStreamsRevoked Event

Raised when the underlying source of data became unavailable for the streams affected by it

```csharp
event EventHandler<IStreamConsumer[]> OnStreamsRevoked;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')