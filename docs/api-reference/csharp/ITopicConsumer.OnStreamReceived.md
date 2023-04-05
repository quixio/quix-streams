#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

## ITopicConsumer.OnStreamReceived Event

Event raised when a new stream has been received for reading.  
Use the Stream Reader interface received to read data from the stream.  
You must execute 'Subscribe' method before starting to receive streams from this event

```csharp
event EventHandler<IStreamConsumer> OnStreamReceived;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')