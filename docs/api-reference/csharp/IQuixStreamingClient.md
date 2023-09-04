#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## IQuixStreamingClient Interface

```csharp
public interface IQuixStreamingClient
```

Derived  
&#8627; [QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

| Methods | |
| :--- | :--- |
| [GetRawTopicConsumer(string, string, Nullable&lt;AutoOffsetReset&gt;)](IQuixStreamingClient.GetRawTopicConsumer(string,string,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.IQuixStreamingClient.GetRawTopicConsumer(string, string, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)') | Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages. |
| [GetRawTopicProducer(string)](IQuixStreamingClient.GetRawTopicProducer(string).md 'QuixStreams.Streaming.IQuixStreamingClient.GetRawTopicProducer(string)') | Gets a topic producer capable of publishing non-quixstreams messages. |
| [GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset)](IQuixStreamingClient.GetTopicConsumer(string,string,CommitOptions,AutoOffsetReset).md 'QuixStreams.Streaming.IQuixStreamingClient.GetTopicConsumer(string, string, QuixStreams.Kafka.Transport.CommitOptions, QuixStreams.Telemetry.Kafka.AutoOffsetReset)') | Gets a topic consumer capable of subscribing to receive incoming streams. |
| [GetTopicProducer(string)](IQuixStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.IQuixStreamingClient.GetTopicProducer(string)') | Gets a topic producer capable of publishing stream messages. |
