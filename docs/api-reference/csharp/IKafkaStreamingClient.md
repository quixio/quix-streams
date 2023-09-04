#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## IKafkaStreamingClient Interface

```csharp
public interface IKafkaStreamingClient
```

Derived  
&#8627; [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient')

| Methods | |
| :--- | :--- |
| [GetRawTopicConsumer(string, string, Nullable&lt;AutoOffsetReset&gt;)](IKafkaStreamingClient.GetRawTopicConsumer(string,string,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.IKafkaStreamingClient.GetRawTopicConsumer(string, string, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)') | Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages. |
| [GetRawTopicProducer(string)](IKafkaStreamingClient.GetRawTopicProducer(string).md 'QuixStreams.Streaming.IKafkaStreamingClient.GetRawTopicProducer(string)') | Gets a topic producer capable of publishing non-quixstreams messages. |
| [GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset)](IKafkaStreamingClient.GetTopicConsumer(string,string,CommitOptions,AutoOffsetReset).md 'QuixStreams.Streaming.IKafkaStreamingClient.GetTopicConsumer(string, string, QuixStreams.Kafka.Transport.CommitOptions, QuixStreams.Telemetry.Kafka.AutoOffsetReset)') | Gets a topic consumer capable of subscribing to receive incoming streams. |
| [GetTopicProducer(string)](IKafkaStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.IKafkaStreamingClient.GetTopicProducer(string)') | Gets a topic producer capable of publishing stream messages. |
