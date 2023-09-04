#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## KafkaStreamingClient Class

A Kafka streaming client capable of creating topic consumer and producers.

```csharp
public class KafkaStreamingClient :
QuixStreams.Streaming.IKafkaStreamingClient
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; KafkaStreamingClient

Implements [IKafkaStreamingClient](IKafkaStreamingClient.md 'QuixStreams.Streaming.IKafkaStreamingClient')

| Constructors | |
| :--- | :--- |
| [KafkaStreamingClient(string, SecurityOptions, IDictionary&lt;string,string&gt;, bool)](KafkaStreamingClient.KafkaStreamingClient(string,SecurityOptions,IDictionary_string,string_,bool).md 'QuixStreams.Streaming.KafkaStreamingClient.KafkaStreamingClient(string, QuixStreams.Streaming.Configuration.SecurityOptions, System.Collections.Generic.IDictionary<string,string>, bool)') | Initializes a new instance of [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient') |

| Methods | |
| :--- | :--- |
| [GetRawTopicConsumer(string, string, Nullable&lt;AutoOffsetReset&gt;)](KafkaStreamingClient.GetRawTopicConsumer(string,string,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.KafkaStreamingClient.GetRawTopicConsumer(string, string, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)') | Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages. |
| [GetRawTopicProducer(string)](KafkaStreamingClient.GetRawTopicProducer(string).md 'QuixStreams.Streaming.KafkaStreamingClient.GetRawTopicProducer(string)') | Gets a topic producer capable of publishing non-quixstreams messages. |
| [GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset)](KafkaStreamingClient.GetTopicConsumer(string,string,CommitOptions,AutoOffsetReset).md 'QuixStreams.Streaming.KafkaStreamingClient.GetTopicConsumer(string, string, QuixStreams.Kafka.Transport.CommitOptions, QuixStreams.Telemetry.Kafka.AutoOffsetReset)') | Gets a topic consumer capable of subscribing to receive incoming streams. |
| [GetTopicProducer(string)](KafkaStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.KafkaStreamingClient.GetTopicProducer(string)') | Gets a topic producer capable of publishing stream messages. |
