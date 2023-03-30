#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## KafkaStreamingClient Class

Streaming client for kafka

```csharp
public class KafkaStreamingClient
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; KafkaStreamingClient

| Constructors | |
| :--- | :--- |
| [KafkaStreamingClient(string, SecurityOptions, IDictionary&lt;string,string&gt;, bool)](KafkaStreamingClient.KafkaStreamingClient(string,SecurityOptions,IDictionary_string,string_,bool).md 'QuixStreams.Streaming.KafkaStreamingClient.KafkaStreamingClient(string, QuixStreams.Streaming.Configuration.SecurityOptions, System.Collections.Generic.IDictionary<string,string>, bool)') | Initializes a new instance of [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient') that is capable of creating topic consumer and producers |

| Methods | |
| :--- | :--- |
| [GetRawTopicConsumer(string, string, Nullable&lt;AutoOffsetReset&gt;)](KafkaStreamingClient.GetRawTopicConsumer(string,string,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.KafkaStreamingClient.GetRawTopicConsumer(string, string, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)') | Open an topic consumer capable of subscribing to receive non-quixstreams incoming messages |
| [GetRawTopicProducer(string)](KafkaStreamingClient.GetRawTopicProducer(string).md 'QuixStreams.Streaming.KafkaStreamingClient.GetRawTopicProducer(string)') | Open an topic consumer capable of subscribing to receive non-quixstreams incoming messages |
| [GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset)](KafkaStreamingClient.GetTopicConsumer(string,string,CommitOptions,AutoOffsetReset).md 'QuixStreams.Streaming.KafkaStreamingClient.GetTopicConsumer(string, string, QuixStreams.Transport.Fw.CommitOptions, QuixStreams.Telemetry.Kafka.AutoOffsetReset)') | Open an topic consumer capable of subscribing to receive incoming streams |
| [GetTopicProducer(string)](KafkaStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.KafkaStreamingClient.GetTopicProducer(string)') | Open an topic producer capable of publishing non-quixstreams messages |
