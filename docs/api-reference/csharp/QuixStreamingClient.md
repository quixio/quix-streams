#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## QuixStreamingClient Class

Streaming client for Kafka configured automatically using Environment Variables and Quix platform endpoints.  
Use this Client when you use this library together with Quix platform.

```csharp
public class QuixStreamingClient :
QuixStreams.Streaming.IQuixStreamingClient
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; QuixStreamingClient

Implements [IQuixStreamingClient](IQuixStreamingClient.md 'QuixStreams.Streaming.IQuixStreamingClient')

| Constructors | |
| :--- | :--- |
| [QuixStreamingClient(string, bool, IDictionary&lt;string,string&gt;, bool, HttpClient)](QuixStreamingClient.QuixStreamingClient(string,bool,IDictionary_string,string_,bool,HttpClient).md 'QuixStreams.Streaming.QuixStreamingClient.QuixStreamingClient(string, bool, System.Collections.Generic.IDictionary<string,string>, bool, System.Net.Http.HttpClient)') | Initializes a new instance of [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient') that is capable of creating topic consumer and producers |

| Fields | |
| :--- | :--- |
| [ApiUrl](QuixStreamingClient.ApiUrl.md 'QuixStreams.Streaming.QuixStreamingClient.ApiUrl') | The base API uri. Defaults to `https://portal-api.platform.quix.ai`, or environment variable `Quix__Portal__Api` if available. |
| [CachePeriod](QuixStreamingClient.CachePeriod.md 'QuixStreams.Streaming.QuixStreamingClient.CachePeriod') | The period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute. |

| Properties | |
| :--- | :--- |
| [TokenValidationConfig](QuixStreamingClient.TokenValidationConfig.md 'QuixStreams.Streaming.QuixStreamingClient.TokenValidationConfig') | Gets or sets the token validation configuration |

| Methods | |
| :--- | :--- |
| [GetRawTopicConsumer(string, string, Nullable&lt;AutoOffsetReset&gt;)](QuixStreamingClient.GetRawTopicConsumer(string,string,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.QuixStreamingClient.GetRawTopicConsumer(string, string, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)') | Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages. |
| [GetRawTopicProducer(string)](QuixStreamingClient.GetRawTopicProducer(string).md 'QuixStreams.Streaming.QuixStreamingClient.GetRawTopicProducer(string)') | Gets a topic producer capable of publishing non-quixstreams messages. |
| [GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset)](QuixStreamingClient.GetTopicConsumer(string,string,CommitOptions,AutoOffsetReset).md 'QuixStreams.Streaming.QuixStreamingClient.GetTopicConsumer(string, string, QuixStreams.Kafka.Transport.CommitOptions, QuixStreams.Telemetry.Kafka.AutoOffsetReset)') | Gets a topic consumer capable of subscribing to receive incoming streams. |
| [GetTopicProducer(string)](QuixStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.QuixStreamingClient.GetTopicProducer(string)') | Gets a topic producer capable of publishing stream messages. |
