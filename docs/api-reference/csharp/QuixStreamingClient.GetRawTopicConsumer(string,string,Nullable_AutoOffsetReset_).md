#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient.GetRawTopicConsumer(string, string, Nullable<AutoOffsetReset>) Method

Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages.

```csharp
public QuixStreams.Streaming.Raw.IRawTopicConsumer GetRawTopicConsumer(string topicIdOrName, string consumerGroup=null, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset> autoOffset=null);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClient.GetRawTopicConsumer(string,string,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

<a name='QuixStreams.Streaming.QuixStreamingClient.GetRawTopicConsumer(string,string,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).consumerGroup'></a>

`consumerGroup` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.

<a name='QuixStreams.Streaming.QuixStreamingClient.GetRawTopicConsumer(string,string,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).autoOffset'></a>

`autoOffset` [System.Nullable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')[QuixStreams.Telemetry.Kafka.AutoOffsetReset](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.AutoOffsetReset 'QuixStreams.Telemetry.Kafka.AutoOffsetReset')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')

The offset to use when there is no saved offset for the consumer group.

Implements [GetRawTopicConsumer(string, string, Nullable&lt;AutoOffsetReset&gt;)](IQuixStreamingClient.GetRawTopicConsumer(string,string,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.IQuixStreamingClient.GetRawTopicConsumer(string, string, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)')

#### Returns
[IRawTopicConsumer](IRawTopicConsumer.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer')  
Instance of [IRawTopicConsumer](IRawTopicConsumer.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer')