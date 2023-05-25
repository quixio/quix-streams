#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[KafkaStreamingClientExtensions](KafkaStreamingClientExtensions.md 'QuixStreams.Streaming.KafkaStreamingClientExtensions')

## KafkaStreamingClientExtensions.GetTopicConsumer(this IKafkaStreamingClient, string, string, CommitMode, AutoOffsetReset) Method

Open an topic consumer capable of subscribing to receive incoming streams

```csharp
public static QuixStreams.Streaming.ITopicConsumer GetTopicConsumer(this QuixStreams.Streaming.IKafkaStreamingClient client, string topic, string consumerGroup=null, QuixStreams.Streaming.Models.CommitMode commitMode=QuixStreams.Streaming.Models.CommitMode.Automatic, QuixStreams.Telemetry.Kafka.AutoOffsetReset autoOffset=QuixStreams.Telemetry.Kafka.AutoOffsetReset.Latest);
```
#### Parameters

<a name='QuixStreams.Streaming.KafkaStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IKafkaStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).client'></a>

`client` [IKafkaStreamingClient](IKafkaStreamingClient.md 'QuixStreams.Streaming.IKafkaStreamingClient')

Streaming Client instance

<a name='QuixStreams.Streaming.KafkaStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IKafkaStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).topic'></a>

`topic` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

<a name='QuixStreams.Streaming.KafkaStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IKafkaStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).consumerGroup'></a>

`consumerGroup` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.

<a name='QuixStreams.Streaming.KafkaStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IKafkaStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).commitMode'></a>

`commitMode` [CommitMode](CommitMode.md 'QuixStreams.Streaming.Models.CommitMode')

The commit strategy to use for this topic

<a name='QuixStreams.Streaming.KafkaStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IKafkaStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).autoOffset'></a>

`autoOffset` [QuixStreams.Telemetry.Kafka.AutoOffsetReset](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.AutoOffsetReset 'QuixStreams.Telemetry.Kafka.AutoOffsetReset')

The offset to use when there is no saved offset for the consumer group.

#### Returns
[ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')  
Instance of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')