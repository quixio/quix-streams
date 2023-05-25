#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClientExtensions](QuixStreamingClientExtensions.md 'QuixStreams.Streaming.QuixStreamingClientExtensions')

## QuixStreamingClientExtensions.GetTopicConsumer(this IQuixStreamingClient, string, string, CommitMode, AutoOffsetReset) Method

Gets a topic consumer capable of subscribing to receive streams in the specified topic

```csharp
public static QuixStreams.Streaming.ITopicConsumer GetTopicConsumer(this QuixStreams.Streaming.IQuixStreamingClient client, string topicId, string consumerGroup=null, QuixStreams.Streaming.Models.CommitMode commitMode=QuixStreams.Streaming.Models.CommitMode.Automatic, QuixStreams.Telemetry.Kafka.AutoOffsetReset autoOffset=QuixStreams.Telemetry.Kafka.AutoOffsetReset.Latest);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IQuixStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).client'></a>

`client` [IQuixStreamingClient](IQuixStreamingClient.md 'QuixStreams.Streaming.IQuixStreamingClient')

Quix Streaming client instance

<a name='QuixStreams.Streaming.QuixStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IQuixStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).topicId'></a>

`topicId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id of the topic. Should look like: myvery-myworkspace-mytopic

<a name='QuixStreams.Streaming.QuixStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IQuixStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).consumerGroup'></a>

`consumerGroup` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.

<a name='QuixStreams.Streaming.QuixStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IQuixStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).commitMode'></a>

`commitMode` [CommitMode](CommitMode.md 'QuixStreams.Streaming.Models.CommitMode')

The commit strategy to use for this topic

<a name='QuixStreams.Streaming.QuixStreamingClientExtensions.GetTopicConsumer(thisQuixStreams.Streaming.IQuixStreamingClient,string,string,QuixStreams.Streaming.Models.CommitMode,QuixStreams.Telemetry.Kafka.AutoOffsetReset).autoOffset'></a>

`autoOffset` [QuixStreams.Telemetry.Kafka.AutoOffsetReset](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.AutoOffsetReset 'QuixStreams.Telemetry.Kafka.AutoOffsetReset')

The offset to use when there is no saved offset for the consumer group.

#### Returns
[ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')  
Instance of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')