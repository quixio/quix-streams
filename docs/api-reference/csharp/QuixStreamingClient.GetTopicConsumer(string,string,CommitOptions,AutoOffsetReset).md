#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient.GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset) Method

Gets a topic consumer capable of subscribing to receive incoming streams.

```csharp
public QuixStreams.Streaming.ITopicConsumer GetTopicConsumer(string topicIdOrName, string consumerGroup=null, QuixStreams.Kafka.Transport.CommitOptions options=null, QuixStreams.Telemetry.Kafka.AutoOffsetReset autoOffset=QuixStreams.Telemetry.Kafka.AutoOffsetReset.Latest);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClient.GetTopicConsumer(string,string,QuixStreams.Kafka.Transport.CommitOptions,QuixStreams.Telemetry.Kafka.AutoOffsetReset).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

<a name='QuixStreams.Streaming.QuixStreamingClient.GetTopicConsumer(string,string,QuixStreams.Kafka.Transport.CommitOptions,QuixStreams.Telemetry.Kafka.AutoOffsetReset).consumerGroup'></a>

`consumerGroup` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.

<a name='QuixStreams.Streaming.QuixStreamingClient.GetTopicConsumer(string,string,QuixStreams.Kafka.Transport.CommitOptions,QuixStreams.Telemetry.Kafka.AutoOffsetReset).options'></a>

`options` [QuixStreams.Kafka.Transport.CommitOptions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Kafka.Transport.CommitOptions 'QuixStreams.Kafka.Transport.CommitOptions')

The settings to use for committing

<a name='QuixStreams.Streaming.QuixStreamingClient.GetTopicConsumer(string,string,QuixStreams.Kafka.Transport.CommitOptions,QuixStreams.Telemetry.Kafka.AutoOffsetReset).autoOffset'></a>

`autoOffset` [QuixStreams.Telemetry.Kafka.AutoOffsetReset](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.AutoOffsetReset 'QuixStreams.Telemetry.Kafka.AutoOffsetReset')

The offset to use when there is no saved offset for the consumer group.

Implements [GetTopicConsumer(string, string, CommitOptions, AutoOffsetReset)](IQuixStreamingClient.GetTopicConsumer(string,string,CommitOptions,AutoOffsetReset).md 'QuixStreams.Streaming.IQuixStreamingClient.GetTopicConsumer(string, string, QuixStreams.Kafka.Transport.CommitOptions, QuixStreams.Telemetry.Kafka.AutoOffsetReset)')

#### Returns
[ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')  
Instance of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')