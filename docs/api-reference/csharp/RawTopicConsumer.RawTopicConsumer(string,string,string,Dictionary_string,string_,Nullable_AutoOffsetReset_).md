#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw').[RawTopicConsumer](RawTopicConsumer.md 'QuixStreams.Streaming.Raw.RawTopicConsumer')

## RawTopicConsumer(string, string, string, Dictionary<string,string>, Nullable<AutoOffsetReset>) Constructor

Initializes a new instance of [RawTopicConsumer](RawTopicConsumer.md 'QuixStreams.Streaming.Raw.RawTopicConsumer')

```csharp
public RawTopicConsumer(string brokerAddress, string topicName, string consumerGroup, System.Collections.Generic.Dictionary<string,string> brokerProperties=null, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset> autoOffset=null);
```
#### Parameters

<a name='QuixStreams.Streaming.Raw.RawTopicConsumer.RawTopicConsumer(string,string,string,System.Collections.Generic.Dictionary_string,string_,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).brokerAddress'></a>

`brokerAddress` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Address of Kafka cluster.

<a name='QuixStreams.Streaming.Raw.RawTopicConsumer.RawTopicConsumer(string,string,string,System.Collections.Generic.Dictionary_string,string_,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).topicName'></a>

`topicName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

<a name='QuixStreams.Streaming.Raw.RawTopicConsumer.RawTopicConsumer(string,string,string,System.Collections.Generic.Dictionary_string,string_,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).consumerGroup'></a>

`consumerGroup` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.

<a name='QuixStreams.Streaming.Raw.RawTopicConsumer.RawTopicConsumer(string,string,string,System.Collections.Generic.Dictionary_string,string_,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).brokerProperties'></a>

`brokerProperties` [System.Collections.Generic.Dictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')

Additional broker properties

<a name='QuixStreams.Streaming.Raw.RawTopicConsumer.RawTopicConsumer(string,string,string,System.Collections.Generic.Dictionary_string,string_,System.Nullable_QuixStreams.Telemetry.Kafka.AutoOffsetReset_).autoOffset'></a>

`autoOffset` [System.Nullable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')[QuixStreams.Telemetry.Kafka.AutoOffsetReset](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.AutoOffsetReset 'QuixStreams.Telemetry.Kafka.AutoOffsetReset')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')

The offset to use when there is no saved offset for the consumer group.