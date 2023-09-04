#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw').[RawTopicConsumer](RawTopicConsumer.md 'QuixStreams.Streaming.Raw.RawTopicConsumer')

## RawTopicConsumer.OnMessageReceived Event

Event raised when a message is received from the topic

```csharp
public event EventHandler<KafkaMessage> OnMessageReceived;
```

Implements [OnMessageReceived](IRawTopicConsumer.OnMessageReceived.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer.OnMessageReceived')

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[QuixStreams.Kafka.KafkaMessage](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Kafka.KafkaMessage 'QuixStreams.Kafka.KafkaMessage')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')