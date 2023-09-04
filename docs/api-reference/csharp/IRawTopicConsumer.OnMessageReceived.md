#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw').[IRawTopicConsumer](IRawTopicConsumer.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer')

## IRawTopicConsumer.OnMessageReceived Event

Event raised when a message is received from the topic

```csharp
event EventHandler<KafkaMessage> OnMessageReceived;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[QuixStreams.Kafka.KafkaMessage](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Kafka.KafkaMessage 'QuixStreams.Kafka.KafkaMessage')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')