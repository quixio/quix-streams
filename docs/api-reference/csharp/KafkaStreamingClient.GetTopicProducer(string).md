#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient')

## KafkaStreamingClient.GetTopicProducer(string) Method

Gets a topic producer capable of publishing stream messages.

```csharp
public QuixStreams.Streaming.ITopicProducer GetTopicProducer(string topic);
```
#### Parameters

<a name='QuixStreams.Streaming.KafkaStreamingClient.GetTopicProducer(string).topic'></a>

`topic` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

Implements [GetTopicProducer(string)](IKafkaStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.IKafkaStreamingClient.GetTopicProducer(string)')

#### Returns
[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')  
Instance of [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')