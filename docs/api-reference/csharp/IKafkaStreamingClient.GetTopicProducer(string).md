#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IKafkaStreamingClient](IKafkaStreamingClient.md 'QuixStreams.Streaming.IKafkaStreamingClient')

## IKafkaStreamingClient.GetTopicProducer(string) Method

Gets a topic producer capable of publishing stream messages.

```csharp
QuixStreams.Streaming.ITopicProducer GetTopicProducer(string topic);
```
#### Parameters

<a name='QuixStreams.Streaming.IKafkaStreamingClient.GetTopicProducer(string).topic'></a>

`topic` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

#### Returns
[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')  
Instance of [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')