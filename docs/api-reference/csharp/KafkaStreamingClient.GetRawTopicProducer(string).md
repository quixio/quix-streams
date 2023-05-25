#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient')

## KafkaStreamingClient.GetRawTopicProducer(string) Method

Gets a topic producer capable of publishing non-quixstreams messages.

```csharp
public QuixStreams.Streaming.Raw.IRawTopicProducer GetRawTopicProducer(string topic);
```
#### Parameters

<a name='QuixStreams.Streaming.KafkaStreamingClient.GetRawTopicProducer(string).topic'></a>

`topic` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

Implements [GetRawTopicProducer(string)](IKafkaStreamingClient.GetRawTopicProducer(string).md 'QuixStreams.Streaming.IKafkaStreamingClient.GetRawTopicProducer(string)')

#### Returns
[IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')  
Instance of [IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')