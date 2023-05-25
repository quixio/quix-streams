#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IKafkaStreamingClient](IKafkaStreamingClient.md 'QuixStreams.Streaming.IKafkaStreamingClient')

## IKafkaStreamingClient.GetRawTopicProducer(string) Method

Gets a topic producer capable of publishing non-quixstreams messages.

```csharp
QuixStreams.Streaming.Raw.IRawTopicProducer GetRawTopicProducer(string topic);
```
#### Parameters

<a name='QuixStreams.Streaming.IKafkaStreamingClient.GetRawTopicProducer(string).topic'></a>

`topic` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

#### Returns
[IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')  
Instance of [IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')