#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient.GetRawTopicProducer(string) Method

Gets a topic producer capable of publishing non-quixstreams messages.

```csharp
public QuixStreams.Streaming.Raw.IRawTopicProducer GetRawTopicProducer(string topicIdOrName);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClient.GetRawTopicProducer(string).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

#### Returns
[IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')  
Instance of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')