#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IQuixStreamingClient](IQuixStreamingClient.md 'QuixStreams.Streaming.IQuixStreamingClient')

## IQuixStreamingClient.GetRawTopicProducer(string) Method

Gets a topic producer capable of publishing non-quixstreams messages.

```csharp
QuixStreams.Streaming.Raw.IRawTopicProducer GetRawTopicProducer(string topicIdOrName);
```
#### Parameters

<a name='QuixStreams.Streaming.IQuixStreamingClient.GetRawTopicProducer(string).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

#### Returns
[IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')  
Instance of [IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer')