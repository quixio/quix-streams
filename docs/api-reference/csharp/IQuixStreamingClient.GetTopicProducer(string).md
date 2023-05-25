#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IQuixStreamingClient](IQuixStreamingClient.md 'QuixStreams.Streaming.IQuixStreamingClient')

## IQuixStreamingClient.GetTopicProducer(string) Method

Gets a topic producer capable of publishing stream messages.

```csharp
QuixStreams.Streaming.ITopicProducer GetTopicProducer(string topicIdOrName);
```
#### Parameters

<a name='QuixStreams.Streaming.IQuixStreamingClient.GetTopicProducer(string).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

#### Returns
[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')  
Instance of [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')