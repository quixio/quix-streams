#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient.GetTopicProducer(string) Method

Gets a topic producer capable of publishing stream messages.

```csharp
public QuixStreams.Streaming.ITopicProducer GetTopicProducer(string topicIdOrName);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClient.GetTopicProducer(string).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

Implements [GetTopicProducer(string)](IQuixStreamingClient.GetTopicProducer(string).md 'QuixStreams.Streaming.IQuixStreamingClient.GetTopicProducer(string)')

#### Returns
[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')  
Instance of [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')