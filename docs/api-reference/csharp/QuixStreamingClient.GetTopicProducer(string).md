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

#### Returns
[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')  
Instance of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')