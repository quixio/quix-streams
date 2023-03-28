#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[StreamConsumer](StreamConsumer.md 'QuixStreams.Streaming.StreamConsumer')

## StreamConsumer(ITopicConsumer, string) Constructor

Initializes a new instance of [StreamConsumer](StreamConsumer.md 'QuixStreams.Streaming.StreamConsumer')  
This constructor is called internally by the [QuixStreams.Telemetry.StreamPipelineFactory](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.StreamPipelineFactory 'QuixStreams.Telemetry.StreamPipelineFactory')

```csharp
internal StreamConsumer(QuixStreams.Streaming.ITopicConsumer topicConsumer, string streamId);
```
#### Parameters

<a name='QuixStreams.Streaming.StreamConsumer.StreamConsumer(QuixStreams.Streaming.ITopicConsumer,string).topicConsumer'></a>

`topicConsumer` [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

The topic the reader belongs to

<a name='QuixStreams.Streaming.StreamConsumer.StreamConsumer(QuixStreams.Streaming.ITopicConsumer,string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Stream Id of the source that has generated this Stream Consumer.   
            Commonly the Stream Id will be coming from the protocol.   
            If no stream Id is passed, like when a new stream is created for producing data, a Guid is generated automatically.