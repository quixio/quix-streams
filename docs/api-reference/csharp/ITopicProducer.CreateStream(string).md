#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer')

## ITopicProducer.CreateStream(string) Method

Creates a new stream and returns the related stream producer to operate it.

```csharp
QuixStreams.Streaming.IStreamProducer CreateStream(string streamId);
```
#### Parameters

<a name='QuixStreams.Streaming.ITopicProducer.CreateStream(string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Stream Id of the created stream

#### Returns
[IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer')  
Stream producer to allow the stream to push data to the platform