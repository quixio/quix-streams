#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## StreamPropertiesProducer Class

Properties and Metadata of the stream.  
All the changes of these properties will be streamed and persisted to the platform.

```csharp
public class StreamPropertiesProducer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamPropertiesProducer

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [StreamPropertiesProducer(IStreamProducerInternal)](StreamPropertiesProducer.StreamPropertiesProducer(IStreamProducerInternal).md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer(QuixStreams.Streaming.IStreamProducerInternal)') | Initializes a new instance of [StreamPropertiesProducer](StreamPropertiesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer') |

| Properties | |
| :--- | :--- |
| [FlushInterval](StreamPropertiesProducer.FlushInterval.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.FlushInterval') | Automatic flush interval of the properties metadata into the channel [ in milliseconds ] |
| [Location](StreamPropertiesProducer.Location.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.Location') | Specify location of the stream in data catalogue. <br/>For example: /cars/ai/carA/. |
| [Metadata](StreamPropertiesProducer.Metadata.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.Metadata') | Metadata of the stream. |
| [Name](StreamPropertiesProducer.Name.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.Name') | Name of the stream. |
| [Parents](StreamPropertiesProducer.Parents.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.Parents') | List of Stream Ids of the Parent streams |
| [TimeOfRecording](StreamPropertiesProducer.TimeOfRecording.md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.TimeOfRecording') | Date Time of recording of the stream. Commonly set to Datetime.UtcNow. |

| Methods | |
| :--- | :--- |
| [AddParent(string)](StreamPropertiesProducer.AddParent(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.AddParent(string)') | Adds a parent stream. |
| [CheckForHeartbeatFlush()](StreamPropertiesProducer.CheckForHeartbeatFlush().md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.CheckForHeartbeatFlush()') | Metadata of the stream. |
| [Dispose()](StreamPropertiesProducer.Dispose().md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.Dispose()') | Flushes internal buffers and disposes |
| [Flush()](StreamPropertiesProducer.Flush().md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.Flush()') | Immediately writes the properties yet to be sent instead of waiting for the flush timer (20ms) |
| [RemoveParent(string)](StreamPropertiesProducer.RemoveParent(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamPropertiesProducer.RemoveParent(string)') | Removes a parent stream |
