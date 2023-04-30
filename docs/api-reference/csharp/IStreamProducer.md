#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## IStreamProducer Interface

Stands for a new stream that we want to send to the platform.  
It provides you helper properties to stream data the platform like parameter values, events, definitions and all the information you can persist to the platform.

```csharp
public interface IStreamProducer :
System.IDisposable
```

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Properties | |
| :--- | :--- |
| [Epoch](IStreamProducer.Epoch.md 'QuixStreams.Streaming.IStreamProducer.Epoch') | Default Epoch used for Parameters and Events |
| [Events](IStreamProducer.Events.md 'QuixStreams.Streaming.IStreamProducer.Events') | Gets the producer for publishing event related information of the stream such as event definitions and values |
| [Properties](IStreamProducer.Properties.md 'QuixStreams.Streaming.IStreamProducer.Properties') | Properties of the stream. The changes will automatically be sent after a slight delay |
| [StreamId](IStreamProducer.StreamId.md 'QuixStreams.Streaming.IStreamProducer.StreamId') | Stream Id of the new stream created by the producer |
| [Timeseries](IStreamProducer.Timeseries.md 'QuixStreams.Streaming.IStreamProducer.Timeseries') | Gets the producer for publishing timeseries related information of the stream such as parameter definitions and values |

| Methods | |
| :--- | :--- |
| [Close(StreamEndType)](IStreamProducer.Close(StreamEndType).md 'QuixStreams.Streaming.IStreamProducer.Close(QuixStreams.Telemetry.Models.StreamEndType)') | Close the stream and flush the pending data to stream. |
| [Flush()](IStreamProducer.Flush().md 'QuixStreams.Streaming.IStreamProducer.Flush()') | Flush the pending data to stream. |

| Events | |
| :--- | :--- |
| [OnWriteException](IStreamProducer.OnWriteException.md 'QuixStreams.Streaming.IStreamProducer.OnWriteException') | Event raised when an exception occurred during the publishing processes |
