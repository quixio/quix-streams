#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer')

## IStreamProducer.Close(StreamEndType) Method

Close the stream and flush the pending data to stream.

```csharp
void Close(QuixStreams.Telemetry.Models.StreamEndType streamState=QuixStreams.Telemetry.Models.StreamEndType.Closed);
```
#### Parameters

<a name='QuixStreams.Streaming.IStreamProducer.Close(QuixStreams.Telemetry.Models.StreamEndType).streamState'></a>

`streamState` [QuixStreams.Telemetry.Models.StreamEndType](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.StreamEndType 'QuixStreams.Telemetry.Models.StreamEndType')

Stream closing state