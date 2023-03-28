#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamProducerInternal](IStreamProducerInternal.md 'QuixStreams.Streaming.IStreamProducerInternal')

## IStreamProducerInternal.Publish(EventDefinitions) Method

Write the optional Event definition properties describing the hierarchical grouping of events  
Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.

```csharp
void Publish(QuixStreams.Telemetry.Models.EventDefinitions definitions);
```
#### Parameters

<a name='QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.EventDefinitions).definitions'></a>

`definitions` [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions')