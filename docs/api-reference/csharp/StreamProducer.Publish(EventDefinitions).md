#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[StreamProducer](StreamProducer.md 'QuixStreams.Streaming.StreamProducer')

## StreamProducer.Publish(EventDefinitions) Method

Write the optional Event definition properties describing the hierarchical grouping of events  
Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.

```csharp
public void Publish(QuixStreams.Telemetry.Models.EventDefinitions definitions);
```
#### Parameters

<a name='QuixStreams.Streaming.StreamProducer.Publish(QuixStreams.Telemetry.Models.EventDefinitions).definitions'></a>

`definitions` [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions')