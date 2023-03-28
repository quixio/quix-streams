#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamProducerInternal](IStreamProducerInternal.md 'QuixStreams.Streaming.IStreamProducerInternal')

## IStreamProducerInternal.Publish(ParameterDefinitions) Method

Write the optional Parameter definition properties describing the hierarchical grouping of parameters  
Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.

```csharp
void Publish(QuixStreams.Telemetry.Models.ParameterDefinitions definitions);
```
#### Parameters

<a name='QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.ParameterDefinitions).definitions'></a>

`definitions` [QuixStreams.Telemetry.Models.ParameterDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.ParameterDefinitions 'QuixStreams.Telemetry.Models.ParameterDefinitions')