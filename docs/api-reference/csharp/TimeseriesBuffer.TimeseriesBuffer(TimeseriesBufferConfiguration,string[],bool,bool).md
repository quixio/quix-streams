#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer(TimeseriesBufferConfiguration, string[], bool, bool) Constructor

Initializes a new instance of [TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

```csharp
internal TimeseriesBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration bufferConfiguration, string[] parametersFilter=null, bool mergeOnFlush=true, bool cleanOnFlush=true);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.TimeseriesBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[],bool,bool).bufferConfiguration'></a>

`bufferConfiguration` [TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

Configuration of the buffer

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.TimeseriesBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[],bool,bool).parametersFilter'></a>

`parametersFilter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

List of parameters to filter

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.TimeseriesBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[],bool,bool).mergeOnFlush'></a>

`mergeOnFlush` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Merge timestamps with the same timestamp and tags when releasing data from the buffer

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.TimeseriesBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration,string[],bool,bool).cleanOnFlush'></a>

`cleanOnFlush` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Clean timestamps with only null values when releasing data from the buffer