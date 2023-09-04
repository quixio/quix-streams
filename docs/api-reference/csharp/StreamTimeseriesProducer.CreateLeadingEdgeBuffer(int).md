#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer')

## StreamTimeseriesProducer.CreateLeadingEdgeBuffer(int) Method

Creates a new [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') using this producer where tags form part of the row's key  
and can't be modified after initial values

```csharp
public QuixStreams.Streaming.Models.LeadingEdgeBuffer CreateLeadingEdgeBuffer(int leadingEdgeDelayMs);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer.CreateLeadingEdgeBuffer(int).leadingEdgeDelayMs'></a>

`leadingEdgeDelayMs` [System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')

Leading edge delay configuration in Milliseconds

#### Returns
[LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer')