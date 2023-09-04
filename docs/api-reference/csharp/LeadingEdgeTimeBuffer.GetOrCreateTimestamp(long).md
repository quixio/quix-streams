#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[LeadingEdgeTimeBuffer](LeadingEdgeTimeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer')

## LeadingEdgeTimeBuffer.GetOrCreateTimestamp(long) Method

Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist.

```csharp
public QuixStreams.Streaming.Models.LeadingEdgeTimeRow GetOrCreateTimestamp(long timestampInNanoseconds);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.GetOrCreateTimestamp(long).timestampInNanoseconds'></a>

`timestampInNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

Timestamp in nanoseconds

#### Returns
[LeadingEdgeTimeRow](LeadingEdgeTimeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeRow')