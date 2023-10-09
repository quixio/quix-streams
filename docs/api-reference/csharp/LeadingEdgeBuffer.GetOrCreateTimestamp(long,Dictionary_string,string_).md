#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer')

## LeadingEdgeBuffer.GetOrCreateTimestamp(long, Dictionary<string,string>) Method

Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist.

```csharp
public QuixStreams.Streaming.Models.LeadingEdgeRow GetOrCreateTimestamp(long timestampInNanoseconds, System.Collections.Generic.Dictionary<string,string> tags=null);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.LeadingEdgeBuffer.GetOrCreateTimestamp(long,System.Collections.Generic.Dictionary_string,string_).timestampInNanoseconds'></a>

`timestampInNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

Timestamp in nanoseconds

<a name='QuixStreams.Streaming.Models.LeadingEdgeBuffer.GetOrCreateTimestamp(long,System.Collections.Generic.Dictionary_string,string_).tags'></a>

`tags` [System.Collections.Generic.Dictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')

Optional Tags

#### Returns
[LeadingEdgeRow](LeadingEdgeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeRow')