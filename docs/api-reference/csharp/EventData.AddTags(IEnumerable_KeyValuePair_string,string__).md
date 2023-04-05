#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[EventData](EventData.md 'QuixStreams.Streaming.Models.EventData')

## EventData.AddTags(IEnumerable<KeyValuePair<string,string>>) Method

Copies the tags from the specified dictionary.  
Conflicting tags will be overwritten

```csharp
public QuixStreams.Streaming.Models.EventData AddTags(System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string,string>> tags);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.EventData.AddTags(System.Collections.Generic.IEnumerable_System.Collections.Generic.KeyValuePair_string,string__).tags'></a>

`tags` [System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[System.Collections.Generic.KeyValuePair&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')

The tags to copy

#### Returns
[EventData](EventData.md 'QuixStreams.Streaming.Models.EventData')  
This instance