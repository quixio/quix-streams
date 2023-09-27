#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[LeadingEdgeRow](LeadingEdgeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeRow')

## LeadingEdgeRow.AddValue(string, byte[], bool) Method

Adds a value to the row

```csharp
public QuixStreams.Streaming.Models.LeadingEdgeRow AddValue(string parameter, byte[] value, bool overwrite=false);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.LeadingEdgeRow.AddValue(string,byte[],bool).parameter'></a>

`parameter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Parameter name

<a name='QuixStreams.Streaming.Models.LeadingEdgeRow.AddValue(string,byte[],bool).value'></a>

`value` [System.Byte](https://docs.microsoft.com/en-us/dotnet/api/System.Byte 'System.Byte')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

Value of the parameter

<a name='QuixStreams.Streaming.Models.LeadingEdgeRow.AddValue(string,byte[],bool).overwrite'></a>

`overwrite` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

If set to true, it will overwrite an existing value for the specified parameter if one already exists.  
            If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current LeadingEdgeTimestamp instance.

#### Returns
[LeadingEdgeRow](LeadingEdgeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeRow')