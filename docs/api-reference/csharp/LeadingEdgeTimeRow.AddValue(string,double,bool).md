#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[LeadingEdgeTimeRow](LeadingEdgeTimeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeRow')

## LeadingEdgeTimeRow.AddValue(string, double, bool) Method

Adds a value to the row

```csharp
public QuixStreams.Streaming.Models.LeadingEdgeTimeRow AddValue(string parameter, double value, bool overwrite=false);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.LeadingEdgeTimeRow.AddValue(string,double,bool).parameter'></a>

`parameter` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Parameter name

<a name='QuixStreams.Streaming.Models.LeadingEdgeTimeRow.AddValue(string,double,bool).value'></a>

`value` [System.Double](https://docs.microsoft.com/en-us/dotnet/api/System.Double 'System.Double')

Value of the parameter

<a name='QuixStreams.Streaming.Models.LeadingEdgeTimeRow.AddValue(string,double,bool).overwrite'></a>

`overwrite` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

If set to true, it will overwrite an existing value for the specified parameter if one already exists.  
            If set to false and a value for the specified parameter already exists, the method ignore the new value and just return the current TimeseriesDataRow instance.

#### Returns
[LeadingEdgeTimeRow](LeadingEdgeTimeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeRow')  
Returns the current LeadingEdgeTimestamp instance. This allows for method chaining