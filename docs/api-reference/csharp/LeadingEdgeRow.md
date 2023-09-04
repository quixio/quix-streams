#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## LeadingEdgeRow Class

Represents a single row of data in the [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer')

```csharp
public class LeadingEdgeRow
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; LeadingEdgeRow

Derived  
&#8627; [LeadingEdgeTimeRow](LeadingEdgeTimeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeRow')

| Fields | |
| :--- | :--- |
| [EpochIncluded](LeadingEdgeRow.EpochIncluded.md 'QuixStreams.Streaming.Models.LeadingEdgeRow.EpochIncluded') | Whether an epoch is included in the timestamp |

| Properties | |
| :--- | :--- |
| [Timestamp](LeadingEdgeRow.Timestamp.md 'QuixStreams.Streaming.Models.LeadingEdgeRow.Timestamp') | The timestamp in nanoseconds |

| Methods | |
| :--- | :--- |
| [AddValue(string, byte[], bool)](LeadingEdgeRow.AddValue(string,byte[],bool).md 'QuixStreams.Streaming.Models.LeadingEdgeRow.AddValue(string, byte[], bool)') | Adds a value to the row |
| [AddValue(string, double, bool)](LeadingEdgeRow.AddValue(string,double,bool).md 'QuixStreams.Streaming.Models.LeadingEdgeRow.AddValue(string, double, bool)') | Adds a value to the row |
| [AddValue(string, string, bool)](LeadingEdgeRow.AddValue(string,string,bool).md 'QuixStreams.Streaming.Models.LeadingEdgeRow.AddValue(string, string, bool)') | Adds a value to the row |
