#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## EventData Class

Represents a single point in time with event value and tags attached to it

```csharp
public class EventData
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; EventData

| Constructors | |
| :--- | :--- |
| [EventData(string, DateTime, string)](EventData.EventData(string,DateTime,string).md 'QuixStreams.Streaming.Models.EventData.EventData(string, System.DateTime, string)') | Create a new empty Event Data instance |
| [EventData(string, TimeSpan, string)](EventData.EventData(string,TimeSpan,string).md 'QuixStreams.Streaming.Models.EventData.EventData(string, System.TimeSpan, string)') | Create a new empty Event Data instance |
| [EventData(string, long, string)](EventData.EventData(string,long,string).md 'QuixStreams.Streaming.Models.EventData.EventData(string, long, string)') | Create a new empty Event Data instance |

| Properties | |
| :--- | :--- |
| [Id](EventData.Id.md 'QuixStreams.Streaming.Models.EventData.Id') | The globally unique identifier of the event |
| [Tags](EventData.Tags.md 'QuixStreams.Streaming.Models.EventData.Tags') | Tags for the timestamp. If key is not found return null |
| [Timestamp](EventData.Timestamp.md 'QuixStreams.Streaming.Models.EventData.Timestamp') | Gets the timestamp in [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime') format |
| [TimestampAsTimeSpan](EventData.TimestampAsTimeSpan.md 'QuixStreams.Streaming.Models.EventData.TimestampAsTimeSpan') | Gets the timestamp in [System.TimeSpan](https://docs.microsoft.com/en-us/dotnet/api/System.TimeSpan 'System.TimeSpan') format |
| [TimestampMilliseconds](EventData.TimestampMilliseconds.md 'QuixStreams.Streaming.Models.EventData.TimestampMilliseconds') | Gets the timestamp in milliseconds |
| [TimestampNanoseconds](EventData.TimestampNanoseconds.md 'QuixStreams.Streaming.Models.EventData.TimestampNanoseconds') | Gets the timestamp in nanoseconds |
| [Value](EventData.Value.md 'QuixStreams.Streaming.Models.EventData.Value') | The value of the event |

| Methods | |
| :--- | :--- |
| [AddTag(string, string)](EventData.AddTag(string,string).md 'QuixStreams.Streaming.Models.EventData.AddTag(string, string)') | Add a new Tag to the event |
| [AddTags(IEnumerable&lt;KeyValuePair&lt;string,string&gt;&gt;)](EventData.AddTags(IEnumerable_KeyValuePair_string,string__).md 'QuixStreams.Streaming.Models.EventData.AddTags(System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string,string>>)') | Copies the tags from the specified dictionary.<br/>Conflicting tags will be overwritten |
| [Clone()](EventData.Clone().md 'QuixStreams.Streaming.Models.EventData.Clone()') | Clones the [EventData](EventData.md 'QuixStreams.Streaming.Models.EventData') |
| [RemoveTag(string)](EventData.RemoveTag(string).md 'QuixStreams.Streaming.Models.EventData.RemoveTag(string)') | Remove a Tag from the event |
