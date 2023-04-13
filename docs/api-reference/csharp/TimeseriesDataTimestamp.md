#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## TimeseriesDataTimestamp Struct

Represents a single point in time with parameter values and tags attached to that time

```csharp
public readonly struct TimeseriesDataTimestamp
```

| Properties | |
| :--- | :--- |
| [Parameters](TimeseriesDataTimestamp.Parameters.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.Parameters') | Parameter values for the timestamp. When a key is not found, returns empty [ParameterValue](ParameterValue.md 'QuixStreams.Streaming.Models.ParameterValue') |
| [Tags](TimeseriesDataTimestamp.Tags.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.Tags') | Tags for the timestamp. When key is not found, returns null |
| [Timestamp](TimeseriesDataTimestamp.Timestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.Timestamp') | Gets the timestamp in [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime') format |
| [TimestampAsTimeSpan](TimeseriesDataTimestamp.TimestampAsTimeSpan.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.TimestampAsTimeSpan') | Gets the timestamp in [System.TimeSpan](https://docs.microsoft.com/en-us/dotnet/api/System.TimeSpan 'System.TimeSpan') format |
| [TimestampMilliseconds](TimeseriesDataTimestamp.TimestampMilliseconds.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.TimestampMilliseconds') | Gets the timestamp in milliseconds |
| [TimestampNanoseconds](TimeseriesDataTimestamp.TimestampNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.TimestampNanoseconds') | Gets the timestamp in nanoseconds |

| Methods | |
| :--- | :--- |
| [AddTag(string, string)](TimeseriesDataTimestamp.AddTag(string,string).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.AddTag(string, string)') | Adds a tag to the values |
| [AddTags(IEnumerable&lt;KeyValuePair&lt;string,string&gt;&gt;)](TimeseriesDataTimestamp.AddTags(IEnumerable_KeyValuePair_string,string__).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.AddTags(System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string,string>>)') | Copies the tags from the specified dictionary.<br/>Conflicting tags will be overwritten |
| [AddValue(string, ParameterValue)](TimeseriesDataTimestamp.AddValue(string,ParameterValue).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.AddValue(string, QuixStreams.Streaming.Models.ParameterValue)') | Adds a new value. |
| [AddValue(string, byte[])](TimeseriesDataTimestamp.AddValue(string,byte[]).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.AddValue(string, byte[])') | Adds a new binary value. |
| [AddValue(string, double)](TimeseriesDataTimestamp.AddValue(string,double).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.AddValue(string, double)') | Adds a new numeric value. |
| [AddValue(string, string)](TimeseriesDataTimestamp.AddValue(string,string).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.AddValue(string, string)') | Adds a new string value. |
| [RemoveTag(string)](TimeseriesDataTimestamp.RemoveTag(string).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.RemoveTag(string)') | Removes a tag from the values |
| [RemoveValue(string)](TimeseriesDataTimestamp.RemoveValue(string).md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp.RemoveValue(string)') | Removes a parameter value. |
