#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## LeadingEdgeBuffer Class

Leading edge buffer where time and tag are treated as a compound key

```csharp
public class LeadingEdgeBuffer
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; LeadingEdgeBuffer

| Properties | |
| :--- | :--- |
| [Epoch](LeadingEdgeBuffer.Epoch.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer.Epoch') | The epoch each timestamp is measured from. If null, epoch (if any) of the underlying producer will be used. |

| Methods | |
| :--- | :--- |
| [Flush()](LeadingEdgeBuffer.Flush().md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer.Flush()') | Publishes all data in the buffer, regardless of leading edge condition. |
| [GetOrCreateTimestamp(long, Dictionary&lt;string,string&gt;)](LeadingEdgeBuffer.GetOrCreateTimestamp(long,Dictionary_string,string_).md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer.GetOrCreateTimestamp(long, System.Collections.Generic.Dictionary<string,string>)') | Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist. |
| [Publish()](LeadingEdgeBuffer.Publish().md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer.Publish()') | Publishes data according to leading edge condition |

| Events | |
| :--- | :--- |
| [OnBackfill](LeadingEdgeBuffer.OnBackfill.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer.OnBackfill') | Data arriving with a timestamp earlier then the latest released timestamp is discarded but released in this event for further processing or forwarding. |
| [OnPublish](LeadingEdgeBuffer.OnPublish.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer.OnPublish') | Event raised when [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') condition is met and before data is published to the underlying [StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer'). |
