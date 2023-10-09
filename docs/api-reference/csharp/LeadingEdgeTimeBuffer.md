#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## LeadingEdgeTimeBuffer Class

Leading edge buffer where time is the only key and tags are not treated as part of the key

```csharp
public class LeadingEdgeTimeBuffer
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; LeadingEdgeTimeBuffer

| Properties | |
| :--- | :--- |
| [Epoch](LeadingEdgeTimeBuffer.Epoch.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.Epoch') | The epoch each timestamp is measured from. If null, epoch (if any) of the underlying producer will be used. |

| Methods | |
| :--- | :--- |
| [Flush()](LeadingEdgeTimeBuffer.Flush().md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.Flush()') | Publishes all data in the buffer, regardless of leading edge condition. |
| [GetOrCreateTimestamp(long)](LeadingEdgeTimeBuffer.GetOrCreateTimestamp(long).md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.GetOrCreateTimestamp(long)') | Gets an already buffered row based on timestamp and tags that can be modified or creates a new one if it doesn't exist. |
| [Publish()](LeadingEdgeTimeBuffer.Publish().md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.Publish()') | Publishes data according to leading edge condition |

| Events | |
| :--- | :--- |
| [OnBackfill](LeadingEdgeTimeBuffer.OnBackfill.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.OnBackfill') | Data arriving with a timestamp earlier then the latest released timestamp is discarded but released in this event for further processing or forwarding. |
| [OnPublish](LeadingEdgeTimeBuffer.OnPublish.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer.OnPublish') | Event raised when [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') condition is met and before data is published to the underlying [StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer'). |
