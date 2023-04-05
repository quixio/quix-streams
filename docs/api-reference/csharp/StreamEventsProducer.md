#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## StreamEventsProducer Class

Helper class for producing [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions') and [EventData](EventData.md 'QuixStreams.Streaming.Models.EventData')

```csharp
public class StreamEventsProducer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamEventsProducer

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Properties | |
| :--- | :--- |
| [DefaultLocation](StreamEventsProducer.DefaultLocation.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.DefaultLocation') | Default Location of the events. Event definitions added with [AddDefinition(string, string, string)](StreamEventsProducer.AddDefinition(string,string,string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinition(string, string, string)') will be inserted at this location.<br/>See [AddLocation(string)](StreamEventsProducer.AddLocation(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddLocation(string)') for adding definitions at a different location without changing default.<br/>Example: "/Group1/SubGroup2" |
| [DefaultTags](StreamEventsProducer.DefaultTags.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.DefaultTags') | Default Tags injected to all Event Values sent by the producer. |
| [Epoch](StreamEventsProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Epoch') | Default epoch used for Timestamp event values. Datetime added on top of all the Timestamps. |

| Methods | |
| :--- | :--- |
| [AddDefinition(string, string, string)](StreamEventsProducer.AddDefinition(string,string,string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinition(string, string, string)') | Add new Event definition to define properties like Name or Level, among others. |
| [AddDefinitions(List&lt;EventDefinition&gt;)](StreamEventsProducer.AddDefinitions(List_EventDefinition_).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinitions(System.Collections.Generic.List<QuixStreams.Streaming.Models.EventDefinition>)') | Adds a list of definitions to the [StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer'). Configure it with the builder methods. |
| [AddLocation(string)](StreamEventsProducer.AddLocation(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddLocation(string)') | Adds a new Location in the event groups hierarchy. |
| [AddTimestamp(DateTime)](StreamEventsProducer.AddTimestamp(DateTime).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestamp(System.DateTime)') | Starts adding a new set of event values at the given timestamp.<br/>Note, [Epoch](StreamEventsProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Epoch') is not used when invoking with [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime') |
| [AddTimestamp(TimeSpan)](StreamEventsProducer.AddTimestamp(TimeSpan).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestamp(System.TimeSpan)') | Starts adding a new set of event values at the given timestamp. |
| [AddTimestampMilliseconds(long)](StreamEventsProducer.AddTimestampMilliseconds(long).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestampMilliseconds(long)') | Starts adding a new set of event values at the given timestamp. |
| [AddTimestampNanoseconds(long)](StreamEventsProducer.AddTimestampNanoseconds(long).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestampNanoseconds(long)') | Starts adding a new set of event values at the given timestamp. |
| [Dispose()](StreamEventsProducer.Dispose().md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Dispose()') | Flushes internal buffers and disposes |
| [Flush()](StreamEventsProducer.Flush().md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Flush()') | Immediately writes the event definitions from the buffer without waiting for buffer condition to fulfill (200ms timeout) |
| [Publish(EventData)](StreamEventsProducer.Publish(EventData).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Publish(QuixStreams.Streaming.Models.EventData)') | Publish an event into the stream. |
| [Publish(ICollection&lt;EventData&gt;)](StreamEventsProducer.Publish(ICollection_EventData_).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Publish(System.Collections.Generic.ICollection<QuixStreams.Streaming.Models.EventData>)') | Publish events into the stream. |
