#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## EventDataBuilder Class

Builder for creating [EventData](EventData.md 'QuixStreams.Streaming.Models.EventData') packages within the [StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

```csharp
public class EventDataBuilder
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; EventDataBuilder

| Constructors | |
| :--- | :--- |
| [EventDataBuilder(StreamEventsProducer, long)](EventDataBuilder.EventDataBuilder(StreamEventsProducer,long).md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer, long)') | Initializes a new instance of [EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder') |

| Methods | |
| :--- | :--- |
| [AddTag(string, string)](EventDataBuilder.AddTag(string,string).md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.AddTag(string, string)') | Adds a tag to the event values |
| [AddTags(IEnumerable&lt;KeyValuePair&lt;string,string&gt;&gt;)](EventDataBuilder.AddTags(IEnumerable_KeyValuePair_string,string__).md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.AddTags(System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string,string>>)') | Adds tags to the event values. |
| [AddValue(string, string)](EventDataBuilder.AddValue(string,string).md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.AddValue(string, string)') | Adds new event value at the time the builder is created for |
| [Publish()](EventDataBuilder.Publish().md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.Publish()') | Publish the events |
