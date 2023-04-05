#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## TimeseriesDataBuilder Class

Builder for managing [TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp') instances on [TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

```csharp
public class TimeseriesDataBuilder
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TimeseriesDataBuilder

| Constructors | |
| :--- | :--- |
| [TimeseriesDataBuilder(TimeseriesBufferProducer, TimeseriesData, TimeseriesDataTimestamp)](TimeseriesDataBuilder.TimeseriesDataBuilder(TimeseriesBufferProducer,TimeseriesData,TimeseriesDataTimestamp).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder(QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer, QuixStreams.Streaming.Models.TimeseriesData, QuixStreams.Streaming.Models.TimeseriesDataTimestamp)') | Initializes a new instance of [TimeseriesDataBuilder](TimeseriesDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder') |

| Methods | |
| :--- | :--- |
| [AddTag(string, string)](TimeseriesDataBuilder.AddTag(string,string).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.AddTag(string, string)') | Adds a tag to the values. |
| [AddTags(IEnumerable&lt;KeyValuePair&lt;string,string&gt;&gt;)](TimeseriesDataBuilder.AddTags(IEnumerable_KeyValuePair_string,string__).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.AddTags(System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string,string>>)') | Adds tags to the values. |
| [AddValue(string, byte[])](TimeseriesDataBuilder.AddValue(string,byte[]).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.AddValue(string, byte[])') | Adds new parameter value at the time the builder is created for |
| [AddValue(string, double)](TimeseriesDataBuilder.AddValue(string,double).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.AddValue(string, double)') | Adds new parameter value at the time the builder is created for |
| [AddValue(string, string)](TimeseriesDataBuilder.AddValue(string,string).md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.AddValue(string, string)') | Adds new parameter value at the time the builder is created for |
| [Publish()](TimeseriesDataBuilder.Publish().md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesDataBuilder.Publish()') | Publish the values |
