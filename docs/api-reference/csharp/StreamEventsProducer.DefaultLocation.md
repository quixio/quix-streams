#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

## StreamEventsProducer.DefaultLocation Property

Default Location of the events. Event definitions added with [AddDefinition(string, string, string)](StreamEventsProducer.AddDefinition(string,string,string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinition(string, string, string)') will be inserted at this location.  
See [AddLocation(string)](StreamEventsProducer.AddLocation(string).md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddLocation(string)') for adding definitions at a different location without changing default.  
Example: "/Group1/SubGroup2"

```csharp
public string DefaultLocation { get; set; }
```

#### Property Value
[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')