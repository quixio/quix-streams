#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw')

## RawTopicProducer Class

Class to write raw messages into a Topic (capable to write non-quixstreams messages)

```csharp
public class RawTopicProducer :
QuixStreams.Streaming.Raw.IRawTopicProducer,
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; RawTopicProducer

Implements [IRawTopicProducer](IRawTopicProducer.md 'QuixStreams.Streaming.Raw.IRawTopicProducer'), [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [RawTopicProducer(string, string, Dictionary&lt;string,string&gt;)](RawTopicProducer.RawTopicProducer(string,string,Dictionary_string,string_).md 'QuixStreams.Streaming.Raw.RawTopicProducer.RawTopicProducer(string, string, System.Collections.Generic.Dictionary<string,string>)') | Initializes a new instance of [RawTopicProducer](RawTopicProducer.md 'QuixStreams.Streaming.Raw.RawTopicProducer') |

| Methods | |
| :--- | :--- |
| [Publish(RawMessage)](RawTopicProducer.Publish(RawMessage).md 'QuixStreams.Streaming.Raw.RawTopicProducer.Publish(QuixStreams.Streaming.Raw.RawMessage)') | Publish data to the topic |

| Events | |
| :--- | :--- |
| [OnDisposed](RawTopicProducer.OnDisposed.md 'QuixStreams.Streaming.Raw.RawTopicProducer.OnDisposed') | Raised when the resource is disposed |
