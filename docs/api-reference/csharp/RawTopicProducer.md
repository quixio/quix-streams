#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw')

## RawTopicProducer Class

Class to produce raw messages into a Topic (capable to producing non-quixstreams messages)

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
| [Dispose()](RawTopicProducer.Dispose().md 'QuixStreams.Streaming.Raw.RawTopicProducer.Dispose()') | Flushes pending messages and disposes underlying resources |
| [Flush()](RawTopicProducer.Flush().md 'QuixStreams.Streaming.Raw.RawTopicProducer.Flush()') | Flushes pending messages to the broker |
| [Publish(KafkaMessage)](RawTopicProducer.Publish(KafkaMessage).md 'QuixStreams.Streaming.Raw.RawTopicProducer.Publish(QuixStreams.Kafka.KafkaMessage)') | Publish message to the topic |

| Events | |
| :--- | :--- |
| [OnDisposed](RawTopicProducer.OnDisposed.md 'QuixStreams.Streaming.Raw.RawTopicProducer.OnDisposed') | Raised when the resource is disposed |
