#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## TopicProducer Class

Implementation of [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer') to produce outgoing streams

```csharp
public class TopicProducer :
QuixStreams.Streaming.ITopicProducer,
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TopicProducer

Implements [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer'), [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [TopicProducer(KafkaProducerConfiguration, string)](TopicProducer.TopicProducer(KafkaProducerConfiguration,string).md 'QuixStreams.Streaming.TopicProducer.TopicProducer(QuixStreams.Telemetry.Kafka.KafkaProducerConfiguration, string)') | Initializes a new instance of [TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer') |
| [TopicProducer(Func&lt;string,TelemetryKafkaProducer&gt;)](TopicProducer.TopicProducer(Func_string,TelemetryKafkaProducer_).md 'QuixStreams.Streaming.TopicProducer.TopicProducer(System.Func<string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer>)') | Initializes a new instance of [TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer') |

| Methods | |
| :--- | :--- |
| [CreateStream()](TopicProducer.CreateStream().md 'QuixStreams.Streaming.TopicProducer.CreateStream()') | Creates a new stream and returns the related stream producer to operate it. |
| [CreateStream(string)](TopicProducer.CreateStream(string).md 'QuixStreams.Streaming.TopicProducer.CreateStream(string)') | Creates a new stream and returns the related stream producer to operate it. |
| [Dispose()](TopicProducer.Dispose().md 'QuixStreams.Streaming.TopicProducer.Dispose()') | Flushes pending data to the broker and disposes underlying resources |
| [Flush()](TopicProducer.Flush().md 'QuixStreams.Streaming.TopicProducer.Flush()') | Flushes pending data to the broker |
| [GetOrCreateStream(string, Action&lt;IStreamProducer&gt;)](TopicProducer.GetOrCreateStream(string,Action_IStreamProducer_).md 'QuixStreams.Streaming.TopicProducer.GetOrCreateStream(string, System.Action<QuixStreams.Streaming.IStreamProducer>)') | Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream. |
| [GetStream(string)](TopicProducer.GetStream(string).md 'QuixStreams.Streaming.TopicProducer.GetStream(string)') | Retrieves a stream that was previously created by this instance, if the stream is not closed. |
| [RemoveStream(string)](TopicProducer.RemoveStream(string).md 'QuixStreams.Streaming.TopicProducer.RemoveStream(string)') | Removes a stream from the internal list of streams |

| Events | |
| :--- | :--- |
| [OnDisposed](TopicProducer.OnDisposed.md 'QuixStreams.Streaming.TopicProducer.OnDisposed') | Raised when the resource finished disposing |
