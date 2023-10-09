#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw')

## RawTopicConsumer Class

Topic class to read incoming raw messages (capable to read non-quixstreams messages)

```csharp
public class RawTopicConsumer :
QuixStreams.Streaming.Raw.IRawTopicConsumer,
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; RawTopicConsumer

Implements [IRawTopicConsumer](IRawTopicConsumer.md 'QuixStreams.Streaming.Raw.IRawTopicConsumer'), [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [RawTopicConsumer(string, string, string, Dictionary&lt;string,string&gt;, Nullable&lt;AutoOffsetReset&gt;)](RawTopicConsumer.RawTopicConsumer(string,string,string,Dictionary_string,string_,Nullable_AutoOffsetReset_).md 'QuixStreams.Streaming.Raw.RawTopicConsumer.RawTopicConsumer(string, string, string, System.Collections.Generic.Dictionary<string,string>, System.Nullable<QuixStreams.Telemetry.Kafka.AutoOffsetReset>)') | Initializes a new instance of [RawTopicConsumer](RawTopicConsumer.md 'QuixStreams.Streaming.Raw.RawTopicConsumer') |

| Methods | |
| :--- | :--- |
| [Subscribe()](RawTopicConsumer.Subscribe().md 'QuixStreams.Streaming.Raw.RawTopicConsumer.Subscribe()') | Start reading data from the topic.<br/>Use 'OnMessageReceived' event to read messages after executing this method |
| [Unsubscribe()](RawTopicConsumer.Unsubscribe().md 'QuixStreams.Streaming.Raw.RawTopicConsumer.Unsubscribe()') | Stops reading data from the topic. |

| Events | |
| :--- | :--- |
| [OnDisposed](RawTopicConsumer.OnDisposed.md 'QuixStreams.Streaming.Raw.RawTopicConsumer.OnDisposed') | Raised when the resource is disposed |
| [OnErrorOccurred](RawTopicConsumer.OnErrorOccurred.md 'QuixStreams.Streaming.Raw.RawTopicConsumer.OnErrorOccurred') | Event raised when a new error occurs |
| [OnMessageReceived](RawTopicConsumer.OnMessageReceived.md 'QuixStreams.Streaming.Raw.RawTopicConsumer.OnMessageReceived') | Event raised when a message is received from the topic |
