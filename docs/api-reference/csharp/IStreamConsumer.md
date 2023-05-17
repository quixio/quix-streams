#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## IStreamConsumer Interface

Stream reader interface. Stands for a new stream read from the platform.  
Allows to read the stream data received from a topic.

```csharp
public interface IStreamConsumer
```

| Properties | |
| :--- | :--- |
| [Events](IStreamConsumer.Events.md 'QuixStreams.Streaming.IStreamConsumer.Events') | Gets the consumer for accessing event related information of the stream such as event definitions and values. |
| [Properties](IStreamConsumer.Properties.md 'QuixStreams.Streaming.IStreamConsumer.Properties') | Gets the consumer for accessing the properties and metadata of the stream. |
| [StreamId](IStreamConsumer.StreamId.md 'QuixStreams.Streaming.IStreamConsumer.StreamId') | Gets the stream Id of the stream. |
| [Timeseries](IStreamConsumer.Timeseries.md 'QuixStreams.Streaming.IStreamConsumer.Timeseries') | Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values. |

| Methods | |
| :--- | :--- |
| [GetStateManager()](IStreamConsumer.GetStateManager().md 'QuixStreams.Streaming.IStreamConsumer.GetStateManager()') | Gets the manager for the stream states |

| Events | |
| :--- | :--- |
| [OnPackageReceived](IStreamConsumer.OnPackageReceived.md 'QuixStreams.Streaming.IStreamConsumer.OnPackageReceived') | Event raised when a stream package has been received. |
| [OnStreamClosed](IStreamConsumer.OnStreamClosed.md 'QuixStreams.Streaming.IStreamConsumer.OnStreamClosed') | Event raised when the stream has closed. |
