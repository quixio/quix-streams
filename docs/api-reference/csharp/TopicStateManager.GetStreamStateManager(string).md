#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[TopicStateManager](TopicStateManager.md 'QuixStreams.Streaming.States.TopicStateManager')

## TopicStateManager.GetStreamStateManager(string) Method

Gets an instance of the [StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager') class for the specified [streamId](TopicStateManager.GetStreamStateManager(string).md#QuixStreams.Streaming.States.TopicStateManager.GetStreamStateManager(string).streamId 'QuixStreams.Streaming.States.TopicStateManager.GetStreamStateManager(string).streamId').

```csharp
public QuixStreams.Streaming.States.StreamStateManager GetStreamStateManager(string streamId);
```
#### Parameters

<a name='QuixStreams.Streaming.States.TopicStateManager.GetStreamStateManager(string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The ID of the stream.

#### Returns
[StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager')  
The newly created [StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager') instance.