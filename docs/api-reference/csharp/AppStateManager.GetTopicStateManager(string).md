#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[AppStateManager](AppStateManager.md 'QuixStreams.Streaming.States.AppStateManager')

## AppStateManager.GetTopicStateManager(string) Method

Gets an instance of the [TopicStateManager](TopicStateManager.md 'QuixStreams.Streaming.States.TopicStateManager') class for the specified [topicName](AppStateManager.GetTopicStateManager(string).md#QuixStreams.Streaming.States.AppStateManager.GetTopicStateManager(string).topicName 'QuixStreams.Streaming.States.AppStateManager.GetTopicStateManager(string).topicName').

```csharp
public QuixStreams.Streaming.States.TopicStateManager GetTopicStateManager(string topicName);
```
#### Parameters

<a name='QuixStreams.Streaming.States.AppStateManager.GetTopicStateManager(string).topicName'></a>

`topicName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The topic name

#### Returns
[TopicStateManager](TopicStateManager.md 'QuixStreams.Streaming.States.TopicStateManager')  
The newly created [TopicStateManager](TopicStateManager.md 'QuixStreams.Streaming.States.TopicStateManager') instance.