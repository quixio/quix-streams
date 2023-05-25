#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## AppStateManager Class

Manages the states of a app.

```csharp
public class AppStateManager
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; AppStateManager

| Constructors | |
| :--- | :--- |
| [AppStateManager(IStateStorage, ILoggerFactory)](AppStateManager.AppStateManager(IStateStorage,ILoggerFactory).md 'QuixStreams.Streaming.States.AppStateManager.AppStateManager(QuixStreams.State.Storage.IStateStorage, Microsoft.Extensions.Logging.ILoggerFactory)') | Initializes a new instance of the AppStateManager class. |

| Methods | |
| :--- | :--- |
| [DeleteTopicState(string)](AppStateManager.DeleteTopicState(string).md 'QuixStreams.Streaming.States.AppStateManager.DeleteTopicState(string)') | Deletes the topic state with the specified name |
| [DeleteTopicStates()](AppStateManager.DeleteTopicStates().md 'QuixStreams.Streaming.States.AppStateManager.DeleteTopicStates()') | Deletes all topic states for the current app. |
| [GetTopicStateManager(string)](AppStateManager.GetTopicStateManager(string).md 'QuixStreams.Streaming.States.AppStateManager.GetTopicStateManager(string)') | Gets an instance of the [TopicStateManager](TopicStateManager.md 'QuixStreams.Streaming.States.TopicStateManager') class for the specified [topicName](AppStateManager.GetTopicStateManager(string).md#QuixStreams.Streaming.States.AppStateManager.GetTopicStateManager(string).topicName 'QuixStreams.Streaming.States.AppStateManager.GetTopicStateManager(string).topicName'). |
| [GetTopicStates()](AppStateManager.GetTopicStates().md 'QuixStreams.Streaming.States.AppStateManager.GetTopicStates()') | Returns an enumerable collection of all available topic states for the current app. |
