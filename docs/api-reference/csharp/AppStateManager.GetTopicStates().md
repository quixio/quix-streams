#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[AppStateManager](AppStateManager.md 'QuixStreams.Streaming.States.AppStateManager')

## AppStateManager.GetTopicStates() Method

Returns an enumerable collection of all available topic states for the current app.

```csharp
public System.Collections.Generic.IEnumerable<string> GetTopicStates();
```

#### Returns
[System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')  
An enumerable collection of string values representing the topic state names.