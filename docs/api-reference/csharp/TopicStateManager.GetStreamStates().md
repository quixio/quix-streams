#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[TopicStateManager](TopicStateManager.md 'QuixStreams.Streaming.States.TopicStateManager')

## TopicStateManager.GetStreamStates() Method

Returns an enumerable collection of all available stream states for the current topic.

```csharp
public System.Collections.Generic.IEnumerable<string> GetStreamStates();
```

#### Returns
[System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')  
An enumerable collection of string values representing the stream state names.