#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## StreamScalarState Class

Represents a scalar storage of a value with a specific means to be persisted.

```csharp
public class StreamScalarState :
QuixStreams.Streaming.States.IStreamState
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamScalarState

Implements [IStreamState](IStreamState.md 'QuixStreams.Streaming.States.IStreamState')

| Properties | |
| :--- | :--- |
| [Value](StreamScalarState.Value.md 'QuixStreams.Streaming.States.StreamScalarState.Value') | Gets or sets the value to the in-memory state. |

| Methods | |
| :--- | :--- |
| [Clear()](StreamScalarState.Clear().md 'QuixStreams.Streaming.States.StreamScalarState.Clear()') | Sets the value of in-memory state to null and marks the state for clearing when flushed. |
| [Flush()](StreamScalarState.Flush().md 'QuixStreams.Streaming.States.StreamScalarState.Flush()') | Flushes the changes made to the in-memory state to the specified storage. |
| [Reset()](StreamScalarState.Reset().md 'QuixStreams.Streaming.States.StreamScalarState.Reset()') | Reset the state to before in-memory modifications |

| Events | |
| :--- | :--- |
| [OnFlushed](StreamScalarState.OnFlushed.md 'QuixStreams.Streaming.States.StreamScalarState.OnFlushed') | Raised immediately after a flush operation is completed. |
| [OnFlushing](StreamScalarState.OnFlushing.md 'QuixStreams.Streaming.States.StreamScalarState.OnFlushing') | Raised immediately before a flush operation is performed. |
