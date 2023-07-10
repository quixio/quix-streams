#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## StreamScalarState<T> Class

Represents a scalar storage of a value with a specific means to be persisted.

```csharp
public class StreamScalarState<T> :
QuixStreams.Streaming.States.IStreamState
```
#### Type parameters

<a name='QuixStreams.Streaming.States.StreamScalarState_T_.T'></a>

`T`

The type of values stored in the StreamState.

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamScalarState<T>

Implements [IStreamState](IStreamState.md 'QuixStreams.Streaming.States.IStreamState')

| Properties | |
| :--- | :--- |
| [Value](StreamScalarState_T_.Value.md 'QuixStreams.Streaming.States.StreamScalarState<T>.Value') | Gets or sets the value to the in-memory state. |

| Methods | |
| :--- | :--- |
| [Flush()](StreamScalarState_T_.Flush().md 'QuixStreams.Streaming.States.StreamScalarState<T>.Flush()') | Flushes the changes made to the in-memory state to the specified storage. |
| [Reset()](StreamScalarState_T_.Reset().md 'QuixStreams.Streaming.States.StreamScalarState<T>.Reset()') | Reset the state to before in-memory modifications |

| Events | |
| :--- | :--- |
| [OnFlushed](StreamScalarState_T_.OnFlushed.md 'QuixStreams.Streaming.States.StreamScalarState<T>.OnFlushed') | Raised immediately after a flush operation is completed. |
| [OnFlushing](StreamScalarState_T_.OnFlushing.md 'QuixStreams.Streaming.States.StreamScalarState<T>.OnFlushing') | Raised immediately before a flush operation is performed. |
