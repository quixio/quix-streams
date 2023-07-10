#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## IStreamState Interface

Interface for a stream state

```csharp
public interface IStreamState
```

Derived  
&#8627; [StreamDictionaryState](StreamDictionaryState.md 'QuixStreams.Streaming.States.StreamDictionaryState')  
&#8627; [StreamDictionaryState&lt;T&gt;](StreamDictionaryState_T_.md 'QuixStreams.Streaming.States.StreamDictionaryState<T>')  
&#8627; [StreamScalarState](StreamScalarState.md 'QuixStreams.Streaming.States.StreamScalarState')  
&#8627; [StreamScalarState&lt;T&gt;](StreamScalarState_T_.md 'QuixStreams.Streaming.States.StreamScalarState<T>')

| Methods | |
| :--- | :--- |
| [Flush()](IStreamState.Flush().md 'QuixStreams.Streaming.States.IStreamState.Flush()') | Flushes the changes made to the in-memory state to the specified storage. |
| [Reset()](IStreamState.Reset().md 'QuixStreams.Streaming.States.IStreamState.Reset()') | Reset the state to before in-memory modifications |

| Events | |
| :--- | :--- |
| [OnFlushed](IStreamState.OnFlushed.md 'QuixStreams.Streaming.States.IStreamState.OnFlushed') | Raised immediately after a flush operation is completed. |
| [OnFlushing](IStreamState.OnFlushing.md 'QuixStreams.Streaming.States.IStreamState.OnFlushing') | Raised immediately before a flush operation is performed. |
