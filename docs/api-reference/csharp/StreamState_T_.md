#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## StreamState<T> Class

Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.

```csharp
public class StreamState<T> :
System.Collections.Generic.IDictionary<string, T>,
System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<string, T>>,
System.Collections.Generic.IEnumerable<System.Collections.Generic.KeyValuePair<string, T>>,
System.Collections.IEnumerable
```
#### Type parameters

<a name='QuixStreams.Streaming.States.StreamState_T_.T'></a>

`T`

The type of values stored in the StreamState.

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamState<T>

Implements [System.Collections.Generic.IDictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')[T](StreamState_T_.md#QuixStreams.Streaming.States.StreamState_T_.T 'QuixStreams.Streaming.States.StreamState<T>.T')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2'), [System.Collections.Generic.ICollection&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.ICollection-1 'System.Collections.Generic.ICollection`1')[System.Collections.Generic.KeyValuePair&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[T](StreamState_T_.md#QuixStreams.Streaming.States.StreamState_T_.T 'QuixStreams.Streaming.States.StreamState<T>.T')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.ICollection-1 'System.Collections.Generic.ICollection`1'), [System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[System.Collections.Generic.KeyValuePair&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[T](StreamState_T_.md#QuixStreams.Streaming.States.StreamState_T_.T 'QuixStreams.Streaming.States.StreamState<T>.T')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.KeyValuePair-2 'System.Collections.Generic.KeyValuePair`2')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1'), [System.Collections.IEnumerable](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.IEnumerable 'System.Collections.IEnumerable')

| Methods | |
| :--- | :--- |
| [Flush()](StreamState_T_.Flush().md 'QuixStreams.Streaming.States.StreamState<T>.Flush()') | Flushes the changes made to the in-memory state to the specified storage. |
| [Reset()](StreamState_T_.Reset().md 'QuixStreams.Streaming.States.StreamState<T>.Reset()') | Reset the state to before in-memory modifications |

| Events | |
| :--- | :--- |
| [OnFlushed](StreamState_T_.OnFlushed.md 'QuixStreams.Streaming.States.StreamState<T>.OnFlushed') | Raised immediately after a flush operation is completed. |
| [OnFlushing](StreamState_T_.OnFlushing.md 'QuixStreams.Streaming.States.StreamState<T>.OnFlushing') | Raised immediately before a flush operation is performed. |
