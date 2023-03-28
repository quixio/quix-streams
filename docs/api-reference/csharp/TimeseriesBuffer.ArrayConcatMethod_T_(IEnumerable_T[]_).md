#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.ArrayConcatMethod<T>(IEnumerable<T[]>) Method

Function which concatenate arrays in an efficient way

```csharp
private static T[] ArrayConcatMethod<T>(System.Collections.Generic.IEnumerable<T[]> inp);
```
#### Type parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod_T_(System.Collections.Generic.IEnumerable_T[]_).T'></a>

`T`
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod_T_(System.Collections.Generic.IEnumerable_T[]_).inp'></a>

`inp` [System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[T](TimeseriesBuffer.ArrayConcatMethod_T_(IEnumerable_T[]_).md#QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod_T_(System.Collections.Generic.IEnumerable_T[]_).T 'QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod<T>(System.Collections.Generic.IEnumerable<T[]>).T')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')

List of arrays to concatenate

#### Returns
[T](TimeseriesBuffer.ArrayConcatMethod_T_(IEnumerable_T[]_).md#QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod_T_(System.Collections.Generic.IEnumerable_T[]_).T 'QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod<T>(System.Collections.Generic.IEnumerable<T[]>).T')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')  
Concatenated array