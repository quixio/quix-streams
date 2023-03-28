#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.GenerateArrayMaskFilterMethod<T>(T[], List<int>) Method

Generic function to filter rows by mapping the filtered index of the original array

```csharp
private static T[] GenerateArrayMaskFilterMethod<T>(T[] inp, System.Collections.Generic.List<int> selectRows);
```
#### Type parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],System.Collections.Generic.List_int_).T'></a>

`T`
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],System.Collections.Generic.List_int_).inp'></a>

`inp` [T](TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],List_int_).md#QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],System.Collections.Generic.List_int_).T 'QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod<T>(T[], System.Collections.Generic.List<int>).T')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')

Original array

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],System.Collections.Generic.List_int_).selectRows'></a>

`selectRows` [System.Collections.Generic.List&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')[System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')

List of indexes of the array to filter

#### Returns
[T](TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],List_int_).md#QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],System.Collections.Generic.List_int_).T 'QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod<T>(T[], System.Collections.Generic.List<int>).T')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')  
Filtered array