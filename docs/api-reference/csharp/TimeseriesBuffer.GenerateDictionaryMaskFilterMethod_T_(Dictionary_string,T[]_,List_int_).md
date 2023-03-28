#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.GenerateDictionaryMaskFilterMethod<T>(Dictionary<string,T[]>, List<int>) Method

Generic function to filter Array rows of a Dictionary of mapped columns in a efficent way

```csharp
private static System.Collections.Generic.Dictionary<string,T[]> GenerateDictionaryMaskFilterMethod<T>(System.Collections.Generic.Dictionary<string,T[]> originalDictionary, System.Collections.Generic.List<int> selectRows);
```
#### Type parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(System.Collections.Generic.Dictionary_string,T[]_,System.Collections.Generic.List_int_).T'></a>

`T`
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(System.Collections.Generic.Dictionary_string,T[]_,System.Collections.Generic.List_int_).originalDictionary'></a>

`originalDictionary` [System.Collections.Generic.Dictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[T](TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(Dictionary_string,T[]_,List_int_).md#QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(System.Collections.Generic.Dictionary_string,T[]_,System.Collections.Generic.List_int_).T 'QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod<T>(System.Collections.Generic.Dictionary<string,T[]>, System.Collections.Generic.List<int>).T')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')

Original dictionary

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(System.Collections.Generic.Dictionary_string,T[]_,System.Collections.Generic.List_int_).selectRows'></a>

`selectRows` [System.Collections.Generic.List&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')[System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')

List of indexes of the array to filter

#### Returns
[System.Collections.Generic.Dictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[T](TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(Dictionary_string,T[]_,List_int_).md#QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(System.Collections.Generic.Dictionary_string,T[]_,System.Collections.Generic.List_int_).T 'QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod<T>(System.Collections.Generic.Dictionary<string,T[]>, System.Collections.Generic.List<int>).T')[[]](https://docs.microsoft.com/en-us/dotnet/api/System.Array 'System.Array')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')  
Filtered dictionary