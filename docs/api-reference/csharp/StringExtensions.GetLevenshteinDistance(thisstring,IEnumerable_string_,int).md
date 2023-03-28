#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Utils](QuixStreams.Streaming.Utils.md 'QuixStreams.Streaming.Utils').[StringExtensions](StringExtensions.md 'QuixStreams.Streaming.Utils.StringExtensions')

## StringExtensions.GetLevenshteinDistance(this string, IEnumerable<string>, int) Method

Returns the strings in the ascending order of keystrokes necessary to change the compared string to the other

```csharp
public static System.Collections.Generic.IEnumerable<string> GetLevenshteinDistance(this string compared, System.Collections.Generic.IEnumerable<string> compareTo, int max);
```
#### Parameters

<a name='QuixStreams.Streaming.Utils.StringExtensions.GetLevenshteinDistance(thisstring,System.Collections.Generic.IEnumerable_string_,int).compared'></a>

`compared` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

<a name='QuixStreams.Streaming.Utils.StringExtensions.GetLevenshteinDistance(thisstring,System.Collections.Generic.IEnumerable_string_,int).compareTo'></a>

`compareTo` [System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')

<a name='QuixStreams.Streaming.Utils.StringExtensions.GetLevenshteinDistance(thisstring,System.Collections.Generic.IEnumerable_string_,int).max'></a>

`max` [System.Int32](https://docs.microsoft.com/en-us/dotnet/api/System.Int32 'System.Int32')

#### Returns
[System.Collections.Generic.IEnumerable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IEnumerable-1 'System.Collections.Generic.IEnumerable`1')