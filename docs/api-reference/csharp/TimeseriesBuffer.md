#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## TimeseriesBuffer Class

Class used to read from the stream in a buffered manner

```csharp
public class TimeseriesBuffer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TimeseriesBuffer

Derived  
&#8627; [TimeseriesBufferConsumer](TimeseriesBufferConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesBufferConsumer')  
&#8627; [TimeseriesBufferProducer](TimeseriesBufferProducer.md 'QuixStreams.Streaming.Models.StreamProducer.TimeseriesBufferProducer')

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [TimeseriesBuffer(TimeseriesBufferConfiguration, string[], bool, bool)](TimeseriesBuffer.TimeseriesBuffer(TimeseriesBufferConfiguration,string[],bool,bool).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.TimeseriesBuffer(QuixStreams.Streaming.Models.TimeseriesBufferConfiguration, string[], bool, bool)') | Initializes a new instance of [TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer') |

| Properties | |
| :--- | :--- |
| [BufferTimeout](TimeseriesBuffer.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.BufferTimeout') | Timeout configuration. [BufferTimeout](TimeseriesBufferConfiguration.BufferTimeout.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.BufferTimeout') |
| [CustomTrigger](TimeseriesBuffer.CustomTrigger.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.CustomTrigger') | Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked with the entire buffer content<br/>Defaults to null (disabled). |
| [CustomTriggerBeforeEnqueue](TimeseriesBuffer.CustomTriggerBeforeEnqueue.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.CustomTriggerBeforeEnqueue') | Gets or set the custom function which is invoked before adding the timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked before adding the timestamp to it.<br/>Defaults to null (disabled). |
| [Filter](TimeseriesBuffer.Filter.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.Filter') | Filter configuration. [Filter](TimeseriesBufferConfiguration.Filter.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.Filter') |
| [PacketSize](TimeseriesBuffer.PacketSize.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.PacketSize') | Packet Size configuration. [PacketSize](TimeseriesBufferConfiguration.PacketSize.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.PacketSize') |
| [TimeSpanInMilliseconds](TimeseriesBuffer.TimeSpanInMilliseconds.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.TimeSpanInMilliseconds') | TimeSpan configuration in Milliseconds. [TimeSpanInMilliseconds](TimeseriesBufferConfiguration.TimeSpanInMilliseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInMilliseconds') |
| [TimeSpanInNanoseconds](TimeseriesBuffer.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.TimeSpanInNanoseconds') | TimeSpan configuration in Nanoseconds. [TimeSpanInNanoseconds](TimeseriesBufferConfiguration.TimeSpanInNanoseconds.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration.TimeSpanInNanoseconds') |

| Methods | |
| :--- | :--- |
| [ArrayConcatMethod&lt;T&gt;(IEnumerable&lt;T[]&gt;)](TimeseriesBuffer.ArrayConcatMethod_T_(IEnumerable_T[]_).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.ArrayConcatMethod<T>(System.Collections.Generic.IEnumerable<T[]>)') | Function which concatenate arrays in an efficient way |
| [ConcatDataFrames(List&lt;TimeseriesDataRaw&gt;)](TimeseriesBuffer.ConcatDataFrames(List_TimeseriesDataRaw_).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.ConcatDataFrames(System.Collections.Generic.List<QuixStreams.Telemetry.Models.TimeseriesDataRaw>)') | Concatenate list of TimeseriesDataRaws into a single TimeseriesDataRaw |
| [CopyTimeseriesDataRawIndex(TimeseriesDataRaw, int, int)](TimeseriesBuffer.CopyTimeseriesDataRawIndex(TimeseriesDataRaw,int,int).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.CopyTimeseriesDataRawIndex(QuixStreams.Telemetry.Models.TimeseriesDataRaw, int, int)') | Copy one timestamp to another index of the buffer |
| [Dispose()](TimeseriesBuffer.Dispose().md 'QuixStreams.Streaming.Models.TimeseriesBuffer.Dispose()') | Dispose the buffer. It releases data out before the actual disposal. |
| [FilterOutNullRows(TimeseriesDataRaw)](TimeseriesBuffer.FilterOutNullRows(TimeseriesDataRaw).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.FilterOutNullRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Remove rows that only contain null values |
| [FlushData(bool)](TimeseriesBuffer.FlushData(bool).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.FlushData(bool)') | Flush data from the buffer and release it to make it available for Read events subscribers |
| [GenerateArrayMaskFilterMethod&lt;T&gt;(T[], List&lt;int&gt;)](TimeseriesBuffer.GenerateArrayMaskFilterMethod_T_(T[],List_int_).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateArrayMaskFilterMethod<T>(T[], System.Collections.Generic.List<int>)') | Generic function to filter rows by mapping the filtered index of the original array |
| [GenerateDictionaryMaskFilterMethod&lt;T&gt;(Dictionary&lt;string,T[]&gt;, List&lt;int&gt;)](TimeseriesBuffer.GenerateDictionaryMaskFilterMethod_T_(Dictionary_string,T[]_,List_int_).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.GenerateDictionaryMaskFilterMethod<T>(System.Collections.Generic.Dictionary<string,T[]>, System.Collections.Generic.List<int>)') | Generic function to filter Array rows of a Dictionary of mapped columns in a efficent way |
| [MergeTimestamps(TimeseriesDataRaw)](TimeseriesBuffer.MergeTimestamps(TimeseriesDataRaw).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.MergeTimestamps(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Merge existing timestamps with the same timestamp and tags |
| [SelectPdrRows(TimeseriesDataRaw, int)](TimeseriesBuffer.SelectPdrRows(TimeseriesDataRaw,int).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw, int)') | Get row subset of the TimeseriesDataRaw starting from startIndex until the last timestamp available |
| [SelectPdrRows(TimeseriesDataRaw, int, int)](TimeseriesBuffer.SelectPdrRows(TimeseriesDataRaw,int,int).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRows(QuixStreams.Telemetry.Models.TimeseriesDataRaw, int, int)') | Get row subset of the TimeseriesDataRaw starting from startIndex and end at startIndex + Count |
| [SelectPdrRowsByMask(TimeseriesDataRaw, List&lt;int&gt;)](TimeseriesBuffer.SelectPdrRowsByMask(TimeseriesDataRaw,List_int_).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.SelectPdrRowsByMask(QuixStreams.Telemetry.Models.TimeseriesDataRaw, System.Collections.Generic.List<int>)') | Get row subset of the TimeseriesDataRaw from a list of selected indexes |
| [WriteChunk(TimeseriesDataRaw)](TimeseriesBuffer.WriteChunk(TimeseriesDataRaw).md 'QuixStreams.Streaming.Models.TimeseriesBuffer.WriteChunk(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Writes a chunck of data into the buffer |

| Events | |
| :--- | :--- |
| [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') | Event invoked when TimeseriesData is received from the buffer |
| [OnRawReleased](TimeseriesBuffer.OnRawReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnRawReleased') | Event invoked when TimeseriesDataRaw is received from the buffer |
