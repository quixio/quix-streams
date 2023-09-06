#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## ParameterValue Struct

Represents a single parameter value of either numeric, string or binary type

```csharp
public readonly struct ParameterValue
```

| Constructors | |
| :--- | :--- |
| [ParameterValue(long, TimeseriesDataParameter)](ParameterValue.ParameterValue(long,TimeseriesDataParameter).md 'QuixStreams.Streaming.Models.ParameterValue.ParameterValue(long, QuixStreams.Streaming.Models.TimeseriesDataParameter)') | Initializes a new instance with the of [ParameterValue](ParameterValue.md 'QuixStreams.Streaming.Models.ParameterValue') with the specified index and parameter |

| Fields | |
| :--- | :--- |
| [Type](ParameterValue.Type.md 'QuixStreams.Streaming.Models.ParameterValue.Type') | Gets the type of value, which is numeric, string or binary if set, else empty |

| Properties | |
| :--- | :--- |
| [BinaryValue](ParameterValue.BinaryValue.md 'QuixStreams.Streaming.Models.ParameterValue.BinaryValue') | The binary value of the parameter |
| [NumericValue](ParameterValue.NumericValue.md 'QuixStreams.Streaming.Models.ParameterValue.NumericValue') | The numeric value of the parameter. |
| [ParameterId](ParameterValue.ParameterId.md 'QuixStreams.Streaming.Models.ParameterValue.ParameterId') | Gets the Parameter Id of the parameter |
| [StringValue](ParameterValue.StringValue.md 'QuixStreams.Streaming.Models.ParameterValue.StringValue') | The string value of the parameter |
| [Value](ParameterValue.Value.md 'QuixStreams.Streaming.Models.ParameterValue.Value') | Gets the underlying value |

| Operators | |
| :--- | :--- |
| [operator ==(ParameterValue, ParameterValue)](ParameterValue.operator(ParameterValue,ParameterValue).md 'QuixStreams.Streaming.Models.ParameterValue.op_Equality(QuixStreams.Streaming.Models.ParameterValue, QuixStreams.Streaming.Models.ParameterValue)') | Equality comparison of Parameter values |
| [operator !=(ParameterValue, ParameterValue)](ParameterValue.operator!(ParameterValue,ParameterValue).md 'QuixStreams.Streaming.Models.ParameterValue.op_Inequality(QuixStreams.Streaming.Models.ParameterValue, QuixStreams.Streaming.Models.ParameterValue)') | Negative equality comparison of Parameter values |
