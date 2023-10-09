#### [QuixStreams.Streaming](index.md 'index')

## QuixStreams.Streaming.Models Namespace

| Classes | |
| :--- | :--- |
| [EventData](EventData.md 'QuixStreams.Streaming.Models.EventData') | Represents a single point in time with event value and tags attached to it |
| [EventDefinition](EventDefinition.md 'QuixStreams.Streaming.Models.EventDefinition') | Describes additional context for the event |
| [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') | Leading edge buffer where time and tag are treated as a compound key |
| [LeadingEdgeRow](LeadingEdgeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeRow') | Represents a single row of data in the [LeadingEdgeBuffer](LeadingEdgeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeBuffer') |
| [LeadingEdgeTimeBuffer](LeadingEdgeTimeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer') | Leading edge buffer where time is the only key and tags are not treated as part of the key |
| [LeadingEdgeTimeRow](LeadingEdgeTimeRow.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeRow') | Represents a single row of data in the [LeadingEdgeTimeBuffer](LeadingEdgeTimeBuffer.md 'QuixStreams.Streaming.Models.LeadingEdgeTimeBuffer') |
| [ParameterDefinition](ParameterDefinition.md 'QuixStreams.Streaming.Models.ParameterDefinition') | Describes additional context for the parameter |
| [TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer') | Represents a class used to consume and produce stream messages in a buffered manner. |
| [TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration') | Describes the configuration for timeseries buffers |
| [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData') | Represents a collection of [TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp') |
| [TimeseriesDataParameter](TimeseriesDataParameter.md 'QuixStreams.Streaming.Models.TimeseriesDataParameter') | Timeseries data parameter |

| Structs | |
| :--- | :--- |
| [ParameterValue](ParameterValue.md 'QuixStreams.Streaming.Models.ParameterValue') | Represents a single parameter value of either numeric, string or binary type |
| [TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp') | Represents a single point in time with parameter values and tags attached to that time |

| Enums | |
| :--- | :--- |
| [CommitMode](CommitMode.md 'QuixStreams.Streaming.Models.CommitMode') | The mode for committing packages |
| [ParameterValueType](ParameterValueType.md 'QuixStreams.Streaming.Models.ParameterValueType') | Describes the type of a Value of a specific Timestamp / Parameter |
