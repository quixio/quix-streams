#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## IStreamProducerInternal Interface

Stands for a new stream that we want to send to the platform.  
It provides you helper properties to stream data the platform like parameter values, events, definitions and all the information you can persist to the platform.

```csharp
internal interface IStreamProducerInternal :
QuixStreams.Streaming.IStreamProducer,
System.IDisposable
```

Derived  
&#8627; [StreamProducer](StreamProducer.md 'QuixStreams.Streaming.StreamProducer')

Implements [IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer'), [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Properties | |
| :--- | :--- |
| [Epoch](IStreamProducerInternal.Epoch.md 'QuixStreams.Streaming.IStreamProducerInternal.Epoch') | Default Epoch used for Parameters and Events |

| Methods | |
| :--- | :--- |
| [Publish(EventDataRaw)](IStreamProducerInternal.Publish(EventDataRaw).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.EventDataRaw)') | Publish a single event to the stream |
| [Publish(EventDefinitions)](IStreamProducerInternal.Publish(EventDefinitions).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.EventDefinitions)') | Write the optional Event definition properties describing the hierarchical grouping of events<br/>Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values. |
| [Publish(ParameterDefinitions)](IStreamProducerInternal.Publish(ParameterDefinitions).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.ParameterDefinitions)') | Write the optional Parameter definition properties describing the hierarchical grouping of parameters<br/>Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values. |
| [Publish(StreamProperties)](IStreamProducerInternal.Publish(StreamProperties).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.StreamProperties)') | Publish a stream properties to the stream |
| [Publish(TimeseriesDataRaw)](IStreamProducerInternal.Publish(TimeseriesDataRaw).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Publish a single Timeseries data package to the stream |
| [Publish(ICollection&lt;EventDataRaw&gt;)](IStreamProducerInternal.Publish(ICollection_EventDataRaw_).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(System.Collections.Generic.ICollection<QuixStreams.Telemetry.Models.EventDataRaw>)') | Publish a set of events to the stream |
| [Publish(List&lt;TimeseriesDataRaw&gt;)](IStreamProducerInternal.Publish(List_TimeseriesDataRaw_).md 'QuixStreams.Streaming.IStreamProducerInternal.Publish(System.Collections.Generic.List<QuixStreams.Telemetry.Models.TimeseriesDataRaw>)') | Publish a set of Timeseries data packages to the stream |

| Events | |
| :--- | :--- |
| [OnBeforeSend](IStreamProducerInternal.OnBeforeSend.md 'QuixStreams.Streaming.IStreamProducerInternal.OnBeforeSend') | Event raised before the message is being sent |
