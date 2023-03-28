#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## StreamProducer Class

Stream writer interface. Stands for a new stream that we want to send to the platform.  
It provides you helper properties to stream data like parameter values, events, definitions and all the information you can persist to the platform.

```csharp
internal class StreamProducer : QuixStreams.Telemetry.StreamPipeline,
QuixStreams.Streaming.IStreamProducer,
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; [QuixStreams.Telemetry.StreamPipeline](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.StreamPipeline 'QuixStreams.Telemetry.StreamPipeline') &#129106; StreamProducer

Implements [IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer'), [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Constructors | |
| :--- | :--- |
| [StreamProducer(ITopicProducerInternal, Func&lt;string,TelemetryKafkaProducer&gt;, string)](StreamProducer.StreamProducer(ITopicProducerInternal,Func_string,TelemetryKafkaProducer_,string).md 'QuixStreams.Streaming.StreamProducer.StreamProducer(QuixStreams.Streaming.ITopicProducerInternal, System.Func<string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer>, string)') | Initializes a new instance of [StreamProducer](StreamProducer.md 'QuixStreams.Streaming.StreamProducer') |

| Properties | |
| :--- | :--- |
| [Epoch](StreamProducer.Epoch.md 'QuixStreams.Streaming.StreamProducer.Epoch') | Default Epoch used for Parameters and Events |
| [Events](StreamProducer.Events.md 'QuixStreams.Streaming.StreamProducer.Events') | Gets the producer for publishing event related information of the stream such as event definitions and values |
| [Properties](StreamProducer.Properties.md 'QuixStreams.Streaming.StreamProducer.Properties') | Properties of the stream. The changes will automatically be sent after a slight delay |
| [Timeseries](StreamProducer.Timeseries.md 'QuixStreams.Streaming.StreamProducer.Timeseries') | Gets the producer for publishing timeseries related information of the stream such as parameter definitions and values |

| Methods | |
| :--- | :--- |
| [Close(StreamEndType)](StreamProducer.Close(StreamEndType).md 'QuixStreams.Streaming.StreamProducer.Close(QuixStreams.Telemetry.Models.StreamEndType)') | Close the stream and flush the pending data to stream. |
| [Publish(EventDataRaw)](StreamProducer.Publish(EventDataRaw).md 'QuixStreams.Streaming.StreamProducer.Publish(QuixStreams.Telemetry.Models.EventDataRaw)') | Publish a single event to the stream |
| [Publish(EventDefinitions)](StreamProducer.Publish(EventDefinitions).md 'QuixStreams.Streaming.StreamProducer.Publish(QuixStreams.Telemetry.Models.EventDefinitions)') | Write the optional Event definition properties describing the hierarchical grouping of events<br/>Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values. |
| [Publish(ParameterDefinitions)](StreamProducer.Publish(ParameterDefinitions).md 'QuixStreams.Streaming.StreamProducer.Publish(QuixStreams.Telemetry.Models.ParameterDefinitions)') | Write the optional Parameter definition properties describing the hierarchical grouping of parameters<br/>Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values. |
| [Publish(StreamProperties)](StreamProducer.Publish(StreamProperties).md 'QuixStreams.Streaming.StreamProducer.Publish(QuixStreams.Telemetry.Models.StreamProperties)') | Publish a stream properties to the stream |
| [Publish(TimeseriesDataRaw)](StreamProducer.Publish(TimeseriesDataRaw).md 'QuixStreams.Streaming.StreamProducer.Publish(QuixStreams.Telemetry.Models.TimeseriesDataRaw)') | Publish a single Timeseries data package to the stream |
| [Publish(ICollection&lt;EventDataRaw&gt;)](StreamProducer.Publish(ICollection_EventDataRaw_).md 'QuixStreams.Streaming.StreamProducer.Publish(System.Collections.Generic.ICollection<QuixStreams.Telemetry.Models.EventDataRaw>)') | Publish a set of events to the stream |
| [Publish(List&lt;TimeseriesDataRaw&gt;)](StreamProducer.Publish(List_TimeseriesDataRaw_).md 'QuixStreams.Streaming.StreamProducer.Publish(System.Collections.Generic.List<QuixStreams.Telemetry.Models.TimeseriesDataRaw>)') | Publish a set of Timeseries data packages to the stream |

| Events | |
| :--- | :--- |
| [OnWriteException](StreamProducer.OnWriteException.md 'QuixStreams.Streaming.StreamProducer.OnWriteException') | Event raised when an exception occurred during the writing processes |
