# Table of Contents

* [quixstreams](#quixstreams)
* [quixstreams.quixstreamingclient](#quixstreams.quixstreamingclient)
  * [TokenValidationConfiguration](#quixstreams.quixstreamingclient.TokenValidationConfiguration)
    * [\_\_init\_\_](#quixstreams.quixstreamingclient.TokenValidationConfiguration.__init__)
    * [enabled](#quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled)
    * [enabled](#quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled)
    * [warning\_before\_expiry](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry)
    * [warning\_before\_expiry](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry)
    * [warn\_about\_pat\_token](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token)
    * [warn\_about\_pat\_token](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token)
  * [QuixStreamingClient](#quixstreams.quixstreamingclient.QuixStreamingClient)
    * [\_\_init\_\_](#quixstreams.quixstreamingclient.QuixStreamingClient.__init__)
    * [get\_topic\_consumer](#quixstreams.quixstreamingclient.QuixStreamingClient.get_topic_consumer)
    * [get\_topic\_producer](#quixstreams.quixstreamingclient.QuixStreamingClient.get_topic_producer)
    * [get\_raw\_topic\_consumer](#quixstreams.quixstreamingclient.QuixStreamingClient.get_raw_topic_consumer)
    * [get\_raw\_topic\_producer](#quixstreams.quixstreamingclient.QuixStreamingClient.get_raw_topic_producer)
    * [token\_validation\_config](#quixstreams.quixstreamingclient.QuixStreamingClient.token_validation_config)
    * [token\_validation\_config](#quixstreams.quixstreamingclient.QuixStreamingClient.token_validation_config)
    * [api\_url](#quixstreams.quixstreamingclient.QuixStreamingClient.api_url)
    * [api\_url](#quixstreams.quixstreamingclient.QuixStreamingClient.api_url)
    * [cache\_period](#quixstreams.quixstreamingclient.QuixStreamingClient.cache_period)
    * [cache\_period](#quixstreams.quixstreamingclient.QuixStreamingClient.cache_period)
* [quixstreams.logging](#quixstreams.logging)
* [quixstreams.models.streampackage](#quixstreams.models.streampackage)
  * [StreamPackage](#quixstreams.models.streampackage.StreamPackage)
    * [\_\_init\_\_](#quixstreams.models.streampackage.StreamPackage.__init__)
    * [transport\_context](#quixstreams.models.streampackage.StreamPackage.transport_context)
    * [to\_json](#quixstreams.models.streampackage.StreamPackage.to_json)
* [quixstreams.models.parameterdefinition](#quixstreams.models.parameterdefinition)
  * [ParameterDefinition](#quixstreams.models.parameterdefinition.ParameterDefinition)
    * [\_\_init\_\_](#quixstreams.models.parameterdefinition.ParameterDefinition.__init__)
* [quixstreams.models.autooffsetreset](#quixstreams.models.autooffsetreset)
  * [AutoOffsetReset](#quixstreams.models.autooffsetreset.AutoOffsetReset)
    * [Latest](#quixstreams.models.autooffsetreset.AutoOffsetReset.Latest)
    * [Earliest](#quixstreams.models.autooffsetreset.AutoOffsetReset.Earliest)
    * [Error](#quixstreams.models.autooffsetreset.AutoOffsetReset.Error)
* [quixstreams.models.timeseriesdataraw](#quixstreams.models.timeseriesdataraw)
  * [TimeseriesDataRaw](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw)
    * [\_\_init\_\_](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.__init__)
    * [to\_dataframe](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.to_dataframe)
    * [from\_dataframe](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.from_dataframe)
    * [set\_values](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.set_values)
    * [epoch](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.epoch)
    * [timestamps](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.timestamps)
    * [numeric\_values](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.numeric_values)
    * [string\_values](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.string_values)
    * [binary\_values](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.binary_values)
    * [tag\_values](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.tag_values)
* [quixstreams.models.netlist](#quixstreams.models.netlist)
  * [NetReadOnlyList](#quixstreams.models.netlist.NetReadOnlyList)
  * [NetList](#quixstreams.models.netlist.NetList)
    * [constructor\_for\_string](#quixstreams.models.netlist.NetList.constructor_for_string)
* [quixstreams.models.parametervalue](#quixstreams.models.parametervalue)
  * [ParameterValue](#quixstreams.models.parametervalue.ParameterValue)
    * [\_\_init\_\_](#quixstreams.models.parametervalue.ParameterValue.__init__)
    * [numeric\_value](#quixstreams.models.parametervalue.ParameterValue.numeric_value)
    * [numeric\_value](#quixstreams.models.parametervalue.ParameterValue.numeric_value)
    * [string\_value](#quixstreams.models.parametervalue.ParameterValue.string_value)
    * [string\_value](#quixstreams.models.parametervalue.ParameterValue.string_value)
    * [binary\_value](#quixstreams.models.parametervalue.ParameterValue.binary_value)
    * [binary\_value](#quixstreams.models.parametervalue.ParameterValue.binary_value)
    * [type](#quixstreams.models.parametervalue.ParameterValue.type)
    * [value](#quixstreams.models.parametervalue.ParameterValue.value)
* [quixstreams.models.timeseriesdata](#quixstreams.models.timeseriesdata)
  * [TimeseriesData](#quixstreams.models.timeseriesdata.TimeseriesData)
    * [\_\_init\_\_](#quixstreams.models.timeseriesdata.TimeseriesData.__init__)
    * [clone](#quixstreams.models.timeseriesdata.TimeseriesData.clone)
    * [add\_timestamp](#quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp)
    * [add\_timestamp\_milliseconds](#quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp_milliseconds)
    * [add\_timestamp\_nanoseconds](#quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp_nanoseconds)
    * [timestamps](#quixstreams.models.timeseriesdata.TimeseriesData.timestamps)
    * [timestamps](#quixstreams.models.timeseriesdata.TimeseriesData.timestamps)
    * [to\_dataframe](#quixstreams.models.timeseriesdata.TimeseriesData.to_dataframe)
    * [from\_panda\_dataframe](#quixstreams.models.timeseriesdata.TimeseriesData.from_panda_dataframe)
* [quixstreams.models.eventdefinition](#quixstreams.models.eventdefinition)
  * [EventDefinition](#quixstreams.models.eventdefinition.EventDefinition)
    * [\_\_init\_\_](#quixstreams.models.eventdefinition.EventDefinition.__init__)
* [quixstreams.models.streamconsumer.streamtimeseriesconsumer](#quixstreams.models.streamconsumer.streamtimeseriesconsumer)
  * [StreamTimeseriesConsumer](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer)
    * [\_\_init\_\_](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.__init__)
    * [on\_data\_received](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_data_received)
    * [on\_data\_received](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_data_received)
    * [on\_raw\_received](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_raw_received)
    * [on\_raw\_received](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_raw_received)
    * [on\_dataframe\_received](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_dataframe_received)
    * [on\_dataframe\_received](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_dataframe_received)
    * [on\_definitions\_changed](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_definitions_changed)
    * [on\_definitions\_changed](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_definitions_changed)
    * [definitions](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.definitions)
    * [create\_buffer](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.create_buffer)
* [quixstreams.models.streamconsumer.streameventsconsumer](#quixstreams.models.streamconsumer.streameventsconsumer)
  * [StreamEventsConsumer](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer)
    * [\_\_init\_\_](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.__init__)
    * [on\_data\_received](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received)
    * [on\_data\_received](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received)
    * [on\_definitions\_changed](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed)
    * [on\_definitions\_changed](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed)
    * [definitions](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.definitions)
* [quixstreams.models.streamconsumer.timeseriesbufferconsumer](#quixstreams.models.streamconsumer.timeseriesbufferconsumer)
  * [TimeseriesBufferConsumer](#quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer)
    * [\_\_init\_\_](#quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.__init__)
* [quixstreams.models.streamconsumer.streampropertiesconsumer](#quixstreams.models.streamconsumer.streampropertiesconsumer)
  * [StreamPropertiesConsumer](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer)
    * [\_\_init\_\_](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.__init__)
    * [on\_changed](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.on_changed)
    * [on\_changed](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.on_changed)
    * [name](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.name)
    * [location](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.location)
    * [time\_of\_recording](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.time_of_recording)
    * [metadata](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.metadata)
    * [parents](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.parents)
* [quixstreams.models.streamconsumer](#quixstreams.models.streamconsumer)
* [quixstreams.models.commitmode](#quixstreams.models.commitmode)
* [quixstreams.models](#quixstreams.models)
* [quixstreams.models.netdict](#quixstreams.models.netdict)
  * [ReadOnlyNetDict](#quixstreams.models.netdict.ReadOnlyNetDict)
  * [NetDict](#quixstreams.models.netdict.NetDict)
    * [constructor\_for\_string\_string](#quixstreams.models.netdict.NetDict.constructor_for_string_string)
* [quixstreams.models.timeseriesbuffer](#quixstreams.models.timeseriesbuffer)
  * [TimeseriesBuffer](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer)
    * [\_\_init\_\_](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.__init__)
    * [on\_data\_released](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_data_released)
    * [on\_data\_released](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_data_released)
    * [on\_raw\_released](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_raw_released)
    * [on\_raw\_released](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_raw_released)
    * [on\_dataframe\_released](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_dataframe_released)
    * [on\_dataframe\_released](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_dataframe_released)
    * [filter](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.filter)
    * [filter](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.filter)
    * [custom\_trigger](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.custom_trigger)
    * [custom\_trigger](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.custom_trigger)
    * [packet\_size](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.packet_size)
    * [packet\_size](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.packet_size)
    * [time\_span\_in\_nanoseconds](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_nanoseconds)
    * [time\_span\_in\_nanoseconds](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_nanoseconds)
    * [time\_span\_in\_milliseconds](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_milliseconds)
    * [time\_span\_in\_milliseconds](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_milliseconds)
    * [buffer\_timeout](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.buffer_timeout)
    * [buffer\_timeout](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.buffer_timeout)
* [quixstreams.models.commitoptions](#quixstreams.models.commitoptions)
  * [CommitOptions](#quixstreams.models.commitoptions.CommitOptions)
    * [\_\_init\_\_](#quixstreams.models.commitoptions.CommitOptions.__init__)
    * [auto\_commit\_enabled](#quixstreams.models.commitoptions.CommitOptions.auto_commit_enabled)
    * [auto\_commit\_enabled](#quixstreams.models.commitoptions.CommitOptions.auto_commit_enabled)
    * [commit\_interval](#quixstreams.models.commitoptions.CommitOptions.commit_interval)
    * [commit\_interval](#quixstreams.models.commitoptions.CommitOptions.commit_interval)
    * [commit\_every](#quixstreams.models.commitoptions.CommitOptions.commit_every)
    * [commit\_every](#quixstreams.models.commitoptions.CommitOptions.commit_every)
* [quixstreams.models.timeseriesdatatimestamp](#quixstreams.models.timeseriesdatatimestamp)
  * [TimeseriesDataTimestamp](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp)
    * [\_\_init\_\_](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.__init__)
    * [parameters](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.parameters)
    * [tags](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.tags)
    * [timestamp\_nanoseconds](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_nanoseconds)
    * [timestamp\_milliseconds](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_milliseconds)
    * [timestamp](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp)
    * [timestamp\_as\_time\_span](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_as_time_span)
    * [add\_value](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_value)
    * [remove\_value](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.remove_value)
    * [add\_tag](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_tag)
    * [remove\_tag](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.remove_tag)
    * [add\_tags](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_tags)
* [quixstreams.models.streamproducer.streameventsproducer](#quixstreams.models.streamproducer.streameventsproducer)
  * [StreamEventsProducer](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer)
    * [\_\_init\_\_](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.__init__)
    * [flush](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.flush)
    * [default\_tags](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_tags)
    * [default\_location](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_location)
    * [default\_location](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_location)
    * [epoch](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.epoch)
    * [epoch](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.epoch)
    * [publish](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.publish)
    * [add\_timestamp](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp)
    * [add\_timestamp\_milliseconds](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp_milliseconds)
    * [add\_timestamp\_nanoseconds](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp_nanoseconds)
    * [add\_definition](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_definition)
    * [add\_location](#quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_location)
* [quixstreams.models.streamproducer.streamtimeseriesproducer](#quixstreams.models.streamproducer.streamtimeseriesproducer)
  * [StreamTimeseriesProducer](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer)
    * [\_\_init\_\_](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.__init__)
    * [flush](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.flush)
    * [add\_definition](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.add_definition)
    * [add\_location](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.add_location)
    * [default\_location](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.default_location)
    * [default\_location](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.default_location)
    * [buffer](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.buffer)
    * [publish](#quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.publish)
* [quixstreams.models.streamproducer.timeseriesbufferproducer](#quixstreams.models.streamproducer.timeseriesbufferproducer)
  * [TimeseriesBufferProducer](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer)
    * [\_\_init\_\_](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.__init__)
    * [default\_tags](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.default_tags)
    * [epoch](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.epoch)
    * [epoch](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.epoch)
    * [add\_timestamp](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.add_timestamp)
    * [add\_timestamp\_nanoseconds](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.add_timestamp_nanoseconds)
    * [flush](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.flush)
    * [publish](#quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.publish)
* [quixstreams.models.streamproducer](#quixstreams.models.streamproducer)
* [quixstreams.models.streamproducer.streampropertiesproducer](#quixstreams.models.streamproducer.streampropertiesproducer)
  * [StreamPropertiesProducer](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer)
    * [\_\_init\_\_](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.__init__)
    * [name](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.name)
    * [name](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.name)
    * [location](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.location)
    * [location](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.location)
    * [metadata](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.metadata)
    * [parents](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.parents)
    * [time\_of\_recording](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.time_of_recording)
    * [time\_of\_recording](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.time_of_recording)
    * [flush\_interval](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush_interval)
    * [flush\_interval](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush_interval)
    * [flush](#quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush)
* [quixstreams.models.streamendtype](#quixstreams.models.streamendtype)
* [quixstreams.models.timeseriesbufferconfiguration](#quixstreams.models.timeseriesbufferconfiguration)
  * [TimeseriesBufferConfiguration](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration)
    * [\_\_init\_\_](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.__init__)
    * [packet\_size](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.packet_size)
    * [packet\_size](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.packet_size)
    * [time\_span\_in\_nanoseconds](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_nanoseconds)
    * [time\_span\_in\_nanoseconds](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_nanoseconds)
    * [time\_span\_in\_milliseconds](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_milliseconds)
    * [time\_span\_in\_milliseconds](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_milliseconds)
    * [buffer\_timeout](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.buffer_timeout)
    * [buffer\_timeout](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.buffer_timeout)
    * [custom\_trigger\_before\_enqueue](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger_before_enqueue)
    * [custom\_trigger\_before\_enqueue](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger_before_enqueue)
    * [filter](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.filter)
    * [filter](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.filter)
    * [custom\_trigger](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger)
    * [custom\_trigger](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger)
* [quixstreams.models.eventdata](#quixstreams.models.eventdata)
  * [EventData](#quixstreams.models.eventdata.EventData)
    * [\_\_init\_\_](#quixstreams.models.eventdata.EventData.__init__)
    * [id](#quixstreams.models.eventdata.EventData.id)
    * [id](#quixstreams.models.eventdata.EventData.id)
    * [value](#quixstreams.models.eventdata.EventData.value)
    * [value](#quixstreams.models.eventdata.EventData.value)
    * [tags](#quixstreams.models.eventdata.EventData.tags)
    * [timestamp\_nanoseconds](#quixstreams.models.eventdata.EventData.timestamp_nanoseconds)
    * [timestamp\_milliseconds](#quixstreams.models.eventdata.EventData.timestamp_milliseconds)
    * [timestamp](#quixstreams.models.eventdata.EventData.timestamp)
    * [timestamp\_as\_time\_span](#quixstreams.models.eventdata.EventData.timestamp_as_time_span)
    * [clone](#quixstreams.models.eventdata.EventData.clone)
    * [add\_tag](#quixstreams.models.eventdata.EventData.add_tag)
    * [add\_tags](#quixstreams.models.eventdata.EventData.add_tags)
    * [remove\_tag](#quixstreams.models.eventdata.EventData.remove_tag)
* [quixstreams.models.eventlevel](#quixstreams.models.eventlevel)
* [quixstreams.raw.rawtopicconsumer](#quixstreams.raw.rawtopicconsumer)
  * [RawTopicConsumer](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer)
    * [\_\_init\_\_](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.__init__)
    * [on\_message\_received](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received)
    * [on\_message\_received](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received)
    * [on\_error\_occurred](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred)
    * [on\_error\_occurred](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred)
    * [subscribe](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.subscribe)
* [quixstreams.raw.rawmessage](#quixstreams.raw.rawmessage)
  * [RawMessage](#quixstreams.raw.rawmessage.RawMessage)
    * [key](#quixstreams.raw.rawmessage.RawMessage.key)
    * [key](#quixstreams.raw.rawmessage.RawMessage.key)
    * [value](#quixstreams.raw.rawmessage.RawMessage.value)
    * [value](#quixstreams.raw.rawmessage.RawMessage.value)
    * [metadata](#quixstreams.raw.rawmessage.RawMessage.metadata)
* [quixstreams.raw](#quixstreams.raw)
* [quixstreams.raw.rawtopicproducer](#quixstreams.raw.rawtopicproducer)
  * [RawTopicProducer](#quixstreams.raw.rawtopicproducer.RawTopicProducer)
    * [\_\_init\_\_](#quixstreams.raw.rawtopicproducer.RawTopicProducer.__init__)
    * [publish](#quixstreams.raw.rawtopicproducer.RawTopicProducer.publish)
* [quixstreams.state.localfilestorage](#quixstreams.state.localfilestorage)
  * [LocalFileStorage](#quixstreams.state.localfilestorage.LocalFileStorage)
    * [\_\_init\_\_](#quixstreams.state.localfilestorage.LocalFileStorage.__init__)
    * [get](#quixstreams.state.localfilestorage.LocalFileStorage.get)
    * [set](#quixstreams.state.localfilestorage.LocalFileStorage.set)
    * [contains\_key](#quixstreams.state.localfilestorage.LocalFileStorage.contains_key)
    * [get\_all\_keys](#quixstreams.state.localfilestorage.LocalFileStorage.get_all_keys)
    * [remove](#quixstreams.state.localfilestorage.LocalFileStorage.remove)
    * [clear](#quixstreams.state.localfilestorage.LocalFileStorage.clear)
* [quixstreams.state.statevalue](#quixstreams.state.statevalue)
  * [StateValue](#quixstreams.state.statevalue.StateValue)
    * [\_\_init\_\_](#quixstreams.state.statevalue.StateValue.__init__)
    * [type](#quixstreams.state.statevalue.StateValue.type)
    * [value](#quixstreams.state.statevalue.StateValue.value)
* [quixstreams.state](#quixstreams.state)
* [quixstreams.state.inmemorystorage](#quixstreams.state.inmemorystorage)
  * [InMemoryStorage](#quixstreams.state.inmemorystorage.InMemoryStorage)
* [quixstreams.state.statetype](#quixstreams.state.statetype)
* [quixstreams.app](#quixstreams.app)
  * [CancellationTokenSource](#quixstreams.app.CancellationTokenSource)
    * [\_\_init\_\_](#quixstreams.app.CancellationTokenSource.__init__)
  * [App](#quixstreams.app.App)
    * [run](#quixstreams.app.App.run)
* [quixstreams.topicproducer](#quixstreams.topicproducer)
  * [TopicProducer](#quixstreams.topicproducer.TopicProducer)
    * [\_\_init\_\_](#quixstreams.topicproducer.TopicProducer.__init__)
    * [on\_disposed](#quixstreams.topicproducer.TopicProducer.on_disposed)
    * [on\_disposed](#quixstreams.topicproducer.TopicProducer.on_disposed)
    * [create\_stream](#quixstreams.topicproducer.TopicProducer.create_stream)
    * [get\_stream](#quixstreams.topicproducer.TopicProducer.get_stream)
    * [get\_or\_create\_stream](#quixstreams.topicproducer.TopicProducer.get_or_create_stream)
* [quixstreams.streamconsumer](#quixstreams.streamconsumer)
  * [StreamConsumer](#quixstreams.streamconsumer.StreamConsumer)
    * [\_\_init\_\_](#quixstreams.streamconsumer.StreamConsumer.__init__)
    * [topic](#quixstreams.streamconsumer.StreamConsumer.topic)
    * [on\_stream\_closed](#quixstreams.streamconsumer.StreamConsumer.on_stream_closed)
    * [on\_stream\_closed](#quixstreams.streamconsumer.StreamConsumer.on_stream_closed)
    * [on\_package\_received](#quixstreams.streamconsumer.StreamConsumer.on_package_received)
    * [on\_package\_received](#quixstreams.streamconsumer.StreamConsumer.on_package_received)
    * [stream\_id](#quixstreams.streamconsumer.StreamConsumer.stream_id)
    * [properties](#quixstreams.streamconsumer.StreamConsumer.properties)
    * [events](#quixstreams.streamconsumer.StreamConsumer.events)
    * [timeseries](#quixstreams.streamconsumer.StreamConsumer.timeseries)
* [quixstreams.kafkastreamingclient](#quixstreams.kafkastreamingclient)
  * [KafkaStreamingClient](#quixstreams.kafkastreamingclient.KafkaStreamingClient)
    * [\_\_init\_\_](#quixstreams.kafkastreamingclient.KafkaStreamingClient.__init__)
    * [get\_topic\_consumer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_consumer)
    * [get\_topic\_producer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_producer)
    * [get\_raw\_topic\_consumer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_consumer)
    * [get\_raw\_topic\_producer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_producer)
* [quixstreams.streamproducer](#quixstreams.streamproducer)
  * [StreamProducer](#quixstreams.streamproducer.StreamProducer)
    * [\_\_init\_\_](#quixstreams.streamproducer.StreamProducer.__init__)
    * [topic](#quixstreams.streamproducer.StreamProducer.topic)
    * [on\_write\_exception](#quixstreams.streamproducer.StreamProducer.on_write_exception)
    * [on\_write\_exception](#quixstreams.streamproducer.StreamProducer.on_write_exception)
    * [stream\_id](#quixstreams.streamproducer.StreamProducer.stream_id)
    * [epoch](#quixstreams.streamproducer.StreamProducer.epoch)
    * [epoch](#quixstreams.streamproducer.StreamProducer.epoch)
    * [properties](#quixstreams.streamproducer.StreamProducer.properties)
    * [timeseries](#quixstreams.streamproducer.StreamProducer.timeseries)
    * [events](#quixstreams.streamproducer.StreamProducer.events)
    * [close](#quixstreams.streamproducer.StreamProducer.close)
* [quixstreams.helpers.timeconverter](#quixstreams.helpers.timeconverter)
* [quixstreams.helpers.enumconverter](#quixstreams.helpers.enumconverter)
* [quixstreams.helpers.dotnet.datetimeconverter](#quixstreams.helpers.dotnet.datetimeconverter)
  * [DateTimeConverter](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter)
    * [datetime\_to\_python](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_python)
    * [datetime\_to\_dotnet](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_dotnet)
    * [timespan\_to\_python](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timespan_to_python)
    * [timedelta\_to\_dotnet](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timedelta_to_dotnet)
* [quixstreams.helpers.nativedecorator](#quixstreams.helpers.nativedecorator)
* [quixstreams.helpers](#quixstreams.helpers)
* [quixstreams.topicconsumer](#quixstreams.topicconsumer)
  * [TopicConsumer](#quixstreams.topicconsumer.TopicConsumer)
    * [\_\_init\_\_](#quixstreams.topicconsumer.TopicConsumer.__init__)
    * [on\_stream\_received](#quixstreams.topicconsumer.TopicConsumer.on_stream_received)
    * [on\_stream\_received](#quixstreams.topicconsumer.TopicConsumer.on_stream_received)
    * [on\_streams\_revoked](#quixstreams.topicconsumer.TopicConsumer.on_streams_revoked)
    * [on\_streams\_revoked](#quixstreams.topicconsumer.TopicConsumer.on_streams_revoked)
    * [on\_revoking](#quixstreams.topicconsumer.TopicConsumer.on_revoking)
    * [on\_revoking](#quixstreams.topicconsumer.TopicConsumer.on_revoking)
    * [on\_committed](#quixstreams.topicconsumer.TopicConsumer.on_committed)
    * [on\_committed](#quixstreams.topicconsumer.TopicConsumer.on_committed)
    * [on\_committing](#quixstreams.topicconsumer.TopicConsumer.on_committing)
    * [on\_committing](#quixstreams.topicconsumer.TopicConsumer.on_committing)
    * [subscribe](#quixstreams.topicconsumer.TopicConsumer.subscribe)
    * [commit](#quixstreams.topicconsumer.TopicConsumer.commit)
* [quixstreams.builders.eventdatabuilder](#quixstreams.builders.eventdatabuilder)
  * [EventDataBuilder](#quixstreams.builders.eventdatabuilder.EventDataBuilder)
    * [\_\_init\_\_](#quixstreams.builders.eventdatabuilder.EventDataBuilder.__init__)
    * [add\_value](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_value)
    * [add\_tag](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tag)
    * [add\_tags](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tags)
    * [publish](#quixstreams.builders.eventdatabuilder.EventDataBuilder.publish)
* [quixstreams.builders.eventdefinitionbuilder](#quixstreams.builders.eventdefinitionbuilder)
  * [EventDefinitionBuilder](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder)
    * [\_\_init\_\_](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.__init__)
    * [set\_level](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_level)
    * [set\_custom\_properties](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_custom_properties)
    * [add\_definition](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.add_definition)
* [quixstreams.builders.parameterdefinitionbuilder](#quixstreams.builders.parameterdefinitionbuilder)
  * [ParameterDefinitionBuilder](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder)
    * [\_\_init\_\_](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.__init__)
    * [set\_range](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_range)
    * [set\_unit](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_unit)
    * [set\_format](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_format)
    * [set\_custom\_properties](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_custom_properties)
    * [add\_definition](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.add_definition)
* [quixstreams.builders.timeseriesdatabuilder](#quixstreams.builders.timeseriesdatabuilder)
  * [TimeseriesDataBuilder](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder)
    * [\_\_init\_\_](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.__init__)
    * [add\_value](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_value)
    * [add\_tag](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tag)
    * [add\_tags](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tags)
    * [publish](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.publish)
* [quixstreams.builders](#quixstreams.builders)
* [quixstreams.configuration.saslmechanism](#quixstreams.configuration.saslmechanism)
* [quixstreams.configuration.securityoptions](#quixstreams.configuration.securityoptions)
  * [SecurityOptions](#quixstreams.configuration.securityoptions.SecurityOptions)
    * [\_\_init\_\_](#quixstreams.configuration.securityoptions.SecurityOptions.__init__)
* [quixstreams.configuration](#quixstreams.configuration)

<a id="quixstreams"></a>

# quixstreams

<a id="quixstreams.quixstreamingclient"></a>

# quixstreams.quixstreamingclient

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration"></a>

## TokenValidationConfiguration Objects

```python
@nativedecorator
class TokenValidationConfiguration(object)
```

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TokenValidationConfiguration

**Arguments**:

  
- `net_pointer` _c_void_p_ - Pointer to an instance of a .net TokenValidationConfiguration

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled"></a>

#### enabled

```python
@property
def enabled() -> bool
```

Gets whether token validation and warnings are enabled. Defaults to true.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled"></a>

#### enabled

```python
@enabled.setter
def enabled(value: bool)
```

Sets whether token validation and warnings are enabled. Defaults to true.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry"></a>

#### warning\_before\_expiry

```python
@property
def warning_before_expiry() -> Union[timedelta, None]
```

Gets whether if the token expires within this period, a warning will be displayed. Defaults to 2 days. Set to None to disable the check

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry"></a>

#### warning\_before\_expiry

```python
@warning_before_expiry.setter
def warning_before_expiry(value: Union[timedelta, None])
```

Sets whether if the token expires within this period, a warning will be displayed. Defaults to 2 days. Set to None to disable the check

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token"></a>

#### warn\_about\_pat\_token

```python
@property
def warn_about_pat_token() -> bool
```

Gets whether to warn if the provided token is not PAT token. Defaults to true.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token"></a>

#### warn\_about\_pat\_token

```python
@warn_about_pat_token.setter
def warn_about_pat_token(value: bool)
```

Sets whether to warn if the provided token is not PAT token. Defaults to true.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient"></a>

## QuixStreamingClient Objects

```python
class QuixStreamingClient(object)
```

Class that is capable of creating input and output topics for reading and writing

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(token: str = None,
             auto_create_topics: bool = True,
             properties: Dict[str, str] = None,
             debug: bool = False)
```

Creates a new instance of quixstreamsClient that is capable of creating input and output topics for reading and writing

**Arguments**:

  
- `token` _string_ - The token to use when talking to Quix. When not provided, Quix__Sdk__Token environment variable will be used
  
- `auto_create_topics` _string_ - Whether topics should be auto created if they don't exist yet. Optional, defaults to true
  
- `properties` - Optional extra properties for broker configuration
  
- `debug` _string_ - Whether debugging should enabled

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_topic_consumer"></a>

#### get\_topic\_consumer

```python
def get_topic_consumer(
    topic_id_or_name: str,
    consumer_group: str = None,
    commit_settings: Union[CommitOptions, CommitMode] = None,
    auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Latest
) -> TopicConsumer
```

Opens an input topic capable of reading incoming streams

**Arguments**:

  
- `topic_id_or_name` _string_ - Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
  
- `consumer_group` _string_ - The consumer group id to use for consuming messages
  
- `commit_settings` _CommitOptions, CommitMode_ - the settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.
  
- `auto_offset_reset` _AutoOffsetReset_ - The offset to use when there is no saved offset for the consumer group. Defaults to latest

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_topic_producer"></a>

#### get\_topic\_producer

```python
def get_topic_producer(topic_id_or_name: str) -> TopicProducer
```

Opens an output topic capable of sending outgoing streams

**Arguments**:

  
- `topic_id_or_name` _string_ - Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_raw_topic_consumer"></a>

#### get\_raw\_topic\_consumer

```python
def get_raw_topic_consumer(
    topic_id_or_name: str,
    consumer_group: str = None,
    auto_offset_reset: Union[AutoOffsetReset,
                             None] = None) -> RawTopicConsumer
```

Opens an input topic for reading raw data from the stream

**Arguments**:

  
- `topic_id_or_name` _string_ - Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order
- `consumer_group` _string_ - Consumer group ( optional )

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_raw_topic_producer"></a>

#### get\_raw\_topic\_producer

```python
def get_raw_topic_producer(topic_id_or_name: str) -> RawTopicProducer
```

Opens an input topic for writing raw data to the stream

**Arguments**:

  
- `topic_id_or_name` _string_ - Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.token_validation_config"></a>

#### token\_validation\_config

```python
@property
def token_validation_config() -> TokenValidationConfiguration
```

Gets the configuration for token validation.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.token_validation_config"></a>

#### token\_validation\_config

```python
@token_validation_config.setter
def token_validation_config(value: TokenValidationConfiguration)
```

Sets the configuration for token validation.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.api_url"></a>

#### api\_url

```python
@property
def api_url() -> str
```

Gets the base API uri. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.api_url"></a>

#### api\_url

```python
@api_url.setter
def api_url(value: str)
```

Sets the base API uri. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.cache_period"></a>

#### cache\_period

```python
@property
def cache_period() -> timedelta
```

Gets the period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.cache_period"></a>

#### cache\_period

```python
@cache_period.setter
def cache_period(value: timedelta)
```

Sets the period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.

<a id="quixstreams.logging"></a>

# quixstreams.logging

<a id="quixstreams.models.streampackage"></a>

# quixstreams.models.streampackage

<a id="quixstreams.models.streampackage.StreamPackage"></a>

## StreamPackage Objects

```python
@nativedecorator
class StreamPackage(object)
```

Default model implementation for non-typed message packages of the Process layer. It holds a value and its type.

<a id="quixstreams.models.streampackage.StreamPackage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamPackage.

NOTE: Do not initialize this class manually. Will be initialized by StreamConsumer.on_package_received

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net StreamPackage.

<a id="quixstreams.models.streampackage.StreamPackage.transport_context"></a>

#### transport\_context

```python
@property
def transport_context() -> Dict[str, str]
```

Get the additional metadata for the stream

<a id="quixstreams.models.streampackage.StreamPackage.to_json"></a>

#### to\_json

```python
def to_json() -> str
```

Serialize the package into Json

<a id="quixstreams.models.parameterdefinition"></a>

# quixstreams.models.parameterdefinition

<a id="quixstreams.models.parameterdefinition.ParameterDefinition"></a>

## ParameterDefinition Objects

```python
class ParameterDefinition(object)
```

Describes additional context for the parameter

<a id="quixstreams.models.parameterdefinition.ParameterDefinition.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of ParameterDefinition

NOTE: Do not initialize this class manually. Instances of it are available on StreamTimeseriesConsumer.definitions

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net ParameterDefinition.

<a id="quixstreams.models.autooffsetreset"></a>

# quixstreams.models.autooffsetreset

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset"></a>

## AutoOffsetReset Objects

```python
class AutoOffsetReset(Enum)
```

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset.Latest"></a>

#### Latest

Latest, starts from newest message if there is no stored offset

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset.Earliest"></a>

#### Earliest

Earliest, starts from the oldest message if there is no stored offset

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset.Error"></a>

#### Error

Error, throws exception if there is no stored offset

<a id="quixstreams.models.timeseriesdataraw"></a>

# quixstreams.models.timeseriesdataraw

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw"></a>

## TimeseriesDataRaw Objects

```python
@nativedecorator
class TimeseriesDataRaw(object)
```

Describes timeseries data in a raw format for multiple timestamps. Class is intended for read only.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesDataRaw.

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net TimeseriesDataRaw.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.to_dataframe"></a>

#### to\_dataframe

```python
def to_dataframe() -> pd.DataFrame
```

Converts TimeseriesDataRaw to pandas DataFrame

**Returns**:

Converted pandas DataFrame

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.from_dataframe"></a>

#### from\_dataframe

```python
@staticmethod
def from_dataframe(data_frame: pd.DataFrame,
                   epoch: int = 0) -> 'TimeseriesDataRaw'
```

Converts from pandas DataFrame to TimeseriesDataRaw

**Arguments**:

- `data_frame`: The pandas DataFrame to convert to TimeseriesData
- `epoch`: The epoch to add to each time value when converting to TimeseriesData. Defaults to 0

**Returns**:

Converted TimeseriesData

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.set_values"></a>

#### set\_values

```python
def set_values(epoch: int, timestamps: [int],
               numeric_values: Dict[str, List[float]],
               string_values: Dict[str, List[str]],
               binary_values: Dict[str,
                                   List[bytes]], tag_values: Dict[str,
                                                                  List[str]])
```

Sets the values of the timeseries data from the provided dictionaries

Dictionary values are matched by index to the provided timestamps

**Arguments**:

- `epoch`: The time from which all timestamps are measured from
- `timestamps`: The timestamps of values in nanoseconds since epoch as an array
- `numeric_values`: the numeric values where the dictionary key is the parameter name and the value is the array of values
- `string_values`: the string values where the dictionary key is the parameter name and the value is the array of values
- `binary_values`: the binary values where the dictionary key is the parameter name and the value is the array of values
- `tag_values`: the tag values where the dictionary key is the parameter name and the value is the array of values

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.epoch"></a>

#### epoch

```python
@property
def epoch() -> int
```

The unix epoch from, which all other timestamps in this model are measured from in nanoseconds.
0 = UNIX epoch (01/01/1970)

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.timestamps"></a>

#### timestamps

```python
@property
def timestamps() -> [int]
```

The timestamps of values in nanoseconds since epoch.
Timestamps are matched by index to numeric_values, string_values, binary_values and tag_values

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.numeric_values"></a>

#### numeric\_values

```python
@property
def numeric_values() -> Dict[str, List[Optional[float]]]
```

The numeric values for parameters.
The key is the parameter Id the values belong to
The value is the numerical values of the parameter. Values are matched by index to timestamps.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.string_values"></a>

#### string\_values

```python
@property
def string_values() -> Dict[str, List[str]]
```

The string values for parameters.
The key is the parameter Id the values belong to
The value is the string values of the parameter. Values are matched by index to timestamps.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.binary_values"></a>

#### binary\_values

```python
@property
def binary_values() -> Dict[str, List[bytes]]
```

The binary values for parameters.
The key is the parameter Id the values belong to
The value is the binary values of the parameter. Values are matched by index to timestamps

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.tag_values"></a>

#### tag\_values

```python
@property
def tag_values() -> Dict[str, List[str]]
```

The tag values for parameters.
The key is the parameter Id the values belong to
The value is the tag values of the parameter. Values are matched by index to timestamps

<a id="quixstreams.models.netlist"></a>

# quixstreams.models.netlist

<a id="quixstreams.models.netlist.NetReadOnlyList"></a>

## NetReadOnlyList Objects

```python
class NetReadOnlyList(object)
```

Experimental. Acts as a proxy between a .net collection and a python list. Useful if .net collection is observable and reacts to changes

<a id="quixstreams.models.netlist.NetList"></a>

## NetList Objects

```python
class NetList(NetReadOnlyList)
```

Experimental. Acts as a proxy between a .net collection and a python list. Useful if .net collection is observable and reacts to changes

<a id="quixstreams.models.netlist.NetList.constructor_for_string"></a>

#### constructor\_for\_string

```python
@staticmethod
def constructor_for_string(net_pointer=None)
```

Creates an empty dotnet list for strings  if no pointer provided, else wraps in NetDict with string converters

<a id="quixstreams.models.parametervalue"></a>

# quixstreams.models.parametervalue

<a id="quixstreams.models.parametervalue.ParameterValue"></a>

## ParameterValue Objects

```python
@nativedecorator
class ParameterValue(object)
```

Represents a single parameter value of either string or numeric type

<a id="quixstreams.models.parametervalue.ParameterValue.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of ParameterValue.

**Arguments**:

  
- `net_object` _.net object_ - The .net object representing ParameterValue.

<a id="quixstreams.models.parametervalue.ParameterValue.numeric_value"></a>

#### numeric\_value

```python
@property
def numeric_value() -> float
```

Gets the numeric value of the parameter, if the underlying parameter is of numeric type

<a id="quixstreams.models.parametervalue.ParameterValue.numeric_value"></a>

#### numeric\_value

```python
@numeric_value.setter
def numeric_value(value: float)
```

Sets the numeric value of the parameter and updates the type to numeric

<a id="quixstreams.models.parametervalue.ParameterValue.string_value"></a>

#### string\_value

```python
@property
def string_value() -> str
```

Gets the string value of the parameter, if the underlying parameter is of string type

<a id="quixstreams.models.parametervalue.ParameterValue.string_value"></a>

#### string\_value

```python
@string_value.setter
def string_value(value: str)
```

Sets the string value of the parameter and updates the type to string

<a id="quixstreams.models.parametervalue.ParameterValue.binary_value"></a>

#### binary\_value

```python
@property
def binary_value() -> bytes
```

Gets the binary value of the parameter, if the underlying parameter is of binary type

<a id="quixstreams.models.parametervalue.ParameterValue.binary_value"></a>

#### binary\_value

```python
@binary_value.setter
def binary_value(value: Union[bytearray, bytes])
```

Sets the binary value of the parameter and updates the type to binary

<a id="quixstreams.models.parametervalue.ParameterValue.type"></a>

#### type

```python
@property
def type() -> ParameterValueType
```

Gets the type of value, which is numeric or string if set, else empty

<a id="quixstreams.models.parametervalue.ParameterValue.value"></a>

#### value

```python
@property
def value()
```

Gets the underlying value

<a id="quixstreams.models.timeseriesdata"></a>

# quixstreams.models.timeseriesdata

<a id="quixstreams.models.timeseriesdata.TimeseriesData"></a>

## TimeseriesData Objects

```python
@nativedecorator
class TimeseriesData(object)
```

Describes timeseries data for multiple timestamps

<a id="quixstreams.models.timeseriesdata.TimeseriesData.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesData.

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net TimeseriesData.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.clone"></a>

#### clone

```python
def clone(parameter_filter: [str] = None)
```

Initializes a new instance of timeseries data with parameters matching the filter if one is provided

Parameters:

**Arguments**:

- `parameter_filter`: The parameter filter. If one is provided, only parameters present in the list will be cloned

<a id="quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp"></a>

#### add\_timestamp

```python
def add_timestamp(time: Union[datetime, timedelta]) -> TimeseriesDataTimestamp
```

Start adding a new set parameters and their tags at the specified time

**Arguments**:

- `time`: The time to use for adding new event values.
| datetime: The datetime to use for adding new event values. Epoch will never be added to this
| timedelta: The time since the default epoch to add the event values at

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp_milliseconds"></a>

#### add\_timestamp\_milliseconds

```python
def add_timestamp_milliseconds(milliseconds: int) -> TimeseriesDataTimestamp
```

Start adding a new set parameters and their tags at the specified time

**Arguments**:

- `milliseconds`: The time in milliseconds since the default epoch to add the event values at

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp_nanoseconds"></a>

#### add\_timestamp\_nanoseconds

```python
def add_timestamp_nanoseconds(nanoseconds: int) -> TimeseriesDataTimestamp
```

Start adding a new set parameters and their tags at the specified time

**Arguments**:

- `nanoseconds`: The time in nanoseconds since the default epoch to add the event values at

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdata.TimeseriesData.timestamps"></a>

#### timestamps

```python
@property
def timestamps() -> List[TimeseriesDataTimestamp]
```

Gets the data as rows of TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdata.TimeseriesData.timestamps"></a>

#### timestamps

```python
@timestamps.setter
def timestamps(timestamp_list: List[TimeseriesDataTimestamp]) -> None
```

Sets the data as rows of TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdata.TimeseriesData.to_dataframe"></a>

#### to\_dataframe

```python
def to_dataframe() -> pd.DataFrame
```

Converts TimeseriesData to pandas DataFrame

**Returns**:

Converted pandas DataFrame

<a id="quixstreams.models.timeseriesdata.TimeseriesData.from_panda_dataframe"></a>

#### from\_panda\_dataframe

```python
@staticmethod
def from_panda_dataframe(data_frame: pd.DataFrame,
                         epoch: int = 0) -> 'TimeseriesData'
```

Converts pandas DataFrame to TimeseriesData

**Arguments**:

- `data_frame`: The pandas DataFrame to convert to TimeseriesData
- `epoch`: The epoch to add to each time value when converting to TimeseriesData. Defaults to 0

**Returns**:

Converted TimeseriesData

<a id="quixstreams.models.eventdefinition"></a>

# quixstreams.models.eventdefinition

<a id="quixstreams.models.eventdefinition.EventDefinition"></a>

## EventDefinition Objects

```python
class EventDefinition(object)
```

Describes additional context for the event

<a id="quixstreams.models.eventdefinition.EventDefinition.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of EventDefinition

NOTE: Do not initialize this class manually. Instances of it are available on StreamEventsConsumer.definitions

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net EventDefinition

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer"></a>

# quixstreams.models.streamconsumer.streamtimeseriesconsumer

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer"></a>

## StreamTimeseriesConsumer Objects

```python
@nativedecorator
class StreamTimeseriesConsumer(object)
```

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamTimeseriesConsumer.
NOTE: Do not initialize this class manually, use StreamConsumer.parameters to access an instance of it

**Arguments**:

  
- `stream_consumer` - The stream the buffer is created for
- `net_pointer` _.net object_ - Pointer to an instance of a .net StreamTimeseriesConsumer

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_data_received"></a>

#### on\_data\_received

```python
@property
def on_data_received() -> Callable[['StreamConsumer', TimeseriesData], None]
```

Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_data_received"></a>

#### on\_data\_received

```python
@on_data_received.setter
def on_data_received(
        value: Callable[['StreamConsumer', TimeseriesData], None]) -> None
```

Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_raw_received"></a>

#### on\_raw\_received

```python
@property
def on_raw_received() -> Callable[['StreamConsumer', TimeseriesDataRaw], None]
```

Gets the handler for when the stream receives data. First parameter is the data is received for, second is the data in TimeseriesDataRaw format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_raw_received"></a>

#### on\_raw\_received

```python
@on_raw_received.setter
def on_raw_received(
        value: Callable[['StreamConsumer', TimeseriesDataRaw], None]) -> None
```

Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesDataRaw format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_dataframe_received"></a>

#### on\_dataframe\_received

```python
@property
def on_dataframe_received(
) -> Callable[['StreamConsumer', pandas.DataFrame], None]
```

Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in pandas DataFrame format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_dataframe_received"></a>

#### on\_dataframe\_received

```python
@on_dataframe_received.setter
def on_dataframe_received(
        value: Callable[['StreamConsumer', pandas.DataFrame], None]) -> None
```

Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in pandas DataFrame format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@property
def on_definitions_changed() -> Callable[['StreamConsumer'], None]
```

Gets the handler for when the stream definitions change. First parameter is the stream the parameter definitions changed for.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@on_definitions_changed.setter
def on_definitions_changed(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for when the stream definitions change. First parameter is the stream the parameter definitions changed for.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.definitions"></a>

#### definitions

```python
@property
def definitions() -> List[ParameterDefinition]
```

Gets the latest set of parameter definitions

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.create_buffer"></a>

#### create\_buffer

```python
def create_buffer(
    *parameter_filter: str,
    buffer_configuration: TimeseriesBufferConfiguration = None
) -> TimeseriesBufferConsumer
```

Creates a new buffer for reading data according to the provided parameter_filter and buffer_configuration

**Arguments**:

- `parameter_filter`: 0 or more parameter identifier to filter as a whitelist. If provided, only these
parameters will be available through this buffer
- `buffer_configuration`: an optional TimeseriesBufferConfiguration.

**Returns**:

a TimeseriesBufferConsumer which will raise new data read via .on_data_released event

<a id="quixstreams.models.streamconsumer.streameventsconsumer"></a>

# quixstreams.models.streamconsumer.streameventsconsumer

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer"></a>

## StreamEventsConsumer Objects

```python
@nativedecorator
class StreamEventsConsumer(object)
```

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamEventsConsumer.
NOTE: Do not initialize this class manually, use StreamConsumer.events to access an instance of it

**Arguments**:

  
- `stream_consumer` - The stream the buffer is created for
- `net_pointer` _.net object_ - Pointer to an instance of a .net StreamEventsConsumer

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received"></a>

#### on\_data\_received

```python
@property
def on_data_received() -> Callable[['StreamConsumer', EventData], None]
```

Gets the handler for when the stream receives event. First parameter is the stream the event is received for, second is the event.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received"></a>

#### on\_data\_received

```python
@on_data_received.setter
def on_data_received(
        value: Callable[['StreamConsumer', EventData], None]) -> None
```

Sets the handler for when the stream receives event. First parameter is the stream the event is received for, second is the event.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@property
def on_definitions_changed() -> Callable[['StreamConsumer'], None]
```

Gets the handler for when the stream definitions change. First parameter is the stream the event definitions changed for.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@on_definitions_changed.setter
def on_definitions_changed(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for when the stream definitions change. First parameter is the stream the event definitions changed for.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.definitions"></a>

#### definitions

```python
@property
def definitions() -> List[EventDefinition]
```

Gets the latest set of event definitions

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer"></a>

# quixstreams.models.streamconsumer.timeseriesbufferconsumer

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer"></a>

## TimeseriesBufferConsumer Objects

```python
class TimeseriesBufferConsumer(TimeseriesBuffer)
```

Class used to write to StreamProducer in a buffered manner

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer, net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesBufferConsumer.
NOTE: Do not initialize this class manually, use StreamTimeseriesConsumer.create_buffer to create it

**Arguments**:

- `stream_consumer` - The stream the buffer is created for
- `net_pointer` - Pointer to an instance of a .net TimeseriesBufferConsumer

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer"></a>

# quixstreams.models.streamconsumer.streampropertiesconsumer

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer"></a>

## StreamPropertiesConsumer Objects

```python
@nativedecorator
class StreamPropertiesConsumer(object)
```

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer: 'StreamConsumer', net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamPropertiesConsumer.
NOTE: Do not initialize this class manually, use StreamConsumer.properties to access an instance of it

**Arguments**:

  
- `stream_consumer` - The stream the buffer is created for
- `net_pointer` - Pointer to an instance of a .net StreamPropertiesConsumer

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.on_changed"></a>

#### on\_changed

```python
@property
def on_changed() -> Callable[['StreamConsumer'], None]
```

Gets the handler for when the stream properties changed. First parameter is the stream it is invoked for.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.on_changed"></a>

#### on\_changed

```python
@on_changed.setter
def on_changed(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for when the stream properties changed. First parameter is the stream it is invoked for.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.name"></a>

#### name

```python
@property
def name() -> str
```

Gets the name of the stream

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.location"></a>

#### location

```python
@property
def location() -> str
```

Gets the location of the stream

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.time_of_recording"></a>

#### time\_of\_recording

```python
@property
def time_of_recording() -> datetime
```

Gets the datetime of the recording

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.metadata"></a>

#### metadata

```python
@property
def metadata() -> Dict[str, str]
```

Gets the metadata of the stream

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.parents"></a>

#### parents

```python
@property
def parents() -> List[str]
```

Gets The ids of streams this stream is derived from

<a id="quixstreams.models.streamconsumer"></a>

# quixstreams.models.streamconsumer

<a id="quixstreams.models.commitmode"></a>

# quixstreams.models.commitmode

<a id="quixstreams.models"></a>

# quixstreams.models

<a id="quixstreams.models.netdict"></a>

# quixstreams.models.netdict

<a id="quixstreams.models.netdict.ReadOnlyNetDict"></a>

## ReadOnlyNetDict Objects

```python
class ReadOnlyNetDict(object)
```

Experimental. Acts as a proxy between a .net dictionary and a python dict. Useful if .net dictionary is observable and reacts to changes

<a id="quixstreams.models.netdict.NetDict"></a>

## NetDict Objects

```python
class NetDict(ReadOnlyNetDict)
```

Experimental. Acts as a proxy between a .net dictionary and a python list.

<a id="quixstreams.models.netdict.NetDict.constructor_for_string_string"></a>

#### constructor\_for\_string\_string

```python
@staticmethod
def constructor_for_string_string(net_pointer=None)
```

Creates an empty dotnet list for strings if no pointer provided, else wraps in NetDict with string converters

<a id="quixstreams.models.timeseriesbuffer"></a>

# quixstreams.models.timeseriesbuffer

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer"></a>

## TimeseriesBuffer Objects

```python
@nativedecorator
class TimeseriesBuffer(object)
```

Class used for buffering parameters
When none of the buffer conditions are configured, the buffer does not buffer at all

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TimeseriesBuffer.
NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

**Arguments**:

  
- `topic` - The topic the stream for which this buffer is created for belongs to
- `stream` - The stream the buffer is created for
- `net_object` _.net object_ - The .net object representing a TimeseriesBuffer

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_data_released"></a>

#### on\_data\_released

```python
@property
def on_data_released() -> Callable[
    [Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]
```

Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_data_released"></a>

#### on\_data\_released

```python
@on_data_released.setter
def on_data_released(
    value: Callable[
        [Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]
) -> None
```

Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_raw_released"></a>

#### on\_raw\_released

```python
@property
def on_raw_released() -> Callable[
    [Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]
```

Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_raw_released"></a>

#### on\_raw\_released

```python
@on_raw_released.setter
def on_raw_released(
    value: Callable[
        [Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]
) -> None
```

Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_dataframe_released"></a>

#### on\_dataframe\_released

```python
@property
def on_dataframe_released() -> Callable[
    [Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]
```

Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_dataframe_released"></a>

#### on\_dataframe\_released

```python
@on_dataframe_released.setter
def on_dataframe_released(
    value: Callable[
        [Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]
) -> None
```

Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.filter"></a>

#### filter

```python
@property
def filter() -> Callable[[TimeseriesDataTimestamp], bool]
```

Gets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.filter"></a>

#### filter

```python
@filter.setter
def filter(value: Callable[[TimeseriesDataTimestamp], bool])
```

Sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.custom_trigger"></a>

#### custom\_trigger

```python
@property
def custom_trigger() -> Callable[[TimeseriesData], bool]
```

Gets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_received is invoked with the entire buffer content
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.custom_trigger"></a>

#### custom\_trigger

```python
@custom_trigger.setter
def custom_trigger(value: Callable[[TimeseriesData], bool])
```

Sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_received is invoked with the entire buffer content
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.packet_size"></a>

#### packet\_size

```python
@property
def packet_size() -> Optional[int]
```

Gets the max packet size in terms of values for the buffer. Each time the buffer has this amount
of data the on_data_released event is invoked and the data is cleared from the buffer.
Defaults to None (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.packet_size"></a>

#### packet\_size

```python
@packet_size.setter
def packet_size(value: Optional[int])
```

Sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
of data the on_data_released event is invoked and the data is cleared from the buffer.
Defaults to None (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@property
def time_span_in_nanoseconds() -> Optional[int]
```

Gets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@time_span_in_nanoseconds.setter
def time_span_in_nanoseconds(value: Optional[int])
```

Sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@property
def time_span_in_milliseconds() -> Optional[int]
```

Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).
Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@time_span_in_milliseconds.setter
def time_span_in_milliseconds(value: Optional[int])
```

Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).
Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.buffer_timeout"></a>

#### buffer\_timeout

```python
@property
def buffer_timeout() -> Optional[int]
```

Gets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
on_data_released event is trigger when the configured value has elapsed or other buffer condition is met.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.buffer_timeout"></a>

#### buffer\_timeout

```python
@buffer_timeout.setter
def buffer_timeout(value: Optional[int])
```

Sets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
on_data_released event is trigger when the configured value has elapsed or other buffer condition is met.
Defaults to none (disabled).

<a id="quixstreams.models.commitoptions"></a>

# quixstreams.models.commitoptions

<a id="quixstreams.models.commitoptions.CommitOptions"></a>

## CommitOptions Objects

```python
@nativedecorator
class CommitOptions(object)
```

<a id="quixstreams.models.commitoptions.CommitOptions.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of CommitOptions

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net CommitOptions.

<a id="quixstreams.models.commitoptions.CommitOptions.auto_commit_enabled"></a>

#### auto\_commit\_enabled

```python
@property
def auto_commit_enabled() -> bool
```

Gets whether automatic committing is enabled.
If automatic committing is not enabled, other values are ignored.
Default is True.

<a id="quixstreams.models.commitoptions.CommitOptions.auto_commit_enabled"></a>

#### auto\_commit\_enabled

```python
@auto_commit_enabled.setter
def auto_commit_enabled(value: bool) -> None
```

Sets whether automatic committing is enabled.
If automatic committing is not enabled, other values are ignored.
Default is True.

<a id="quixstreams.models.commitoptions.CommitOptions.commit_interval"></a>

#### commit\_interval

```python
@property
def commit_interval() -> Optional[int]
```

Gets the interval of automatic commit in ms. Default is 5000.

<a id="quixstreams.models.commitoptions.CommitOptions.commit_interval"></a>

#### commit\_interval

```python
@commit_interval.setter
def commit_interval(value: Optional[int]) -> None
```

Sets the interval of automatic commit in ms. Default is 5000.

<a id="quixstreams.models.commitoptions.CommitOptions.commit_every"></a>

#### commit\_every

```python
@property
def commit_every() -> Optional[int]
```

Gets the number of messages to automatically commit at. Default is 5000.

<a id="quixstreams.models.commitoptions.CommitOptions.commit_every"></a>

#### commit\_every

```python
@commit_every.setter
def commit_every(value: Optional[int]) -> None
```

Sets the number of messages to automatically commit at. Default is 5000.

<a id="quixstreams.models.timeseriesdatatimestamp"></a>

# quixstreams.models.timeseriesdatatimestamp

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp"></a>

## TimeseriesDataTimestamp Objects

```python
@nativedecorator
class TimeseriesDataTimestamp()
```

Represents a single point in time with parameter values and tags attached to that time

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TimeseriesDataTimestamp.

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net TimeseriesDataTimestamp.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.parameters"></a>

#### parameters

```python
@property
def parameters() -> Dict[str, ParameterValue]
```

Parameter values for the timestamp. When a key is not found, returns empty ParameterValue
The dictionary key is the parameter id
The dictionary value is the value (ParameterValue)

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.tags"></a>

#### tags

```python
@property
def tags() -> Dict[str, str]
```

Tags for the timestamp.
The dictionary key is the tag id
The dictionary value is the tag value

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_nanoseconds"></a>

#### timestamp\_nanoseconds

```python
@property
def timestamp_nanoseconds() -> int
```

Gets timestamp in nanoseconds

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_milliseconds"></a>

#### timestamp\_milliseconds

```python
@property
def timestamp_milliseconds() -> int
```

Gets timestamp in milliseconds

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp"></a>

#### timestamp

```python
@property
def timestamp() -> datetime
```

Gets the timestamp in datetime format

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_as_time_span"></a>

#### timestamp\_as\_time\_span

```python
@property
def timestamp_as_time_span() -> timedelta
```

Gets the timestamp in timespan format

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_value"></a>

#### add\_value

```python
def add_value(
    parameter_id: str, value: Union[float, str, int, bytearray,
                                    bytes]) -> 'TimeseriesDataTimestamp'
```

Adds a new value for the parameter

:param parameter_id: The parameter to add the value for
    :param value: the value to add. Can be float or string

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.remove_value"></a>

#### remove\_value

```python
def remove_value(parameter_id: str) -> 'TimeseriesDataTimestamp'
```

Removes the value for the parameter

:param parameter_id: The parameter to remove the value for

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, tag_value: str) -> 'TimeseriesDataTimestamp'
```

Adds a tag to the values

:param tag_id: The id of the tag to the set the value for
    :param tag_value: the value to set

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.remove_tag"></a>

#### remove\_tag

```python
def remove_tag(tag_id: str) -> 'TimeseriesDataTimestamp'
```

Removes a tag from the values

:param tag_id: The id of the tag to remove

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'TimeseriesDataTimestamp'
```

Copies the tags from the specified dictionary. Conflicting tags will be overwritten

:param tags: The tags to add

**Returns**:

TimeseriesDataTimestamp

<a id="quixstreams.models.streamproducer.streameventsproducer"></a>

# quixstreams.models.streamproducer.streameventsproducer

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer"></a>

## StreamEventsProducer Objects

```python
@nativedecorator
class StreamEventsProducer(object)
```

Group all the Events properties, builders and helpers that allow to stream event values and event definitions to the platform.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamEventsProducer.

Events:

net_pointer: Pointer to an instance of a .net StreamEventsProducer.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately writes the event definitions from the buffer without waiting for buffer condition to fulfill (200ms timeout) `TODO` verify 200ms

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_tags"></a>

#### default\_tags

```python
@property
def default_tags() -> Dict[str, str]
```

Gets default tags injected to all Events Values sent by the writer.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_location"></a>

#### default\_location

```python
@property
def default_location() -> str
```

Gets the default Location of the events. Event definitions added with add_definition  will be inserted at this location.
See add_location for adding definitions at a different location without changing default.
Example: "/Group1/SubGroup2"

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_location"></a>

#### default\_location

```python
@default_location.setter
def default_location(value: str)
```

Sets the default Location of the events. Event definitions added with add_definition will be inserted at this location.
See add_location for adding definitions at a different location without changing default.
Example: "/Group1/SubGroup2"

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.epoch"></a>

#### epoch

```python
@property
def epoch() -> datetime
```

Gets the default epoch used for event values

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.epoch"></a>

#### epoch

```python
@epoch.setter
def epoch(value: datetime)
```

Sets the default epoch used for event values

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.publish"></a>

#### publish

```python
def publish(data: Union[EventData, pd.DataFrame], **columns) -> None
```

Publishes event into the stream.

Parameters: data: EventData object or a pandas dataframe. columns: Column names if the dataframe has
different columns from 'id', 'timestamp' and 'value'. For instance if 'id' is in the column 'event_id',
id='event_id' must be passed as an argument.

**Raises**:

  TypeError if the data argument is neither an EventData nor pandas dataframe.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp"></a>

#### add\_timestamp

```python
def add_timestamp(time: Union[datetime, timedelta]) -> EventDataBuilder
```

Start adding a new set of event values at the given timestamp.

**Arguments**:

- `time`: The time to use for adding new event values.
| datetime: The datetime to use for adding new event values. NOTE, epoch is not used
| timedelta: The time since the default epoch to add the event values at

**Returns**:

EventDataBuilder

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp_milliseconds"></a>

#### add\_timestamp\_milliseconds

```python
def add_timestamp_milliseconds(milliseconds: int) -> EventDataBuilder
```

Start adding a new set of event values at the given timestamp.

**Arguments**:

- `milliseconds`: The time in milliseconds since the default epoch to add the event values at

**Returns**:

EventDataBuilder

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp_nanoseconds"></a>

#### add\_timestamp\_nanoseconds

```python
def add_timestamp_nanoseconds(nanoseconds: int) -> EventDataBuilder
```

Start adding a new set of event values at the given timestamp.

**Arguments**:

- `nanoseconds`: The time in nanoseconds since the default epoch to add the event values at

**Returns**:

EventDataBuilder

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_definition"></a>

#### add\_definition

```python
def add_definition(event_id: str,
                   name: str = None,
                   description: str = None) -> EventDefinitionBuilder
```

Add new event definition to the StreamPropertiesProducer. Configure it with the builder methods.

**Arguments**:

- `event_id`: The id of the event. Must match the event id used to send data.
- `name`: The human friendly display name of the event
- `description`: The description of the event

**Returns**:

EventDefinitionBuilder to define properties of the event or add additional events

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_location"></a>

#### add\_location

```python
def add_location(location: str) -> EventDefinitionBuilder
```

Add a new location in the events groups hierarchy

**Arguments**:

- `location`: The group location

**Returns**:

EventDefinitionBuilder to define the events under the specified location

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer"></a>

# quixstreams.models.streamproducer.streamtimeseriesproducer

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer"></a>

## StreamTimeseriesProducer Objects

```python
@nativedecorator
class StreamTimeseriesProducer(object)
```

Group all the Parameters properties, builders and helpers that allow to stream parameter values and parameter definitions to the platform.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_producer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamTimeseriesProducer.

**Arguments**:

  
- `stream_producer` - The stream the writer is created for
- `net_pointer` - Pointer to an instance of a .net StreamTimeseriesProducer.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.flush"></a>

#### flush

```python
def flush()
```

Flushes the timeseries data and definitions from the buffer without waiting for buffer condition to fulfill for either

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.add_definition"></a>

#### add\_definition

```python
def add_definition(parameter_id: str,
                   name: str = None,
                   description: str = None) -> ParameterDefinitionBuilder
```

Add new parameter definition to the StreamPropertiesProducer. Configure it with the builder methods.

**Arguments**:

- `parameter_id`: The id of the parameter. Must match the parameter id used to send data.
- `name`: The human friendly display name of the parameter
- `description`: The description of the parameter

**Returns**:

ParameterDefinitionBuilder to define properties of the parameter or add additional parameters

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.add_location"></a>

#### add\_location

```python
def add_location(location: str) -> ParameterDefinitionBuilder
```

Add a new location in the parameters groups hierarchy

**Arguments**:

- `location`: The group location

**Returns**:

ParameterDefinitionBuilder to define the parameters under the specified location

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.default_location"></a>

#### default\_location

```python
@property
def default_location() -> str
```

Gets the default Location of the parameters. Parameter definitions added with add_definition  will be inserted at this location.
See add_location for adding definitions at a different location without changing default.
Example: "/Group1/SubGroup2"

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.default_location"></a>

#### default\_location

```python
@default_location.setter
def default_location(value: str)
```

Sets the default Location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
See add_location for adding definitions at a different location without changing default.
Example: "/Group1/SubGroup2"

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.buffer"></a>

#### buffer

```python
@property
def buffer() -> TimeseriesBufferProducer
```

Get the buffer for writing timeseries data

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.publish"></a>

#### publish

```python
def publish(
        packet: Union[TimeseriesData, pd.DataFrame,
                      TimeseriesDataRaw]) -> None
```

Publishes the given packet to the stream without any buffering.

**Arguments**:

- `packet`: The packet containing TimeseriesData or panda DataFrame
packet type panda.DataFrame
    Note 1: panda data frame should contain 'time' label, else the first integer label will be taken as time.

    Note 2: Tags should be prefixed by TAG__ or they will be treated as parameters

    Examples
    -------
    Send a panda data frame
         pdf = panda.DataFrame({'time': [1, 5],
         'panda_param': [123.2, 5]})

         instance.publish(pdf)

    Send a panda data frame with multiple values
         pdf = panda.DataFrame({'time': [1, 5, 10],
         'panda_param': [123.2, None, 12],
         'panda_param2': ["val1", "val2", None])

         instance.publish(pdf)

    Send a panda data frame with tags
         pdf = panda.DataFrame({'time': [1, 5, 10],
         'panda_param': [123.2, 5, 12],,
         'TAG__Tag1': ["v1", 2, None],
         'TAG__Tag2': [1, None, 3]})

         instance.publish(pdf)

for other type examples see the specific type

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer"></a>

# quixstreams.models.streamproducer.timeseriesbufferproducer

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer"></a>

## TimeseriesBufferProducer Objects

```python
@nativedecorator
class TimeseriesBufferProducer(TimeseriesBuffer)
```

Class used to write to StreamProducer in a buffered manner

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_producer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TimeseriesBufferProducer.
NOTE: Do not initialize this class manually, use StreamTimeseriesProducer.buffer to access an instance of it

**Arguments**:

  
- `stream_producer` - The stream the buffer is created for
- `net_pointer` - Pointer to an instance of a .net TimeseriesBufferProducer

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.default_tags"></a>

#### default\_tags

```python
@property
def default_tags() -> Dict[str, str]
```

Get default tags injected to all Parameters Values sent by the writer.

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.epoch"></a>

#### epoch

```python
@property
def epoch() -> datetime
```

Get the default epoch used for parameter values

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.epoch"></a>

#### epoch

```python
@epoch.setter
def epoch(value: datetime)
```

Set the default epoch used for parameter values

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.add_timestamp"></a>

#### add\_timestamp

```python
def add_timestamp(time: Union[datetime, timedelta]) -> TimeseriesDataBuilder
```

Start adding a new set of parameter values at the given timestamp.

**Arguments**:

- `time`: The time to use for adding new parameter values.
| datetime: The datetime to use for adding new parameter values. NOTE, epoch is not used
| timedelta: The time since the default epoch to add the parameter values at

**Returns**:

TimeseriesDataBuilder

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.add_timestamp_nanoseconds"></a>

#### add\_timestamp\_nanoseconds

```python
def add_timestamp_nanoseconds(nanoseconds: int) -> TimeseriesDataBuilder
```

Start adding a new set of parameter values at the given timestamp.

**Arguments**:

- `nanoseconds`: The time in nanoseconds since the default epoch to add the parameter values at

**Returns**:

TimeseriesDataBuilder

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately writes the data from the buffer without waiting for buffer condition to fulfill

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.publish"></a>

#### publish

```python
def publish(packet: Union[TimeseriesData, pd.DataFrame]) -> None
```

Publishes the given packet to the stream without any buffering.

**Arguments**:

- `packet`: The packet containing TimeseriesData or panda DataFrame
packet type panda.DataFrame
    Note 1: panda data frame should contain 'time' label, else the first integer label will be taken as time.

    Note 2: Tags should be prefixed by TAG__ or they will be treated as parameters

    Examples
    -------
    Send a panda data frame
         pdf = panda.DataFrame({'time': [1, 5],
         'panda_param': [123.2, 5]})

         instance.publish(pdf)

    Send a panda data frame with multiple values
         pdf = panda.DataFrame({'time': [1, 5, 10],
         'panda_param': [123.2, None, 12],
         'panda_param2': ["val1", "val2", None])

         instance.publish(pdf)

    Send a panda data frame with tags
         pdf = panda.DataFrame({'time': [1, 5, 10],
         'panda_param': [123.2, 5, 12],,
         'TAG__Tag1': ["v1", 2, None],
         'TAG__Tag2': [1, None, 3]})

         instance.publish(pdf)

for other type examples see the specific type

<a id="quixstreams.models.streamproducer"></a>

# quixstreams.models.streamproducer

<a id="quixstreams.models.streamproducer.streampropertiesproducer"></a>

# quixstreams.models.streamproducer.streampropertiesproducer

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer"></a>

## StreamPropertiesProducer Objects

```python
@nativedecorator
class StreamPropertiesProducer(object)
```

Provides additional context for the stream

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamPropertiesProducer.

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net StreamPropertiesProducer.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.name"></a>

#### name

```python
@property
def name() -> str
```

Gets the human friendly name of the stream

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.name"></a>

#### name

```python
@name.setter
def name(value: str)
```

Sets the human friendly name of the stream

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.location"></a>

#### location

```python
@property
def location() -> str
```

Gets the location of the stream in data catalogue. For example: /cars/ai/carA/.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.location"></a>

#### location

```python
@location.setter
def location(value: str)
```

Sets the location of the stream in data catalogue. For example: /cars/ai/carA/.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.metadata"></a>

#### metadata

```python
@property
def metadata() -> Dict[str, str]
```

Gets the metadata of the stream

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.parents"></a>

#### parents

```python
@property
def parents() -> List[str]
```

Gets The ids of streams this stream is derived from

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.time_of_recording"></a>

#### time\_of\_recording

```python
@property
def time_of_recording() -> datetime
```

Gets the datetime of the recording

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.time_of_recording"></a>

#### time\_of\_recording

```python
@time_of_recording.setter
def time_of_recording(value: datetime)
```

Sets the time of recording for the stream. Commonly set to utc now.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush_interval"></a>

#### flush\_interval

```python
@property
def flush_interval() -> int
```

Get automatic flush interval of the properties metadata into the channel [ in milliseconds ]
Defaults to 30000.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush_interval"></a>

#### flush\_interval

```python
@flush_interval.setter
def flush_interval(value: int)
```

Set automatic flush interval of the properties metadata into the channel [ in milliseconds ]

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately writes the properties yet to be sent instead of waiting for the flush timer (20ms)

<a id="quixstreams.models.streamendtype"></a>

# quixstreams.models.streamendtype

<a id="quixstreams.models.timeseriesbufferconfiguration"></a>

# quixstreams.models.timeseriesbufferconfiguration

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration"></a>

## TimeseriesBufferConfiguration Objects

```python
@nativedecorator
class TimeseriesBufferConfiguration(object)
```

Describes the configuration for parameter buffers
When none of the buffer conditions are configured, the buffer immediate invokes the on_data_released

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesBufferConfiguration.

**Arguments**:

- `_net_object`: Can be ignored, here for internal purposes .net object: The .net object representing a TimeseriesBufferConfiguration.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.packet_size"></a>

#### packet\_size

```python
@property
def packet_size() -> Optional[int]
```

Gets the max packet size in terms of values for the buffer. Each time the buffer has this amount
of data the on_data_released event is invoked and the data is cleared from the buffer.
Defaults to None (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.packet_size"></a>

#### packet\_size

```python
@packet_size.setter
def packet_size(value: Optional[int])
```

Sets the max packet size in terms of values for the buffer. Each time the buffer has this amount
of data the on_data_released event is invoked and the data is cleared from the buffer.
Defaults to None (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@property
def time_span_in_nanoseconds() -> Optional[int]
```

Gets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@time_span_in_nanoseconds.setter
def time_span_in_nanoseconds(value: Optional[int])
```

Sets the maximum time between timestamps for the buffer in nanoseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@property
def time_span_in_milliseconds() -> Optional[int]
```

Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).
Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@time_span_in_milliseconds.setter
def time_span_in_milliseconds(value: Optional[int])
```

Gets the maximum time between timestamps for the buffer in milliseconds. When the difference between the
earliest and latest buffered timestamp surpasses this number the on_data_released event
is invoked and the data is cleared from the buffer.
Defaults to none (disabled).
Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with same underlying value.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.buffer_timeout"></a>

#### buffer\_timeout

```python
@property
def buffer_timeout() -> Optional[int]
```

Gets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
on_data_released event is triggered when the configured value has elapsed or other buffer condition is met.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.buffer_timeout"></a>

#### buffer\_timeout

```python
@buffer_timeout.setter
def buffer_timeout(value: Optional[int])
```

Sets the maximum duration in milliseconds for which the buffer will be held before triggering on_data_released event.
on_data_released event is triggered when the configured value has elapsed or other buffer condition is met.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger_before_enqueue"></a>

#### custom\_trigger\_before\_enqueue

```python
@property
def custom_trigger_before_enqueue(
) -> Callable[[TimeseriesDataTimestamp], bool]
```

Gets the custom function which is invoked before adding the timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked before adding the timestamp to it.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger_before_enqueue"></a>

#### custom\_trigger\_before\_enqueue

```python
@custom_trigger_before_enqueue.setter
def custom_trigger_before_enqueue(value: Callable[[TimeseriesDataTimestamp],
                                                  bool])
```

Sets the custom function which is invoked before adding the timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked before adding the timestamp to it.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.filter"></a>

#### filter

```python
@property
def filter() -> Callable[[TimeseriesDataTimestamp], bool]
```

Gets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.filter"></a>

#### filter

```python
@filter.setter
def filter(value: Callable[[TimeseriesDataTimestamp], bool])
```

Sets the custom function to filter the incoming data before adding it to the buffer. If returns true, data is added otherwise not.
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger"></a>

#### custom\_trigger

```python
@property
def custom_trigger() -> Callable[[TimeseriesData], bool]
```

Gets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked with the entire buffer content
Defaults to none (disabled).

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger"></a>

#### custom\_trigger

```python
@custom_trigger.setter
def custom_trigger(value: Callable[[TimeseriesData], bool])
```

Sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, TimeseriesBuffer.on_data_released is invoked with the entire buffer content
Defaults to none (disabled).

<a id="quixstreams.models.eventdata"></a>

# quixstreams.models.eventdata

<a id="quixstreams.models.eventdata.EventData"></a>

## EventData Objects

```python
@nativedecorator
class EventData(object)
```

Represents a single point in time with event value and tags attached to it

<a id="quixstreams.models.eventdata.EventData.__init__"></a>

#### \_\_init\_\_

```python
def __init__(event_id: str = None,
             time: Union[int, str, datetime, pd.Timestamp] = None,
             value: str = None,
             net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of EventData.

Parameters:

**Arguments**:

- `event_id`: the unique id of the event the value belongs to
- `time`: the time at which the event has occurred in nanoseconds since epoch or as a datetime
- `value`: the value of the event
- `net_pointer`: Pointer to an instance of a .net EventData

<a id="quixstreams.models.eventdata.EventData.id"></a>

#### id

```python
@property
def id() -> str
```

Gets the globally unique identifier of the event

<a id="quixstreams.models.eventdata.EventData.id"></a>

#### id

```python
@id.setter
def id(value: str) -> None
```

Sets the globally unique identifier of the event

<a id="quixstreams.models.eventdata.EventData.value"></a>

#### value

```python
@property
def value() -> str
```

Gets the value of the event

<a id="quixstreams.models.eventdata.EventData.value"></a>

#### value

```python
@value.setter
def value(value: str) -> None
```

Sets the value of the event

<a id="quixstreams.models.eventdata.EventData.tags"></a>

#### tags

```python
@property
def tags() -> Dict[str, str]
```

Tags for the timestamp. When key is not found, returns None
The dictionary key is the tag id
The dictionary value is the tag value

<a id="quixstreams.models.eventdata.EventData.timestamp_nanoseconds"></a>

#### timestamp\_nanoseconds

```python
@property
def timestamp_nanoseconds() -> int
```

Gets timestamp in nanoseconds

<a id="quixstreams.models.eventdata.EventData.timestamp_milliseconds"></a>

#### timestamp\_milliseconds

```python
@property
def timestamp_milliseconds() -> int
```

Gets timestamp in milliseconds

<a id="quixstreams.models.eventdata.EventData.timestamp"></a>

#### timestamp

```python
@property
def timestamp() -> datetime
```

Gets the timestamp in datetime format

<a id="quixstreams.models.eventdata.EventData.timestamp_as_time_span"></a>

#### timestamp\_as\_time\_span

```python
@property
def timestamp_as_time_span() -> timedelta
```

Gets the timestamp in timespan format

<a id="quixstreams.models.eventdata.EventData.clone"></a>

#### clone

```python
def clone()
```

Clones the event data

<a id="quixstreams.models.eventdata.EventData.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, tag_value: str) -> 'EventData'
```

Adds a tag to the event

:param tag_id: The id of the tag to the set the value for
    :param tag_value: the value to set

**Returns**:

EventData

<a id="quixstreams.models.eventdata.EventData.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'EventData'
```

Adds the tags from the specified dictionary. Conflicting tags will be overwritten

:param tags: The tags to add

**Returns**:

EventData

<a id="quixstreams.models.eventdata.EventData.remove_tag"></a>

#### remove\_tag

```python
def remove_tag(tag_id: str) -> 'EventData'
```

Removes a tag from the event

:param tag_id: The id of the tag to remove

**Returns**:

EventData

<a id="quixstreams.models.eventlevel"></a>

# quixstreams.models.eventlevel

<a id="quixstreams.raw.rawtopicconsumer"></a>

# quixstreams.raw.rawtopicconsumer

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer"></a>

## RawTopicConsumer Objects

```python
@nativedecorator
class RawTopicConsumer(object)
```

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamConsumer.

NOTE: Do not initialize this class manually, use StreamingClient.on_stream_received to read streams

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net RawTopicConsumer

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received"></a>

#### on\_message\_received

```python
@property
def on_message_received() -> Callable[['RawTopicConsumer', RawMessage], None]
```

Gets the handler for when topic receives message. First parameter is the topic the message is received for, second is the RawMessage.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received"></a>

#### on\_message\_received

```python
@on_message_received.setter
def on_message_received(
        value: Callable[['RawTopicConsumer', RawMessage], None]) -> None
```

Sets the handler for when topic receives message. First parameter is the topic the message is received for, second is the RawMessage.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred"></a>

#### on\_error\_occurred

```python
@property
def on_error_occurred() -> Callable[['RawTopicConsumer', BaseException], None]
```

Gets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the topic
 the error is received for, second is the exception.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred"></a>

#### on\_error\_occurred

```python
@on_error_occurred.setter
def on_error_occurred(
        value: Callable[['RawTopicConsumer', BaseException], None]) -> None
```

Sets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the topic
 the error is received for, second is the exception.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.subscribe"></a>

#### subscribe

```python
def subscribe()
```

Starts subscribing to the streams

<a id="quixstreams.raw.rawmessage"></a>

# quixstreams.raw.rawmessage

<a id="quixstreams.raw.rawmessage.RawMessage"></a>

## RawMessage Objects

```python
@nativedecorator
class RawMessage(object)
```

Class to hold the raw value being read from the message broker

<a id="quixstreams.raw.rawmessage.RawMessage.key"></a>

#### key

```python
@property
def key() -> bytes
```

Get the optional key of the message. Depending on broker and message it is not guaranteed

<a id="quixstreams.raw.rawmessage.RawMessage.key"></a>

#### key

```python
@key.setter
def key(value: Union[bytearray, bytes])
```

Set the message key

<a id="quixstreams.raw.rawmessage.RawMessage.value"></a>

#### value

```python
@property
def value()
```

Get message value (bytes content of message)

<a id="quixstreams.raw.rawmessage.RawMessage.value"></a>

#### value

```python
@value.setter
def value(value: Union[bytearray, bytes])
```

Set message value (bytes content of message)

<a id="quixstreams.raw.rawmessage.RawMessage.metadata"></a>

#### metadata

```python
@property
def metadata() -> Dict[str, str]
```

Get the default Epoch used for Parameters and Events

<a id="quixstreams.raw"></a>

# quixstreams.raw

<a id="quixstreams.raw.rawtopicproducer"></a>

# quixstreams.raw.rawtopicproducer

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer"></a>

## RawTopicProducer Objects

```python
@nativedecorator
class RawTopicProducer(object)
```

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of RawTopicProducer

**Arguments**:

  
- `net_pointer` _c_void_p_ - Pointer to an instance of a .net RawTopicProducer

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer.publish"></a>

#### publish

```python
def publish(message: Union[RawMessage, bytes, bytearray])
```

Publish the packet to the output topic.

params:
(message): either bytes, bytearray or instance of RawMessage

<a id="quixstreams.state.localfilestorage"></a>

# quixstreams.state.localfilestorage

<a id="quixstreams.state.localfilestorage.LocalFileStorage"></a>

## LocalFileStorage Objects

```python
class LocalFileStorage(object)
```

<a id="quixstreams.state.localfilestorage.LocalFileStorage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(storage_directory=None, auto_create_dir=True)
```

Initializes the LocalFileStorage containing the whole

<a id="quixstreams.state.localfilestorage.LocalFileStorage.get"></a>

#### get

```python
def get(key: str) -> any
```

Get value at key
param key:
return: one of
    str
    int
    float
    bool
    bytes
    bytearray
    object (via pickle)

<a id="quixstreams.state.localfilestorage.LocalFileStorage.set"></a>

#### set

```python
def set(key: str, value: any)
```

Set value at the key
param key:
param value:
    StateValue
    str
    int
    float
    bool
    bytes
    bytearray
    object (via pickle)

<a id="quixstreams.state.localfilestorage.LocalFileStorage.contains_key"></a>

#### contains\_key

```python
def contains_key(key: str) -> bool
```

Check whether the storage contains the key

<a id="quixstreams.state.localfilestorage.LocalFileStorage.get_all_keys"></a>

#### get\_all\_keys

```python
def get_all_keys()
```

Get set containing all the keys inside storage

<a id="quixstreams.state.localfilestorage.LocalFileStorage.remove"></a>

#### remove

```python
def remove(key) -> None
```

Remove key from the storage

<a id="quixstreams.state.localfilestorage.LocalFileStorage.clear"></a>

#### clear

```python
def clear()
```

Clear storage ( remove all the keys )

<a id="quixstreams.state.statevalue"></a>

# quixstreams.state.statevalue

<a id="quixstreams.state.statevalue.StateValue"></a>

## StateValue Objects

```python
class StateValue(object)
```

<a id="quixstreams.state.statevalue.StateValue.__init__"></a>

#### \_\_init\_\_

```python
def __init__(value: any)
```

Initialize the boxed value inside the store

Parameter
    value one of the types:
        StateValue
        str
        int
        float
        bool
        bytes
        bytearray
        object (via pickle)

<a id="quixstreams.state.statevalue.StateValue.type"></a>

#### type

```python
@property
def type()
```

Get the state type

<a id="quixstreams.state.statevalue.StateValue.value"></a>

#### value

```python
@property
def value()
```

Get wrapped value

<a id="quixstreams.state"></a>

# quixstreams.state

<a id="quixstreams.state.inmemorystorage"></a>

# quixstreams.state.inmemorystorage

<a id="quixstreams.state.inmemorystorage.InMemoryStorage"></a>

## InMemoryStorage Objects

```python
class InMemoryStorage()
```

In memory storage with an optional backing store

<a id="quixstreams.state.statetype"></a>

# quixstreams.state.statetype

<a id="quixstreams.app"></a>

# quixstreams.app

<a id="quixstreams.app.CancellationTokenSource"></a>

## CancellationTokenSource Objects

```python
class CancellationTokenSource()
```

Signals to a System.Threading.CancellationToken that it should be canceled.

<a id="quixstreams.app.CancellationTokenSource.__init__"></a>

#### \_\_init\_\_

```python
def __init__()
```

Creates a new instance of CancellationTokenSource

<a id="quixstreams.app.App"></a>

## App Objects

```python
class App()
```

Helper class to handle default streaming behaviors and handle automatic resource cleanup on shutdown

<a id="quixstreams.app.App.run"></a>

#### run

```python
@staticmethod
def run(cancellation_token: CancellationToken = None,
        before_shutdown: Callable[[], None] = None)
```

Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown

It also ensures input topics defined at the time of invocation are opened for read.

**Arguments**:

- `cancellation_token`: An optional cancellation token to abort the application run with
- `before_shutdown`: An optional function to call before shutting down

<a id="quixstreams.topicproducer"></a>

# quixstreams.topicproducer

<a id="quixstreams.topicproducer.TopicProducer"></a>

## TopicProducer Objects

```python
@nativedecorator
class TopicProducer(object)
```

Interface to operate with the streaming platform for reading or writing

<a id="quixstreams.topicproducer.TopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TopicProducer.
NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

**Arguments**:

  
- `net_object` _.net object_ - The .net object representing a StreamingClient

<a id="quixstreams.topicproducer.TopicProducer.on_disposed"></a>

#### on\_disposed

```python
@property
def on_disposed() -> Callable[['TopicProducer'], None]
```

Gets the handler for when the topic is disposed. First parameter is the topic which got disposed.

<a id="quixstreams.topicproducer.TopicProducer.on_disposed"></a>

#### on\_disposed

```python
@on_disposed.setter
def on_disposed(value: Callable[['TopicProducer'], None]) -> None
```

Sets the handler for when the topic is disposed. First parameter is the topic which got disposed.

<a id="quixstreams.topicproducer.TopicProducer.create_stream"></a>

#### create\_stream

```python
def create_stream(stream_id: str = None) -> StreamProducer
```

Create new stream and returns the related stream writer to operate it.

**Arguments**:

  
- `stream_id` _string_ - Optional, provide if you wish to overwrite the generated stream id. Useful if you wish
  to always stream a certain source into the same stream

<a id="quixstreams.topicproducer.TopicProducer.get_stream"></a>

#### get\_stream

```python
def get_stream(stream_id: str) -> StreamProducer
```

Retrieves a stream that was previously created by this instance, if the stream is not closed.

**Arguments**:

  
- `stream_id` _string_ - The id of the stream

<a id="quixstreams.topicproducer.TopicProducer.get_or_create_stream"></a>

#### get\_or\_create\_stream

```python
def get_or_create_stream(
    stream_id: str,
    on_stream_created: Callable[[StreamProducer],
                                None] = None) -> StreamProducer
```

Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream.

**Arguments**:

  
- `stream_id` _string_ - The Id of the stream you want to get or create
- `on_stream_created` _Callable[[StreamProducer], None]_ - A void callback taking StreamProducer

<a id="quixstreams.streamconsumer"></a>

# quixstreams.streamconsumer

<a id="quixstreams.streamconsumer.StreamConsumer"></a>

## StreamConsumer Objects

```python
@nativedecorator
class StreamConsumer(object)
```

Handles reading stream from a topic

<a id="quixstreams.streamconsumer.StreamConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p, topic_consumer: 'TopicConsumer',
             on_close_cb_always: Callable[['StreamConsumer'], None])
```

Initializes a new instance of StreamConsumer.

NOTE: Do not initialize this class manually, use StreamingClient.on_stream_received to read streams
topic_consumer: The topic consumer the stream consumer is created by

**Arguments**:

- `net_pointer`: Pointer to an instance of a .net StreamConsumer

<a id="quixstreams.streamconsumer.StreamConsumer.topic"></a>

#### topic

```python
@property
def topic() -> 'TopicConsumer'
```

Gets the topic the stream was raised for

<a id="quixstreams.streamconsumer.StreamConsumer.on_stream_closed"></a>

#### on\_stream\_closed

```python
@property
def on_stream_closed() -> Callable[['StreamConsumer', 'StreamEndType'], None]
```

Gets the handler for when the stream closes. First parameter is the stream which closes, second is the close type.

<a id="quixstreams.streamconsumer.StreamConsumer.on_stream_closed"></a>

#### on\_stream\_closed

```python
@on_stream_closed.setter
def on_stream_closed(
        value: Callable[['StreamConsumer', 'StreamEndType'], None]) -> None
```

Sets the handler for when the stream closes. First parameter is the stream which closes, second is the close type.

<a id="quixstreams.streamconsumer.StreamConsumer.on_package_received"></a>

#### on\_package\_received

```python
@property
def on_package_received() -> Callable[['StreamConsumer', any], None]
```

Gets the handler for when the stream receives a package of any type. First parameter is the stream which receives it, second is the package.

<a id="quixstreams.streamconsumer.StreamConsumer.on_package_received"></a>

#### on\_package\_received

```python
@on_package_received.setter
def on_package_received(
        value: Callable[['StreamConsumer', any], None]) -> None
```

Sets the handler for when the stream receives a package of any type. First parameter is the stream which receives it, second is the package.

<a id="quixstreams.streamconsumer.StreamConsumer.stream_id"></a>

#### stream\_id

```python
@property
def stream_id() -> str
```

Get the id of the stream being read

<a id="quixstreams.streamconsumer.StreamConsumer.properties"></a>

#### properties

```python
@property
def properties() -> StreamPropertiesConsumer
```

Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values

<a id="quixstreams.streamconsumer.StreamConsumer.events"></a>

#### events

```python
@property
def events() -> StreamEventsConsumer
```

Gets the consumer for accessing event related information of the stream such as event definitions and values

<a id="quixstreams.streamconsumer.StreamConsumer.timeseries"></a>

#### timeseries

```python
@property
def timeseries() -> StreamTimeseriesConsumer
```

Gets the reader for accessing parameter related information of the stream such as definitions and parameter values

<a id="quixstreams.kafkastreamingclient"></a>

# quixstreams.kafkastreamingclient

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient"></a>

## KafkaStreamingClient Objects

```python
@nativedecorator
class KafkaStreamingClient(object)
```

Class that is capable of creating input and output topics for reading and writing

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(broker_address: str,
             security_options: SecurityOptions = None,
             properties: Dict[str, str] = None,
             debug: bool = False)
```

Creates a new instance of KafkaStreamingClient that is capable of creating input and output topics for reading and writing

**Arguments**:

  
- `brokerAddress` _string_ - Address of Kafka cluster
  
- `security_options` _string_ - Optional security options
  
- `properties` - Optional extra properties for broker configuration
  
- `debug` _string_ - Whether debugging should be enabled

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_consumer"></a>

#### get\_topic\_consumer

```python
def get_topic_consumer(
    topic: str,
    consumer_group: str = None,
    commit_settings: Union[CommitOptions, CommitMode] = None,
    auto_offset_reset: AutoOffsetReset = AutoOffsetReset.Latest
) -> TopicConsumer
```

Opens an input topic capable of reading incoming streams

**Arguments**:

  
- `topic` _string_ - Name of the topic
  
- `consumer_group` _string_ - The consumer group id to use for consuming messages
  
- `commit_settings` _CommitOptions, CommitMode_ - the settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.
  
- `auto_offset_reset` _AutoOffsetReset_ - The offset to use when there is no saved offset for the consumer group. Defaults to latest

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_producer"></a>

#### get\_topic\_producer

```python
def get_topic_producer(topic: str) -> TopicProducer
```

Opens an output topic capable of sending outgoing streams

**Arguments**:

  
- `topic` _string_ - Name of the topic

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_consumer"></a>

#### get\_raw\_topic\_consumer

```python
def get_raw_topic_consumer(
    topic: str,
    consumer_group: str = None,
    auto_offset_reset: Union[AutoOffsetReset,
                             None] = None) -> RawTopicConsumer
```

Opens an input topic for reading raw data from the stream

**Arguments**:

  
- `topic` _string_ - Name of the topic
- `consumer_group` _string_ - Consumer group ( optional )

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_producer"></a>

#### get\_raw\_topic\_producer

```python
def get_raw_topic_producer(topic: str) -> RawTopicProducer
```

Opens an input topic for writing raw data to the stream

**Arguments**:

  
- `topic` _string_ - Name of the topic

<a id="quixstreams.streamproducer"></a>

# quixstreams.streamproducer

<a id="quixstreams.streamproducer.StreamProducer"></a>

## StreamProducer Objects

```python
@nativedecorator
class StreamProducer(object)
```

Handles writing stream to a topic

<a id="quixstreams.streamproducer.StreamProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(topic_producer: 'TopicProducer', net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamProducer.
NOTE: Do not initialize this class manually, use StreamingClient.create_stream to write streams

**Arguments**:

- `topic_producer` - The topic producer the stream producer publishes to
- `net_object` _.net object_ - The .net object representing a StreamProducer

<a id="quixstreams.streamproducer.StreamProducer.topic"></a>

#### topic

```python
@property
def topic() -> 'TopicProducer'
```

Gets the topic the stream is writing to

<a id="quixstreams.streamproducer.StreamProducer.on_write_exception"></a>

#### on\_write\_exception

```python
@property
def on_write_exception() -> Callable[['StreamProducer', BaseException], None]
```

Gets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the stream
 is received for, second is the exception.

<a id="quixstreams.streamproducer.StreamProducer.on_write_exception"></a>

#### on\_write\_exception

```python
@on_write_exception.setter
def on_write_exception(
        value: Callable[['StreamProducer', BaseException], None]) -> None
```

Sets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the stream
 is received for, second is the exception.

<a id="quixstreams.streamproducer.StreamProducer.stream_id"></a>

#### stream\_id

```python
@property
def stream_id() -> str
```

Gets the unique id the stream being written

<a id="quixstreams.streamproducer.StreamProducer.epoch"></a>

#### epoch

```python
@property
def epoch() -> datetime
```

Gets the default Epoch used for Parameters and Events

<a id="quixstreams.streamproducer.StreamProducer.epoch"></a>

#### epoch

```python
@epoch.setter
def epoch(value: datetime)
```

Set the default Epoch used for Parameters and Events

<a id="quixstreams.streamproducer.StreamProducer.properties"></a>

#### properties

```python
@property
def properties() -> StreamPropertiesProducer
```

Properties of the stream. The changes will automatically be sent after a slight delay

<a id="quixstreams.streamproducer.StreamProducer.timeseries"></a>

#### timeseries

```python
@property
def timeseries() -> StreamTimeseriesProducer
```

Gets the producer for publishing timeseries related information of the stream such as parameter definitions and values

<a id="quixstreams.streamproducer.StreamProducer.events"></a>

#### events

```python
@property
def events() -> StreamEventsProducer
```

Gets the producer for publishing event related information of the stream such as event definitions and values

<a id="quixstreams.streamproducer.StreamProducer.close"></a>

#### close

```python
def close(end_type: StreamEndType = StreamEndType.Closed)
```

Close the stream and flush the pending data to stream

<a id="quixstreams.helpers.timeconverter"></a>

# quixstreams.helpers.timeconverter

<a id="quixstreams.helpers.enumconverter"></a>

# quixstreams.helpers.enumconverter

<a id="quixstreams.helpers.dotnet.datetimeconverter"></a>

# quixstreams.helpers.dotnet.datetimeconverter

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter"></a>

## DateTimeConverter Objects

```python
class DateTimeConverter()
```

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_python"></a>

#### datetime\_to\_python

```python
@staticmethod
def datetime_to_python(hptr: ctypes.c_void_p) -> datetime.datetime
```

Converts dotnet pointer to DateTime and frees the pointer

Parameters
----------

hptr: c_void_p
    Handler Pointer to .Net type DateTime

Returns
-------

datetime.datetime:
    Python type datetime

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_dotnet"></a>

#### datetime\_to\_dotnet

```python
@staticmethod
def datetime_to_dotnet(value: datetime.datetime) -> ctypes.c_void_p
```

Parameters
----------

value: datetime.datetime
    Python type datetime

Returns
-------

ctypes.c_void_p:
    Handler Pointer to .Net type DateTime

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timespan_to_python"></a>

#### timespan\_to\_python

```python
@staticmethod
def timespan_to_python(uptr: ctypes.c_void_p) -> datetime.timedelta
```

Converts dotnet pointer to Timespan as binary and frees the pointer

Parameters
----------

uptr: c_void_p
    Pointer to .Net type TimeSpan

Returns
-------

datetime.timedelta:
    Python type timedelta

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timedelta_to_dotnet"></a>

#### timedelta\_to\_dotnet

```python
@staticmethod
def timedelta_to_dotnet(value: datetime.timedelta) -> ctypes.c_void_p
```

Parameters
----------

value: datetime.timedelta
    Python type timedelta

Returns
-------

ctypes.c_void_p:
    Pointer to unmanaged memory containing TimeSpan

<a id="quixstreams.helpers.nativedecorator"></a>

# quixstreams.helpers.nativedecorator

<a id="quixstreams.helpers"></a>

# quixstreams.helpers

<a id="quixstreams.topicconsumer"></a>

# quixstreams.topicconsumer

<a id="quixstreams.topicconsumer.TopicConsumer"></a>

## TopicConsumer Objects

```python
@nativedecorator
class TopicConsumer(object)
```

Interface to operate with the streaming platform for reading or writing

<a id="quixstreams.topicconsumer.TopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamingClient.
NOTE: Do not initialize this class manually, use StreamingClient.create_input to create it

**Arguments**:

  
- `net_pointer` _.net pointer_ - The .net pointer to TopicConsumer instance

<a id="quixstreams.topicconsumer.TopicConsumer.on_stream_received"></a>

#### on\_stream\_received

```python
@property
def on_stream_received() -> Callable[['StreamConsumer'], None]
```

Gets the handler for when a stream is received for the topic. First parameter is the stream.

<a id="quixstreams.topicconsumer.TopicConsumer.on_stream_received"></a>

#### on\_stream\_received

```python
@on_stream_received.setter
def on_stream_received(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for when a stream is received for the topic. First parameter is the stream.

<a id="quixstreams.topicconsumer.TopicConsumer.on_streams_revoked"></a>

#### on\_streams\_revoked

```python
@property
def on_streams_revoked(
) -> Callable[['TopicConsumer', List['StreamConsumer']], None]
```

Gets the handler for when a stream is received for the topic. First parameter is the topic the stream is received for, second is the streams revoked.

<a id="quixstreams.topicconsumer.TopicConsumer.on_streams_revoked"></a>

#### on\_streams\_revoked

```python
@on_streams_revoked.setter
def on_streams_revoked(
        value: Callable[['TopicConsumer', List['StreamConsumer']],
                        None]) -> None
```

Sets the handler for when a stream is received for the topic. First parameter is the topic the stream is received for, second is the streams revoked.

<a id="quixstreams.topicconsumer.TopicConsumer.on_revoking"></a>

#### on\_revoking

```python
@property
def on_revoking() -> Callable[['TopicConsumer'], None]
```

Gets the handler for when topic is revoking. First parameter is the topic the revocation is happening for.

<a id="quixstreams.topicconsumer.TopicConsumer.on_revoking"></a>

#### on\_revoking

```python
@on_revoking.setter
def on_revoking(value: Callable[['TopicConsumer'], None]) -> None
```

Sets the handler for when topic is revoking. First parameter is the topic the revocation is happening for.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committed"></a>

#### on\_committed

```python
@property
def on_committed() -> Callable[['TopicConsumer'], None]
```

Gets the handler for when the topic finished committing data read up to this point. First parameter is the topic the commit happened for.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committed"></a>

#### on\_committed

```python
@on_committed.setter
def on_committed(value: Callable[['TopicConsumer'], None]) -> None
```

Sets the handler for when the topic finished committing data read up to this point. First parameter is the topic the commit happened for.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committing"></a>

#### on\_committing

```python
@property
def on_committing() -> Callable[['TopicConsumer'], None]
```

Gets the handler for when the topic beginning to commit data read up to this point. First parameter is the topic the commit is happening for.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committing"></a>

#### on\_committing

```python
@on_committing.setter
def on_committing(value: Callable[['TopicConsumer'], None]) -> None
```

Sets the handler for when the topic beginning to commit data read up to this point. First parameter is the topic the commit is happening for.

<a id="quixstreams.topicconsumer.TopicConsumer.subscribe"></a>

#### subscribe

```python
def subscribe()
```

Subscribes to streams in the topic
Use 'on_stream_received' event to read incoming streams

<a id="quixstreams.topicconsumer.TopicConsumer.commit"></a>

#### commit

```python
def commit()
```

Commit packages read up until now

<a id="quixstreams.builders.eventdatabuilder"></a>

# quixstreams.builders.eventdatabuilder

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder"></a>

## EventDataBuilder Objects

```python
@nativedecorator
class EventDataBuilder(object)
```

Builder for creating event data packages for StreamPropertiesProducer

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of EventDataBuilder.

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net EventDataBuilder.

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.add_value"></a>

#### add\_value

```python
def add_value(event_id: str, value: str) -> 'EventDataBuilder'
```

Adds new event at the time the builder is created for

**Arguments**:

- `event_id`: The id of the event to set the value for
- `value`: the string value

**Returns**:

The builder

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, value: str) -> 'EventDataBuilder'
```

Sets tag value for the values

**Arguments**:

- `tag_id`: The id of the tag
- `value`: The value of the tag

**Returns**:

The builder

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'EventDataBuilder'
```

Copies the tags from the specified dictionary. Conflicting tags will be overwritten

:param tags: The tags to add

**Returns**:

the builder

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.publish"></a>

#### publish

```python
def publish()
```

Publishes the values to the StreamEventsProducer buffer. See StreamEventsProducer buffer settings for more information when the values are sent to the broker

<a id="quixstreams.builders.eventdefinitionbuilder"></a>

# quixstreams.builders.eventdefinitionbuilder

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder"></a>

## EventDefinitionBuilder Objects

```python
@nativedecorator
class EventDefinitionBuilder(object)
```

Builder for creating event and group definitions for StreamPropertiesProducer

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of EventDefinitionBuilder.

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net EventDefinitionBuilder.

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_level"></a>

#### set\_level

```python
def set_level(level: EventLevel) -> 'EventDefinitionBuilder'
```

Set the custom properties of the event

**Arguments**:

- `level`: The severity level of the event

**Returns**:

The builder

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_custom_properties"></a>

#### set\_custom\_properties

```python
def set_custom_properties(custom_properties: str) -> 'EventDefinitionBuilder'
```

Set the custom properties of the event

**Arguments**:

- `custom_properties`: The custom properties of the event

**Returns**:

The builder

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.add_definition"></a>

#### add\_definition

```python
def add_definition(event_id: str,
                   name: str = None,
                   description: str = None) -> 'EventDefinitionBuilder'
```

Add new event definition to the StreamPropertiesProducer. Configure it with the builder methods.

**Arguments**:

- `event_id`: The id of the event. Must match the event id used to send data.
- `name`: The human friendly display name of the event
- `description`: The description of the event

**Returns**:

The builder

<a id="quixstreams.builders.parameterdefinitionbuilder"></a>

# quixstreams.builders.parameterdefinitionbuilder

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder"></a>

## ParameterDefinitionBuilder Objects

```python
@nativedecorator
class ParameterDefinitionBuilder(object)
```

Builder for creating parameter and group definitions for StreamPropertiesProducer

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of ParameterDefinitionBuilder.

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net ParameterDefinitionBuilder.

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_range"></a>

#### set\_range

```python
def set_range(minimum_value: float,
              maximum_value: float) -> 'ParameterDefinitionBuilder'
```

Set the minimum and maximum range of the parameter

**Arguments**:

- `minimum_value`: The minimum value
- `maximum_value`: The maximum value

**Returns**:

The builder

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_unit"></a>

#### set\_unit

```python
def set_unit(unit: str) -> 'ParameterDefinitionBuilder'
```

Set the unit of the parameter

**Arguments**:

- `unit`: The unit of the parameter

**Returns**:

The builder

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_format"></a>

#### set\_format

```python
def set_format(format: str) -> 'ParameterDefinitionBuilder'
```

Set the format of the parameter

**Arguments**:

- `format`: The format of the parameter

**Returns**:

The builder

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_custom_properties"></a>

#### set\_custom\_properties

```python
def set_custom_properties(
        custom_properties: str) -> 'ParameterDefinitionBuilder'
```

Set the custom properties of the parameter

**Arguments**:

- `custom_properties`: The custom properties of the parameter

**Returns**:

The builder

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.add_definition"></a>

#### add\_definition

```python
def add_definition(parameter_id: str,
                   name: str = None,
                   description: str = None) -> 'ParameterDefinitionBuilder'
```

Add new parameter definition to the StreamPropertiesProducer. Configure it with the builder methods.

**Arguments**:

- `parameter_id`: The id of the parameter. Must match the parameter id used to send data.
- `name`: The human friendly display name of the parameter
- `description`: The description of the parameter

**Returns**:

The builder

<a id="quixstreams.builders.timeseriesdatabuilder"></a>

# quixstreams.builders.timeseriesdatabuilder

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder"></a>

## TimeseriesDataBuilder Objects

```python
@nativedecorator
class TimeseriesDataBuilder(object)
```

Builder for creating timeseries data packages for StreamPropertiesProducer

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TimeseriesDataBuilder.

**Arguments**:

  
- `net_pointer` - Pointer to an instance of a .net TimeseriesDataBuilder.

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_value"></a>

#### add\_value

```python
def add_value(
    parameter_id: str, value: Union[str, float, int, bytes,
                                    bytearray]) -> 'TimeseriesDataBuilder'
```

Adds new parameter value at the time the builder is created for

**Arguments**:

- `parameter_id`: The id of the parameter to set the value for
- `value`: the value as string or float

**Returns**:

The builder

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, value: str) -> 'TimeseriesDataBuilder'
```

Adds tag value for the values. If

**Arguments**:

- `tag_id`: The id of the tag
- `value`: The value of the tag

**Returns**:

The builder

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'TimeseriesDataBuilder'
```

Copies the tags from the specified dictionary. Conflicting tags will be overwritten

:param tags: The tags to add

**Returns**:

the builder

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.publish"></a>

#### publish

```python
def publish()
```

Publishes the values to the StreamTimeseriesProducer buffer. See StreamTimeseriesProducer buffer settings for more information when the values are sent to the broker

<a id="quixstreams.builders"></a>

# quixstreams.builders

<a id="quixstreams.configuration.saslmechanism"></a>

# quixstreams.configuration.saslmechanism

<a id="quixstreams.configuration.securityoptions"></a>

# quixstreams.configuration.securityoptions

<a id="quixstreams.configuration.securityoptions.SecurityOptions"></a>

## SecurityOptions Objects

```python
class SecurityOptions(object)
```

Kafka security option for configuring authentication

<a id="quixstreams.configuration.securityoptions.SecurityOptions.__init__"></a>

#### \_\_init\_\_

```python
def __init__(ssl_certificates: str,
             username: str,
             password: str,
             sasl_mechanism: SaslMechanism = SaslMechanism.ScramSha256)
```

Create a new instance of SecurityOptions that is configured for SSL encryption with SASL authentication


**Arguments**:

  
- `ssl_certificates` _string_ - The folder/file that contains the certificate authority certificate(s) to validate the ssl connection. Example: "./certificates/ca.cert"
  
- `username` _string_ - The username for the SASL authentication
  
- `password` _string_ - The password for the SASL authentication
  
- `sasl_mechanism` _SaslMechanism_ - The SASL mechanism to use. Defaulting to ScramSha256 for backward compatibility

<a id="quixstreams.configuration"></a>

# quixstreams.configuration

