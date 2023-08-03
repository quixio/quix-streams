# Table of Contents

* [quixstreams](#quixstreams)
* [quixstreams.helpers.dotnet.datetimeconverter](#quixstreams.helpers.dotnet.datetimeconverter)
  * [DateTimeConverter](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter)
    * [datetime\_to\_python](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_python)
    * [datetime\_to\_dotnet](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_dotnet)
    * [timespan\_to\_python](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timespan_to_python)
    * [timedelta\_to\_dotnet](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timedelta_to_dotnet)
* [quixstreams.helpers.defaultdictkeyed](#quixstreams.helpers.defaultdictkeyed)
* [quixstreams.helpers.timeconverter](#quixstreams.helpers.timeconverter)
  * [TimeConverter](#quixstreams.helpers.timeconverter.TimeConverter)
    * [offset\_from\_utc](#quixstreams.helpers.timeconverter.TimeConverter.offset_from_utc)
    * [to\_unix\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.to_unix_nanoseconds)
    * [to\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.to_nanoseconds)
    * [from\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.from_nanoseconds)
    * [from\_unix\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.from_unix_nanoseconds)
    * [from\_string](#quixstreams.helpers.timeconverter.TimeConverter.from_string)
* [quixstreams.helpers.exceptionconverter](#quixstreams.helpers.exceptionconverter)
* [quixstreams.helpers.nativedecorator](#quixstreams.helpers.nativedecorator)
* [quixstreams.helpers](#quixstreams.helpers)
* [quixstreams.helpers.enumconverter](#quixstreams.helpers.enumconverter)
* [quixstreams.raw.rawtopicproducer](#quixstreams.raw.rawtopicproducer)
  * [RawTopicProducer](#quixstreams.raw.rawtopicproducer.RawTopicProducer)
    * [\_\_init\_\_](#quixstreams.raw.rawtopicproducer.RawTopicProducer.__init__)
    * [publish](#quixstreams.raw.rawtopicproducer.RawTopicProducer.publish)
    * [dispose](#quixstreams.raw.rawtopicproducer.RawTopicProducer.dispose)
    * [flush](#quixstreams.raw.rawtopicproducer.RawTopicProducer.flush)
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
    * [\_\_init\_\_](#quixstreams.raw.rawmessage.RawMessage.__init__)
    * [get\_net\_pointer](#quixstreams.raw.rawmessage.RawMessage.get_net_pointer)
    * [key](#quixstreams.raw.rawmessage.RawMessage.key)
    * [key](#quixstreams.raw.rawmessage.RawMessage.key)
    * [value](#quixstreams.raw.rawmessage.RawMessage.value)
    * [value](#quixstreams.raw.rawmessage.RawMessage.value)
    * [metadata](#quixstreams.raw.rawmessage.RawMessage.metadata)
* [quixstreams.raw](#quixstreams.raw)
* [quixstreams.configuration.securityoptions](#quixstreams.configuration.securityoptions)
  * [SecurityOptions](#quixstreams.configuration.securityoptions.SecurityOptions)
    * [\_\_init\_\_](#quixstreams.configuration.securityoptions.SecurityOptions.__init__)
    * [get\_net\_pointer](#quixstreams.configuration.securityoptions.SecurityOptions.get_net_pointer)
* [quixstreams.configuration.saslmechanism](#quixstreams.configuration.saslmechanism)
* [quixstreams.configuration](#quixstreams.configuration)
* [quixstreams.quixstreamingclient](#quixstreams.quixstreamingclient)
  * [TokenValidationConfiguration](#quixstreams.quixstreamingclient.TokenValidationConfiguration)
    * [\_\_init\_\_](#quixstreams.quixstreamingclient.TokenValidationConfiguration.__init__)
    * [enabled](#quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled)
    * [enabled](#quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled)
    * [warning\_before\_expiry](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry)
    * [warning\_before\_expiry](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry)
    * [warn\_about\_pat\_token](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token)
    * [warn\_about\_pat\_token](#quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token)
    * [get\_net\_pointer](#quixstreams.quixstreamingclient.TokenValidationConfiguration.get_net_pointer)
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
    * [get\_net\_pointer](#quixstreams.quixstreamingclient.QuixStreamingClient.get_net_pointer)
* [quixstreams.topicproducer](#quixstreams.topicproducer)
  * [TopicProducer](#quixstreams.topicproducer.TopicProducer)
    * [\_\_init\_\_](#quixstreams.topicproducer.TopicProducer.__init__)
    * [dispose](#quixstreams.topicproducer.TopicProducer.dispose)
    * [flush](#quixstreams.topicproducer.TopicProducer.flush)
    * [on\_disposed](#quixstreams.topicproducer.TopicProducer.on_disposed)
    * [on\_disposed](#quixstreams.topicproducer.TopicProducer.on_disposed)
    * [create\_stream](#quixstreams.topicproducer.TopicProducer.create_stream)
    * [get\_stream](#quixstreams.topicproducer.TopicProducer.get_stream)
    * [get\_or\_create\_stream](#quixstreams.topicproducer.TopicProducer.get_or_create_stream)
* [quixstreams.models.commitoptions](#quixstreams.models.commitoptions)
  * [CommitOptions](#quixstreams.models.commitoptions.CommitOptions)
    * [\_\_init\_\_](#quixstreams.models.commitoptions.CommitOptions.__init__)
    * [auto\_commit\_enabled](#quixstreams.models.commitoptions.CommitOptions.auto_commit_enabled)
    * [auto\_commit\_enabled](#quixstreams.models.commitoptions.CommitOptions.auto_commit_enabled)
    * [commit\_interval](#quixstreams.models.commitoptions.CommitOptions.commit_interval)
    * [commit\_interval](#quixstreams.models.commitoptions.CommitOptions.commit_interval)
    * [commit\_every](#quixstreams.models.commitoptions.CommitOptions.commit_every)
    * [commit\_every](#quixstreams.models.commitoptions.CommitOptions.commit_every)
* [quixstreams.models.eventlevel](#quixstreams.models.eventlevel)
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
    * [get\_net\_pointer](#quixstreams.models.timeseriesdata.TimeseriesData.get_net_pointer)
* [quixstreams.models.eventdefinition](#quixstreams.models.eventdefinition)
  * [EventDefinition](#quixstreams.models.eventdefinition.EventDefinition)
    * [\_\_init\_\_](#quixstreams.models.eventdefinition.EventDefinition.__init__)
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
* [quixstreams.models.parameterdefinition](#quixstreams.models.parameterdefinition)
  * [ParameterDefinition](#quixstreams.models.parameterdefinition.ParameterDefinition)
    * [\_\_init\_\_](#quixstreams.models.parameterdefinition.ParameterDefinition.__init__)
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
    * [get\_net\_pointer](#quixstreams.models.parametervalue.ParameterValue.get_net_pointer)
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
    * [convert\_to\_timeseriesdata](#quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.convert_to_timeseriesdata)
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
    * [get\_net\_pointer](#quixstreams.models.timeseriesbuffer.TimeseriesBuffer.get_net_pointer)
* [quixstreams.models.streampackage](#quixstreams.models.streampackage)
  * [StreamPackage](#quixstreams.models.streampackage.StreamPackage)
    * [\_\_init\_\_](#quixstreams.models.streampackage.StreamPackage.__init__)
    * [transport\_context](#quixstreams.models.streampackage.StreamPackage.transport_context)
    * [to\_json](#quixstreams.models.streampackage.StreamPackage.to_json)
    * [get\_net\_pointer](#quixstreams.models.streampackage.StreamPackage.get_net_pointer)
* [quixstreams.models.codecsettings](#quixstreams.models.codecsettings)
  * [CodecSettings](#quixstreams.models.codecsettings.CodecSettings)
    * [set\_global\_codec\_type](#quixstreams.models.codecsettings.CodecSettings.set_global_codec_type)
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
    * [get\_net\_pointer](#quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.get_net_pointer)
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
    * [get\_net\_pointer](#quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.get_net_pointer)
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
    * [get\_net\_pointer](#quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.get_net_pointer)
* [quixstreams.models.streamconsumer](#quixstreams.models.streamconsumer)
* [quixstreams.models.commitmode](#quixstreams.models.commitmode)
* [quixstreams.models.codectype](#quixstreams.models.codectype)
  * [CodecType](#quixstreams.models.codectype.CodecType)
* [quixstreams.models.netlist](#quixstreams.models.netlist)
  * [NetReadOnlyList](#quixstreams.models.netlist.NetReadOnlyList)
  * [NetList](#quixstreams.models.netlist.NetList)
    * [constructor\_for\_string](#quixstreams.models.netlist.NetList.constructor_for_string)
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
    * [get\_net\_pointer](#quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.get_net_pointer)
* [quixstreams.models.netdict](#quixstreams.models.netdict)
  * [ReadOnlyNetDict](#quixstreams.models.netdict.ReadOnlyNetDict)
  * [NetDict](#quixstreams.models.netdict.NetDict)
    * [constructor\_for\_string\_string](#quixstreams.models.netdict.NetDict.constructor_for_string_string)
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
    * [get\_net\_pointer](#quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.get_net_pointer)
* [quixstreams.models.streamendtype](#quixstreams.models.streamendtype)
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
    * [get\_net\_pointer](#quixstreams.models.eventdata.EventData.get_net_pointer)
* [quixstreams.models](#quixstreams.models)
* [quixstreams.models.autooffsetreset](#quixstreams.models.autooffsetreset)
  * [AutoOffsetReset](#quixstreams.models.autooffsetreset.AutoOffsetReset)
    * [Latest](#quixstreams.models.autooffsetreset.AutoOffsetReset.Latest)
    * [Earliest](#quixstreams.models.autooffsetreset.AutoOffsetReset.Earliest)
    * [Error](#quixstreams.models.autooffsetreset.AutoOffsetReset.Error)
* [quixstreams.app](#quixstreams.app)
  * [CancellationTokenSource](#quixstreams.app.CancellationTokenSource)
    * [\_\_init\_\_](#quixstreams.app.CancellationTokenSource.__init__)
    * [is\_cancellation\_requested](#quixstreams.app.CancellationTokenSource.is_cancellation_requested)
    * [cancel](#quixstreams.app.CancellationTokenSource.cancel)
    * [token](#quixstreams.app.CancellationTokenSource.token)
    * [get\_net\_pointer](#quixstreams.app.CancellationTokenSource.get_net_pointer)
  * [App](#quixstreams.app.App)
    * [run](#quixstreams.app.App.run)
    * [get\_state\_manager](#quixstreams.app.App.get_state_manager)
    * [set\_state\_storage](#quixstreams.app.App.set_state_storage)
* [quixstreams.states.topicstatemanager](#quixstreams.states.topicstatemanager)
  * [TopicStateManager](#quixstreams.states.topicstatemanager.TopicStateManager)
    * [\_\_init\_\_](#quixstreams.states.topicstatemanager.TopicStateManager.__init__)
    * [get\_stream\_states](#quixstreams.states.topicstatemanager.TopicStateManager.get_stream_states)
    * [get\_stream\_state\_manager](#quixstreams.states.topicstatemanager.TopicStateManager.get_stream_state_manager)
    * [delete\_stream\_state](#quixstreams.states.topicstatemanager.TopicStateManager.delete_stream_state)
    * [delete\_stream\_states](#quixstreams.states.topicstatemanager.TopicStateManager.delete_stream_states)
* [quixstreams.states.streamstatemanager](#quixstreams.states.streamstatemanager)
  * [StreamStateManager](#quixstreams.states.streamstatemanager.StreamStateManager)
    * [\_\_init\_\_](#quixstreams.states.streamstatemanager.StreamStateManager.__init__)
    * [get\_dict\_state](#quixstreams.states.streamstatemanager.StreamStateManager.get_dict_state)
    * [get\_scalar\_state](#quixstreams.states.streamstatemanager.StreamStateManager.get_scalar_state)
    * [get\_states](#quixstreams.states.streamstatemanager.StreamStateManager.get_states)
    * [delete\_state](#quixstreams.states.streamstatemanager.StreamStateManager.delete_state)
    * [delete\_states](#quixstreams.states.streamstatemanager.StreamStateManager.delete_states)
* [quixstreams.states.scalarstreamstate](#quixstreams.states.scalarstreamstate)
  * [ScalarStreamState](#quixstreams.states.scalarstreamstate.ScalarStreamState)
    * [\_\_init\_\_](#quixstreams.states.scalarstreamstate.ScalarStreamState.__init__)
    * [type](#quixstreams.states.scalarstreamstate.ScalarStreamState.type)
    * [on\_flushed](#quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushed)
    * [on\_flushed](#quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushed)
    * [on\_flushing](#quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushing)
    * [on\_flushing](#quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushing)
    * [flush](#quixstreams.states.scalarstreamstate.ScalarStreamState.flush)
    * [reset](#quixstreams.states.scalarstreamstate.ScalarStreamState.reset)
    * [value](#quixstreams.states.scalarstreamstate.ScalarStreamState.value)
    * [value](#quixstreams.states.scalarstreamstate.ScalarStreamState.value)
* [quixstreams.states.dictstreamstate](#quixstreams.states.dictstreamstate)
  * [DictStreamState](#quixstreams.states.dictstreamstate.DictStreamState)
    * [\_\_init\_\_](#quixstreams.states.dictstreamstate.DictStreamState.__init__)
    * [type](#quixstreams.states.dictstreamstate.DictStreamState.type)
    * [on\_flushed](#quixstreams.states.dictstreamstate.DictStreamState.on_flushed)
    * [on\_flushed](#quixstreams.states.dictstreamstate.DictStreamState.on_flushed)
    * [on\_flushing](#quixstreams.states.dictstreamstate.DictStreamState.on_flushing)
    * [on\_flushing](#quixstreams.states.dictstreamstate.DictStreamState.on_flushing)
    * [flush](#quixstreams.states.dictstreamstate.DictStreamState.flush)
    * [reset](#quixstreams.states.dictstreamstate.DictStreamState.reset)
* [quixstreams.states](#quixstreams.states)
* [quixstreams.states.appstatemanager](#quixstreams.states.appstatemanager)
  * [AppStateManager](#quixstreams.states.appstatemanager.AppStateManager)
    * [\_\_init\_\_](#quixstreams.states.appstatemanager.AppStateManager.__init__)
    * [get\_topic\_states](#quixstreams.states.appstatemanager.AppStateManager.get_topic_states)
    * [get\_topic\_state\_manager](#quixstreams.states.appstatemanager.AppStateManager.get_topic_state_manager)
    * [delete\_topic\_state](#quixstreams.states.appstatemanager.AppStateManager.delete_topic_state)
    * [delete\_topic\_states](#quixstreams.states.appstatemanager.AppStateManager.delete_topic_states)
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
    * [get\_state\_manager](#quixstreams.topicconsumer.TopicConsumer.get_state_manager)
    * [get\_net\_pointer](#quixstreams.topicconsumer.TopicConsumer.get_net_pointer)
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
    * [get\_dict\_state](#quixstreams.streamconsumer.StreamConsumer.get_dict_state)
    * [get\_scalar\_state](#quixstreams.streamconsumer.StreamConsumer.get_scalar_state)
    * [get\_state\_manager](#quixstreams.streamconsumer.StreamConsumer.get_state_manager)
    * [get\_net\_pointer](#quixstreams.streamconsumer.StreamConsumer.get_net_pointer)
* [quixstreams.state.localfilestorage](#quixstreams.state.localfilestorage)
  * [LocalFileStorage](#quixstreams.state.localfilestorage.LocalFileStorage)
    * [\_\_init\_\_](#quixstreams.state.localfilestorage.LocalFileStorage.__init__)
* [quixstreams.state.inmemorystorage](#quixstreams.state.inmemorystorage)
  * [InMemoryStorage](#quixstreams.state.inmemorystorage.InMemoryStorage)
    * [\_\_init\_\_](#quixstreams.state.inmemorystorage.InMemoryStorage.__init__)
* [quixstreams.state.statetype](#quixstreams.state.statetype)
* [quixstreams.state.statevalue](#quixstreams.state.statevalue)
  * [StateValue](#quixstreams.state.statevalue.StateValue)
    * [\_\_init\_\_](#quixstreams.state.statevalue.StateValue.__init__)
    * [type](#quixstreams.state.statevalue.StateValue.type)
    * [value](#quixstreams.state.statevalue.StateValue.value)
    * [get\_net\_pointer](#quixstreams.state.statevalue.StateValue.get_net_pointer)
* [quixstreams.state.istatestorage](#quixstreams.state.istatestorage)
  * [IStateStorage](#quixstreams.state.istatestorage.IStateStorage)
    * [\_\_init\_\_](#quixstreams.state.istatestorage.IStateStorage.__init__)
    * [is\_case\_sensitive](#quixstreams.state.istatestorage.IStateStorage.is_case_sensitive)
    * [get\_or\_create\_sub\_storage](#quixstreams.state.istatestorage.IStateStorage.get_or_create_sub_storage)
    * [delete\_sub\_storages](#quixstreams.state.istatestorage.IStateStorage.delete_sub_storages)
    * [delete\_sub\_storage](#quixstreams.state.istatestorage.IStateStorage.delete_sub_storage)
    * [get\_sub\_storages](#quixstreams.state.istatestorage.IStateStorage.get_sub_storages)
    * [get](#quixstreams.state.istatestorage.IStateStorage.get)
    * [set](#quixstreams.state.istatestorage.IStateStorage.set)
    * [contains\_key](#quixstreams.state.istatestorage.IStateStorage.contains_key)
    * [get\_all\_keys](#quixstreams.state.istatestorage.IStateStorage.get_all_keys)
    * [remove](#quixstreams.state.istatestorage.IStateStorage.remove)
    * [clear](#quixstreams.state.istatestorage.IStateStorage.clear)
* [quixstreams.state](#quixstreams.state)
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
    * [flush](#quixstreams.streamproducer.StreamProducer.flush)
    * [close](#quixstreams.streamproducer.StreamProducer.close)
* [quixstreams.builders.parameterdefinitionbuilder](#quixstreams.builders.parameterdefinitionbuilder)
  * [ParameterDefinitionBuilder](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder)
    * [\_\_init\_\_](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.__init__)
    * [set\_range](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_range)
    * [set\_unit](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_unit)
    * [set\_format](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_format)
    * [set\_custom\_properties](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_custom_properties)
    * [add\_definition](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.add_definition)
* [quixstreams.builders.eventdatabuilder](#quixstreams.builders.eventdatabuilder)
  * [EventDataBuilder](#quixstreams.builders.eventdatabuilder.EventDataBuilder)
    * [\_\_init\_\_](#quixstreams.builders.eventdatabuilder.EventDataBuilder.__init__)
    * [add\_value](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_value)
    * [add\_tag](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tag)
    * [add\_tags](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tags)
    * [publish](#quixstreams.builders.eventdatabuilder.EventDataBuilder.publish)
* [quixstreams.builders.timeseriesdatabuilder](#quixstreams.builders.timeseriesdatabuilder)
  * [TimeseriesDataBuilder](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder)
    * [\_\_init\_\_](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.__init__)
    * [add\_value](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_value)
    * [add\_tag](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tag)
    * [add\_tags](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tags)
    * [publish](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.publish)
* [quixstreams.builders.eventdefinitionbuilder](#quixstreams.builders.eventdefinitionbuilder)
  * [EventDefinitionBuilder](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder)
    * [\_\_init\_\_](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.__init__)
    * [set\_level](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_level)
    * [set\_custom\_properties](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_custom_properties)
    * [add\_definition](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.add_definition)
* [quixstreams.builders](#quixstreams.builders)
* [quixstreams.kafkastreamingclient](#quixstreams.kafkastreamingclient)
  * [KafkaStreamingClient](#quixstreams.kafkastreamingclient.KafkaStreamingClient)
    * [\_\_init\_\_](#quixstreams.kafkastreamingclient.KafkaStreamingClient.__init__)
    * [get\_topic\_consumer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_consumer)
    * [get\_topic\_producer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_producer)
    * [get\_raw\_topic\_consumer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_consumer)
    * [get\_raw\_topic\_producer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_producer)
* [quixstreams.exceptions.quixapiexception](#quixstreams.exceptions.quixapiexception)
* [quixstreams.logging](#quixstreams.logging)

<a id="quixstreams"></a>

# quixstreams

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

Converts dotnet pointer to DateTime and frees the pointer.

**Arguments**:

- `hptr` - Handler Pointer to .Net type DateTime
  

**Returns**:

  datetime.datetime:
  Python type datetime

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_dotnet"></a>

#### datetime\_to\_dotnet

```python
@staticmethod
def datetime_to_dotnet(value: datetime.datetime) -> ctypes.c_void_p
```

**Arguments**:

- `value` - Python type datetime
  

**Returns**:

  ctypes.c_void_p:
  Handler Pointer to .Net type DateTime

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timespan_to_python"></a>

#### timespan\_to\_python

```python
@staticmethod
def timespan_to_python(uptr: ctypes.c_void_p) -> datetime.timedelta
```

Converts dotnet pointer to Timespan as binary and frees the pointer.

**Arguments**:

- `uptr` - Pointer to .Net type TimeSpan
  

**Returns**:

  datetime.timedelta:
  Python type timedelta

<a id="quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timedelta_to_dotnet"></a>

#### timedelta\_to\_dotnet

```python
@staticmethod
def timedelta_to_dotnet(value: datetime.timedelta) -> ctypes.c_void_p
```

**Arguments**:

- `value` - Python type timedelta
  

**Returns**:

  ctypes.c_void_p:
  Pointer to unmanaged memory containing TimeSpan

<a id="quixstreams.helpers.defaultdictkeyed"></a>

# quixstreams.helpers.defaultdictkeyed

<a id="quixstreams.helpers.timeconverter"></a>

# quixstreams.helpers.timeconverter

<a id="quixstreams.helpers.timeconverter.TimeConverter"></a>

## TimeConverter Objects

```python
class TimeConverter()
```

A utility class for converting between different time representations.

<a id="quixstreams.helpers.timeconverter.TimeConverter.offset_from_utc"></a>

#### offset\_from\_utc

The local time ahead of utc by this amount of nanoseconds

<a id="quixstreams.helpers.timeconverter.TimeConverter.to_unix_nanoseconds"></a>

#### to\_unix\_nanoseconds

```python
@staticmethod
def to_unix_nanoseconds(value: datetime) -> int
```

Converts a datetime object to UNIX timestamp in nanoseconds.

**Arguments**:

- `value` - The datetime object to be converted.
  

**Returns**:

- `int` - The UNIX timestamp in nanoseconds.

<a id="quixstreams.helpers.timeconverter.TimeConverter.to_nanoseconds"></a>

#### to\_nanoseconds

```python
@staticmethod
def to_nanoseconds(value: timedelta) -> int
```

Converts a timedelta object to nanoseconds.

**Arguments**:

- `value` - The timedelta object to be converted.
  

**Returns**:

- `int` - The duration in nanoseconds.

<a id="quixstreams.helpers.timeconverter.TimeConverter.from_nanoseconds"></a>

#### from\_nanoseconds

```python
@staticmethod
def from_nanoseconds(value: int) -> timedelta
```

Converts a duration in nanoseconds to a timedelta object.

**Arguments**:

- `value` - The duration in nanoseconds.
  

**Returns**:

- `timedelta` - The corresponding timedelta object.

<a id="quixstreams.helpers.timeconverter.TimeConverter.from_unix_nanoseconds"></a>

#### from\_unix\_nanoseconds

```python
@staticmethod
def from_unix_nanoseconds(value: int) -> datetime
```

Converts a UNIX timestamp in nanoseconds to a datetime object.

**Arguments**:

- `value` - The UNIX timestamp in nanoseconds.
  

**Returns**:

- `datetime` - The corresponding datetime object.

<a id="quixstreams.helpers.timeconverter.TimeConverter.from_string"></a>

#### from\_string

```python
@staticmethod
def from_string(value: str) -> int
```

Converts a string representation of a timestamp to a UNIX timestamp in nanoseconds.

**Arguments**:

- `value` - The string representation of a timestamp.
  

**Returns**:

- `int` - The corresponding UNIX timestamp in nanoseconds.

<a id="quixstreams.helpers.exceptionconverter"></a>

# quixstreams.helpers.exceptionconverter

<a id="quixstreams.helpers.nativedecorator"></a>

# quixstreams.helpers.nativedecorator

<a id="quixstreams.helpers"></a>

# quixstreams.helpers

<a id="quixstreams.helpers.enumconverter"></a>

# quixstreams.helpers.enumconverter

<a id="quixstreams.raw.rawtopicproducer"></a>

# quixstreams.raw.rawtopicproducer

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer"></a>

## RawTopicProducer Objects

```python
@nativedecorator
class RawTopicProducer(object)
```

Class to produce raw messages into a Topic (capable of producing non-quixstreams messages)

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of the RawTopicProducer class.

**Arguments**:

- `net_pointer` - Pointer to an instance of a .NET RawTopicProducer object.

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer.publish"></a>

#### publish

```python
def publish(message: Union[RawMessage, bytes, bytearray])
```

Publishes the given message to the associated topic producer.

**Arguments**:

- `message` - The message to be published, which can be either
  a RawMessage instance, bytes, or a bytearray.

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer.dispose"></a>

#### dispose

```python
def dispose()
```

Flushes pending messages and disposes underlying resources

<a id="quixstreams.raw.rawtopicproducer.RawTopicProducer.flush"></a>

#### flush

```python
def flush()
```

Flushes pending messages to the broker

<a id="quixstreams.raw.rawtopicconsumer"></a>

# quixstreams.raw.rawtopicconsumer

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer"></a>

## RawTopicConsumer Objects

```python
@nativedecorator
class RawTopicConsumer(object)
```

Topic class to consume incoming raw messages (capable to consuming non-quixstreams messages).

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of RawTopicConsumer.

**Notes**:

  Do not initialize this class manually, use KafkaStreamingClient.get_raw_topic_consumer.
  

**Arguments**:

- `net_pointer` - Pointer to an instance of a .net RawTopicConsumer.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received"></a>

#### on\_message\_received

```python
@property
def on_message_received() -> Callable[['RawTopicConsumer', RawMessage], None]
```

Gets the handler for when a topic receives a message.

**Returns**:

  Callable[[RawTopicConsumer, RawMessage], None]: The event handler for when a topic receives a message.
  The first parameter is the RawTopicConsumer instance for which the message is received, and the second is the RawMessage.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received"></a>

#### on\_message\_received

```python
@on_message_received.setter
def on_message_received(
        value: Callable[['RawTopicConsumer', RawMessage], None]) -> None
```

Sets the handler for when a topic receives a message.

**Arguments**:

- `value` - The new event handler for when a topic receives a message.
  The first parameter is the RawTopicConsumer instance for which the message is received, and the second is the RawMessage.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred"></a>

#### on\_error\_occurred

```python
@property
def on_error_occurred() -> Callable[['RawTopicConsumer', BaseException], None]
```

Gets the handler for when a stream experiences an exception during the asynchronous write process.

**Returns**:

  Callable[[RawTopicConsumer, BaseException], None]: The event handler for when a stream experiences an exception during the asynchronous write process.
  The first parameter is the RawTopicConsumer instance for which the error is received, and the second is the exception.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred"></a>

#### on\_error\_occurred

```python
@on_error_occurred.setter
def on_error_occurred(
        value: Callable[['RawTopicConsumer', BaseException], None]) -> None
```

Sets the handler for when a stream experiences an exception during the asynchronous write process.

**Arguments**:

- `value` - The new handler for when a stream experiences an exception during the asynchronous write process.
  The first parameter is the RawTopicConsumer instance for which the error is received, and the second is the exception.

<a id="quixstreams.raw.rawtopicconsumer.RawTopicConsumer.subscribe"></a>

#### subscribe

```python
def subscribe()
```

Starts subscribing to the streams.

<a id="quixstreams.raw.rawmessage"></a>

# quixstreams.raw.rawmessage

<a id="quixstreams.raw.rawmessage.RawMessage"></a>

## RawMessage Objects

```python
@nativedecorator
class RawMessage(object)
```

The message consumed from topic without any transformation.

<a id="quixstreams.raw.rawmessage.RawMessage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(data: Union[ctypes.c_void_p, bytes, bytearray])
```

Initializes a new instance of RawMessage.

**Arguments**:

- `data` - The raw data to be stored in the message. Must be one of ctypes_c.void_p, bytes, or bytearray.

<a id="quixstreams.raw.rawmessage.RawMessage.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .net object pointer of the RawMessage instance.

**Returns**:

- `ctypes.c_void_p` - The .net object pointer of the RawMessage instance.

<a id="quixstreams.raw.rawmessage.RawMessage.key"></a>

#### key

```python
@property
def key() -> bytes
```

Gets the optional key of the message. Depending on the broker and message, it is not guaranteed.

**Returns**:

- `bytes` - The optional key of the message.

<a id="quixstreams.raw.rawmessage.RawMessage.key"></a>

#### key

```python
@key.setter
def key(value: Union[bytearray, bytes])
```

Sets the message key.

**Arguments**:

- `value` - The key to set for the message.

<a id="quixstreams.raw.rawmessage.RawMessage.value"></a>

#### value

```python
@property
def value()
```

Gets the message value (bytes content of the message).

**Returns**:

  Union[bytearray, bytes]: The message value (bytes content of the message).

<a id="quixstreams.raw.rawmessage.RawMessage.value"></a>

#### value

```python
@value.setter
def value(value: Union[bytearray, bytes])
```

Sets the message value (bytes content of the message).

**Arguments**:

- `value` - The value to set for the message.

<a id="quixstreams.raw.rawmessage.RawMessage.metadata"></a>

#### metadata

```python
@property
def metadata() -> Dict[str, str]
```

Gets the wrapped message metadata.

**Returns**:

  Dict[str, str]: The wrapped message metadata.

<a id="quixstreams.raw"></a>

# quixstreams.raw

<a id="quixstreams.configuration.securityoptions"></a>

# quixstreams.configuration.securityoptions

<a id="quixstreams.configuration.securityoptions.SecurityOptions"></a>

## SecurityOptions Objects

```python
class SecurityOptions(object)
```

A class representing security options for configuring SSL encryption with SASL authentication in Kafka.

<a id="quixstreams.configuration.securityoptions.SecurityOptions.__init__"></a>

#### \_\_init\_\_

```python
def __init__(ssl_certificates: str,
             username: str,
             password: str,
             sasl_mechanism: SaslMechanism = SaslMechanism.ScramSha256)
```

Initializes a new instance of SecurityOptions configured for SSL encryption with SASL authentication.

**Arguments**:

- `ssl_certificates` - The path to the folder or file containing the certificate authority
  certificate(s) used to validate the SSL connection.
- `Example` - "./certificates/ca.cert"
- `username` - The username for SASL authentication.
- `password` - The password for SASL authentication.
- `sasl_mechanism` - The SASL mechanism to use. Defaults to ScramSha256.

<a id="quixstreams.configuration.securityoptions.SecurityOptions.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer()
```

Retrieves the .NET pointer for the current SecurityOptions instance.

**Returns**:

- `ctypes.c_void_p` - The .NET pointer.

<a id="quixstreams.configuration.saslmechanism"></a>

# quixstreams.configuration.saslmechanism

<a id="quixstreams.configuration"></a>

# quixstreams.configuration

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

Initializes a new instance of TokenValidationConfiguration.

**Arguments**:

- `net_pointer` - Pointer to an instance of a .NET TokenValidationConfiguration.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled"></a>

#### enabled

```python
@property
def enabled() -> bool
```

Gets whether token validation and warnings are enabled. Defaults to true.

**Returns**:

- `bool` - True if token validation and warnings are enabled, False otherwise.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.enabled"></a>

#### enabled

```python
@enabled.setter
def enabled(value: bool)
```

Sets whether token validation and warnings are enabled. Defaults to true.

**Arguments**:

- `value` - True to enable token validation and warnings, False to disable.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry"></a>

#### warning\_before\_expiry

```python
@property
def warning_before_expiry() -> Union[timedelta, None]
```

Gets the period within which, if the token expires, a warning will be displayed. Defaults to 2 days. Set to None to disable the check.

**Returns**:

  Union[timedelta, None]: The period within which a warning will be displayed if the token expires or None if the check is disabled.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warning_before_expiry"></a>

#### warning\_before\_expiry

```python
@warning_before_expiry.setter
def warning_before_expiry(value: Union[timedelta, None])
```

Sets the period within which, if the token expires, a warning will be displayed. Defaults to 2 days. Set to None to disable the check.

**Arguments**:

- `value` - The new period within which a warning will be displayed if the token expires or None to disable the check.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token"></a>

#### warn\_about\_pat\_token

```python
@property
def warn_about_pat_token() -> bool
```

Gets whether to warn if the provided token is not a PAT token. Defaults to true.

**Returns**:

- `bool` - True if the warning is enabled, False otherwise.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.warn_about_pat_token"></a>

#### warn\_about\_pat\_token

```python
@warn_about_pat_token.setter
def warn_about_pat_token(value: bool)
```

Sets whether to warn if the provided token is not a PAT token. Defaults to true.

**Arguments**:

- `value` - True to enable the warning, False to disable.

<a id="quixstreams.quixstreamingclient.TokenValidationConfiguration.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .NET object pointer.

**Returns**:

- `ctypes.c_void_p` - The .NET pointer

<a id="quixstreams.quixstreamingclient.QuixStreamingClient"></a>

## QuixStreamingClient Objects

```python
class QuixStreamingClient(object)
```

Streaming client for Kafka configured automatically using Environment Variables and Quix platform endpoints.
Use this Client when you use this library together with Quix platform.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(token: str = None,
             auto_create_topics: bool = True,
             properties: Dict[str, str] = None,
             debug: bool = False)
```

Initializes a new instance of the QuixStreamingClient capable of creating topic consumers and producers.

**Arguments**:

- `token` - The token to use when talking to Quix. If not provided, the Quix__Sdk__Token environment variable will be used. Defaults to None.
- `auto_create_topics` - Whether topics should be auto-created if they don't exist yet. Defaults to True.
- `properties` - Additional broker properties. Defaults to None.
- `debug` - Whether debugging should be enabled. Defaults to False.

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

Opens a topic consumer capable of subscribing to receive incoming streams.

**Arguments**:

- `topic_id_or_name` - ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.
- `consumer_group` - The consumer group ID to use for consuming messages. If None, the consumer group is not used, and only consuming new messages. Defaults to None.
- `commit_settings` - The settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.
- `auto_offset_reset` - The offset to use when there is no saved offset for the consumer group. Defaults to AutoOffsetReset.Latest.
  

**Returns**:

- `TopicConsumer` - An instance of TopicConsumer for the specified topic.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_topic_producer"></a>

#### get\_topic\_producer

```python
def get_topic_producer(topic_id_or_name: str) -> TopicProducer
```

Gets a topic producer capable of producing outgoing streams.

**Arguments**:

- `topic_id_or_name` - ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.
  

**Returns**:

- `TopicProducer` - An instance of TopicProducer for the specified topic.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_raw_topic_consumer"></a>

#### get\_raw\_topic\_consumer

```python
def get_raw_topic_consumer(
    topic_id_or_name: str,
    consumer_group: str = None,
    auto_offset_reset: Union[AutoOffsetReset,
                             None] = None) -> RawTopicConsumer
```

Gets a topic consumer for consuming raw data from the stream.

**Arguments**:

- `topic_id_or_name` - ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.
- `consumer_group` - The consumer group ID to use for consuming messages. Defaults to None.
- `auto_offset_reset` - The offset to use when there is no saved offset for the consumer group. Defaults to None.
  

**Returns**:

- `RawTopicConsumer` - An instance of RawTopicConsumer for the specified topic.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_raw_topic_producer"></a>

#### get\_raw\_topic\_producer

```python
def get_raw_topic_producer(topic_id_or_name: str) -> RawTopicProducer
```

Gets a topic producer for producing raw data to the stream.

**Arguments**:

- `topic_id_or_name` - ID or name of the topic. If name is provided, the workspace will be derived from the environment variable or token, in that order.
  

**Returns**:

- `RawTopicProducer` - An instance of RawTopicProducer for the specified topic.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.token_validation_config"></a>

#### token\_validation\_config

```python
@property
def token_validation_config() -> TokenValidationConfiguration
```

Gets the configuration for token validation.

**Returns**:

- `TokenValidationConfiguration` - The current token validation configuration.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.token_validation_config"></a>

#### token\_validation\_config

```python
@token_validation_config.setter
def token_validation_config(value: TokenValidationConfiguration)
```

Sets the configuration for token validation.

**Arguments**:

- `value` - The new token validation configuration.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.api_url"></a>

#### api\_url

```python
@property
def api_url() -> str
```

Gets the base API URI. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.

**Returns**:

- `str` - The current base API URI.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.api_url"></a>

#### api\_url

```python
@api_url.setter
def api_url(value: str)
```

Sets the base API URI. Defaults to https://portal-api.platform.quix.ai, or environment variable Quix__Portal__Api if available.

**Arguments**:

- `value` - The new base API URI.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.cache_period"></a>

#### cache\_period

```python
@property
def cache_period() -> timedelta
```

Gets the period for which some API responses will be cached to avoid an excessive amount of calls. Defaults to 1 minute.

**Returns**:

- `timedelta` - The current cache period.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.cache_period"></a>

#### cache\_period

```python
@cache_period.setter
def cache_period(value: timedelta)
```

Sets the period for which some API responses will be cached to avoid an excessive amount of calls. Defaults to 1 minute.

**Arguments**:

- `value` - The new cache period.

<a id="quixstreams.quixstreamingclient.QuixStreamingClient.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .NET object pointer.

**Returns**:

- `ctypes.c_void_p` - The .NET pointer

<a id="quixstreams.topicproducer"></a>

# quixstreams.topicproducer

<a id="quixstreams.topicproducer.TopicProducer"></a>

## TopicProducer Objects

```python
@nativedecorator
class TopicProducer(object)
```

Interface to operate with the streaming platform for publishing messages

<a id="quixstreams.topicproducer.TopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TopicProducer.

NOTE: Do not initialize this class manually, use KafkaStreamingClient.get_topic_producer to create it.

**Arguments**:

- `net_pointer` - The .net object representing a StreamingClient.

<a id="quixstreams.topicproducer.TopicProducer.dispose"></a>

#### dispose

```python
def dispose()
```

Flushes pending data to the broker and disposes underlying resources

<a id="quixstreams.topicproducer.TopicProducer.flush"></a>

#### flush

```python
def flush()
```

Flushes pending data to the broker

<a id="quixstreams.topicproducer.TopicProducer.on_disposed"></a>

#### on\_disposed

```python
@property
def on_disposed() -> Callable[['TopicProducer'], None]
```

Gets the handler for when the topic is disposed.

**Returns**:

  Callable[[TopicProducer], None]: The event handler for topic disposal.
  The first parameter is the TopicProducer instance that got disposed.

<a id="quixstreams.topicproducer.TopicProducer.on_disposed"></a>

#### on\_disposed

```python
@on_disposed.setter
def on_disposed(value: Callable[['TopicProducer'], None]) -> None
```

Sets the handler for when the topic is disposed.

**Arguments**:

- `value` - The event handler for topic disposal.
  The first parameter is the TopicProducer instance that got disposed.

<a id="quixstreams.topicproducer.TopicProducer.create_stream"></a>

#### create\_stream

```python
def create_stream(stream_id: str = None) -> StreamProducer
```

Create a new stream and returns the related StreamProducer to operate it.

**Arguments**:

- `stream_id` - Provide if you wish to overwrite the generated stream id. Useful if you wish
  to always stream a certain source into the same stream.
  

**Returns**:

- `StreamProducer` - The created StreamProducer instance.

<a id="quixstreams.topicproducer.TopicProducer.get_stream"></a>

#### get\_stream

```python
def get_stream(stream_id: str) -> StreamProducer
```

Retrieves a stream that was previously created by this instance, if the stream is not closed.

**Arguments**:

- `stream_id` - The id of the stream.
  

**Returns**:

- `StreamProducer` - The retrieved StreamProducer instance or None if not found.

<a id="quixstreams.topicproducer.TopicProducer.get_or_create_stream"></a>

#### get\_or\_create\_stream

```python
def get_or_create_stream(
    stream_id: str,
    on_stream_created: Callable[[StreamProducer],
                                None] = None) -> StreamProducer
```

Retrieves a stream that was previously created by this instance if the stream is not closed, otherwise creates a new stream.

**Arguments**:

- `stream_id` - The id of the stream you want to get or create.
- `on_stream_created` - A callback function that takes a StreamProducer as a parameter.
  

**Returns**:

- `StreamProducer` - The retrieved or created StreamProducer instance.

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

<a id="quixstreams.models.eventlevel"></a>

# quixstreams.models.eventlevel

<a id="quixstreams.models.timeseriesdata"></a>

# quixstreams.models.timeseriesdata

<a id="quixstreams.models.timeseriesdata.TimeseriesData"></a>

## TimeseriesData Objects

```python
@nativedecorator
class TimeseriesData(object)
```

Describes timeseries data for multiple timestamps.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesData.

**Arguments**:

- `net_pointer` - Pointer to an instance of a .net TimeseriesData.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.clone"></a>

#### clone

```python
def clone(parameter_filter: Optional[List[str]] = None)
```

Initializes a new instance of timeseries data with parameters matching the filter if one is provided.

**Arguments**:

- `parameter_filter` - The parameter filter. If one is provided, only parameters
  present in the list will be cloned.
  

**Returns**:

- `TimeseriesData` - A new instance of TimeseriesData with filtered parameters.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp"></a>

#### add\_timestamp

```python
def add_timestamp(time: Union[datetime, timedelta]) -> TimeseriesDataTimestamp
```

Start adding a new set of parameters and their tags at the specified time.

**Arguments**:

- `time` - The time to use for adding new event values.
  | datetime: The datetime to use for adding new event values. Epoch will never be added to this
  | timedelta: The time since the default epoch to add the event values at
  

**Returns**:

- `TimeseriesDataTimestamp` - A new TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp_milliseconds"></a>

#### add\_timestamp\_milliseconds

```python
def add_timestamp_milliseconds(milliseconds: int) -> TimeseriesDataTimestamp
```

Start adding a new set of parameters and their tags at the specified time.

**Arguments**:

- `milliseconds` - The time in milliseconds since the default epoch to add the event values at.
  

**Returns**:

- `TimeseriesDataTimestamp` - A new TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.add_timestamp_nanoseconds"></a>

#### add\_timestamp\_nanoseconds

```python
def add_timestamp_nanoseconds(nanoseconds: int) -> TimeseriesDataTimestamp
```

Start adding a new set of parameters and their tags at the specified time.

**Arguments**:

- `nanoseconds` - The time in nanoseconds since the default epoch to add the event values at.
  

**Returns**:

- `TimeseriesDataTimestamp` - A new TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.timestamps"></a>

#### timestamps

```python
@property
def timestamps() -> List[TimeseriesDataTimestamp]
```

Gets the data as rows of TimeseriesDataTimestamp.

**Returns**:

- `List[TimeseriesDataTimestamp]` - A list of TimeseriesDataTimestamp instances.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.timestamps"></a>

#### timestamps

```python
@timestamps.setter
def timestamps(timestamp_list: List[TimeseriesDataTimestamp]) -> None
```

Sets the data as rows of TimeseriesDataTimestamp.

**Arguments**:

- `timestamp_list` - A list of TimeseriesDataTimestamp instances to set.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.to_dataframe"></a>

#### to\_dataframe

```python
def to_dataframe() -> pd.DataFrame
```

Converts TimeseriesData to pandas DataFrame.

**Returns**:

- `pd.DataFrame` - Converted pandas DataFrame.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.from_panda_dataframe"></a>

#### from\_panda\_dataframe

```python
@staticmethod
def from_panda_dataframe(data_frame: pd.DataFrame,
                         epoch: int = 0) -> 'TimeseriesData'
```

Converts pandas DataFrame to TimeseriesData.

**Arguments**:

- `data_frame` - The pandas DataFrame to convert to TimeseriesData.
- `epoch` - The epoch to add to each time value when converting to TimeseriesData. Defaults to 0.
  

**Returns**:

- `TimeseriesData` - Converted TimeseriesData instance.

<a id="quixstreams.models.timeseriesdata.TimeseriesData.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the .net pointer of the current instance.

**Returns**:

- `ctypes.c_void_p` - The .net pointer of the current instance

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

<a id="quixstreams.models.streamproducer.streameventsproducer"></a>

# quixstreams.models.streamproducer.streameventsproducer

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer"></a>

## StreamEventsProducer Objects

```python
@nativedecorator
class StreamEventsProducer(object)
```

Helper class for producing EventDefinitions and EventData.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamEventsProducer.

**Arguments**:

- `net_pointer` - Pointer to an instance of a .NET StreamEventsProducer.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately publishes the event definitions from the buffer without waiting for buffer condition to fulfill
(200ms timeout). TODO: Verify 200ms timeout value.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_tags"></a>

#### default\_tags

```python
@property
def default_tags() -> Dict[str, str]
```

Gets default tags injected to all event values sent by the producer.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.default_location"></a>

#### default\_location

```python
@property
def default_location() -> str
```

Gets the default Location of the events. Event definitions added with add_definition will be inserted at this location.
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

**Arguments**:

- `value` - Location string, e.g., "/Group1/SubGroup2".

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.epoch"></a>

#### epoch

```python
@property
def epoch() -> datetime
```

The unix epoch from, which all other timestamps in this model are measured from in nanoseconds.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.epoch"></a>

#### epoch

```python
@epoch.setter
def epoch(value: datetime)
```

Sets the default epoch used for event values.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.publish"></a>

#### publish

```python
def publish(data: Union[EventData, pd.DataFrame], **columns) -> None
```

Publish an event into the stream.

**Arguments**:

- `data` - EventData object or a pandas dataframe.
- `columns` - Column names if the dataframe has different columns from 'id', 'timestamp', and 'value'.
  For instance, if 'id' is in the column 'event_id', id='event_id' must be passed as an argument.
  

**Raises**:

- `TypeError` - If the data argument is neither an EventData nor pandas dataframe.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp"></a>

#### add\_timestamp

```python
def add_timestamp(time: Union[datetime, timedelta]) -> EventDataBuilder
```

Start adding a new set of event values at the given timestamp.

**Arguments**:

- `time` - The time to use for adding new event values.
  * datetime: The datetime to use for adding new event values. NOTE, epoch is not used.
  * timedelta: The time since the default epoch to add the event values at
  

**Returns**:

- `EventDataBuilder` - Event data builder to add event values at the provided time.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp_milliseconds"></a>

#### add\_timestamp\_milliseconds

```python
def add_timestamp_milliseconds(milliseconds: int) -> EventDataBuilder
```

Start adding a new set of event values at the given timestamp.

**Arguments**:

- `milliseconds` - The time in milliseconds since the default epoch to add the event values at.
  

**Returns**:

- `EventDataBuilder` - Event data builder to add event values at the provided time.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_timestamp_nanoseconds"></a>

#### add\_timestamp\_nanoseconds

```python
def add_timestamp_nanoseconds(nanoseconds: int) -> EventDataBuilder
```

Start adding a new set of event values at the given timestamp.

**Arguments**:

- `nanoseconds` - The time in nanoseconds since the default epoch to add the event values at.
  

**Returns**:

- `EventDataBuilder` - Event data builder to add event values at the provided time.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_definition"></a>

#### add\_definition

```python
def add_definition(event_id: str,
                   name: str = None,
                   description: str = None) -> EventDefinitionBuilder
```

Add new Event definition to define properties like Name or Level, among others.

**Arguments**:

- `event_id` - The id of the event. Must match the event id used to send data.
- `name` - The human-friendly display name of the event.
- `description` - The description of the event.
  

**Returns**:

- `EventDefinitionBuilder` - EventDefinitionBuilder to define properties of the event or add additional events.

<a id="quixstreams.models.streamproducer.streameventsproducer.StreamEventsProducer.add_location"></a>

#### add\_location

```python
def add_location(location: str) -> EventDefinitionBuilder
```

Add a new location in the events groups hierarchy.

**Arguments**:

- `location` - The group location.
  

**Returns**:

- `EventDefinitionBuilder` - EventDefinitionBuilder to define the events under the specified location.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer"></a>

# quixstreams.models.streamproducer.streamtimeseriesproducer

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer"></a>

## StreamTimeseriesProducer Objects

```python
@nativedecorator
class StreamTimeseriesProducer(object)
```

Helper class for producing ParameterDefinition and TimeseriesData.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_producer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamTimeseriesProducer.

**Arguments**:

- `stream_producer` - The Stream producer which owns this stream timeseries producer.
- `net_pointer` - Pointer to an instance of a .net StreamTimeseriesProducer.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately publish timeseries data and definitions from the buffer without waiting for buffer condition to fulfill for either.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.add_definition"></a>

#### add\_definition

```python
def add_definition(parameter_id: str,
                   name: str = None,
                   description: str = None) -> ParameterDefinitionBuilder
```

Add new parameter definition to the StreamTimeseriesProducer. Configure it with the builder methods.

**Arguments**:

- `parameter_id` - The id of the parameter. Must match the parameter id used to send data.
- `name` - The human friendly display name of the parameter.
- `description` - The description of the parameter.
  

**Returns**:

- `ParameterDefinitionBuilder` - Builder to define the parameter properties.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.add_location"></a>

#### add\_location

```python
def add_location(location: str) -> ParameterDefinitionBuilder
```

Add a new location in the parameters groups hierarchy.

**Arguments**:

- `location` - The group location.
  

**Returns**:

- `ParameterDefinitionBuilder` - Builder to define the parameters under the specified location.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.default_location"></a>

#### default\_location

```python
@property
def default_location() -> str
```

Gets the default location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
See add_location for adding definitions at a different location without changing default.

**Returns**:

- `str` - The default location of the parameters, e.g., "/Group1/SubGroup2".

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.default_location"></a>

#### default\_location

```python
@default_location.setter
def default_location(value: str)
```

Sets the default location of the parameters. Parameter definitions added with add_definition will be inserted at this location.
See add_location for adding definitions at a different location without changing default.

**Arguments**:

- `value` - The new default location of the parameters, e.g., "/Group1/SubGroup2".

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.buffer"></a>

#### buffer

```python
@property
def buffer() -> TimeseriesBufferProducer
```

Get the buffer for producing timeseries data.

**Returns**:

- `TimeseriesBufferProducer` - The buffer for producing timeseries data.

<a id="quixstreams.models.streamproducer.streamtimeseriesproducer.StreamTimeseriesProducer.publish"></a>

#### publish

```python
def publish(
    packet: Union[TimeseriesData, pd.DataFrame, TimeseriesDataRaw,
                  TimeseriesDataTimestamp]
) -> None
```

Publish the given packet to the stream without any buffering.

**Arguments**:

- `packet` - The packet containing TimeseriesData, TimeseriesDataRaw, TimeseriesDataTimestamp, or pandas DataFrame.
  

**Notes**:

  - Pandas DataFrame should contain 'time' label, else the first integer label will be taken as time.
  - Tags should be prefixed by TAG__ or they will be treated as parameters.
  

**Examples**:

  Send a pandas DataFrame:
  pdf = pandas.DataFrame({'time': [1, 5],
- `'panda_param'` - [123.2, 5]})
  instance.publish(pdf)
  
  Send a pandas DataFrame with multiple values:
  pdf = pandas.DataFrame({'time': [1, 5, 10],
- `'panda_param'` - [123.2, None, 12],
- `'panda_param2'` - ["val1", "val2", None]})
  instance.publish(pdf)
  
  Send a pandas DataFrame with tags:
  pdf = pandas.DataFrame({'time': [1, 5, 10],
- `'panda_param'` - [123.2, 5, 12],
- `'TAG__Tag1'` - ["v1", 2, None],
- `'TAG__Tag2'` - [1, None, 3]})
  instance.publish(pdf)
  

**Raises**:

- `Exception` - If the given type is not supported for publishing.

<a id="quixstreams.models.streamproducer.streampropertiesproducer"></a>

# quixstreams.models.streamproducer.streampropertiesproducer

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer"></a>

## StreamPropertiesProducer Objects

```python
@nativedecorator
class StreamPropertiesProducer(object)
```

Represents properties and metadata of the stream.
All changes to these properties are automatically published to the underlying stream.

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

Gets the human friendly name of the stream.

**Returns**:

- `str` - The human friendly name of the stream.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.name"></a>

#### name

```python
@name.setter
def name(value: str)
```

Sets the human friendly name of the stream.

**Arguments**:

- `value` - The new human friendly name of the stream.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.location"></a>

#### location

```python
@property
def location() -> str
```

Gets the location of the stream in the data catalogue.

**Returns**:

- `str` - The location of the stream in the data catalogue, e.g., "/cars/ai/carA/".

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.location"></a>

#### location

```python
@location.setter
def location(value: str)
```

Sets the location of the stream in the data catalogue.

**Arguments**:

- `value` - The new location of the stream in the data catalogue.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.metadata"></a>

#### metadata

```python
@property
def metadata() -> Dict[str, str]
```

"
Gets the metadata of the stream.

**Returns**:

  Dict[str, str]: The metadata of the stream.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.parents"></a>

#### parents

```python
@property
def parents() -> List[str]
```

Gets the list of stream ids of the parent streams.

**Returns**:

- `List[str]` - The list of stream ids of the parent streams.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.time_of_recording"></a>

#### time\_of\_recording

```python
@property
def time_of_recording() -> datetime
```

Gets the datetime of the stream recording.

**Returns**:

- `datetime` - The datetime of the stream recording.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.time_of_recording"></a>

#### time\_of\_recording

```python
@time_of_recording.setter
def time_of_recording(value: datetime)
```

Sets the time of the stream recording.

**Arguments**:

- `value` - The new time of the stream recording.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush_interval"></a>

#### flush\_interval

```python
@property
def flush_interval() -> int
```

Gets the automatic flush interval of the properties metadata into the channel (in milliseconds).

**Returns**:

- `int` - The automatic flush interval in milliseconds, default is 30000.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush_interval"></a>

#### flush\_interval

```python
@flush_interval.setter
def flush_interval(value: int)
```

Sets the automatic flush interval of the properties metadata into the channel (in milliseconds).

**Arguments**:

- `value` - The new flush interval in milliseconds.

<a id="quixstreams.models.streamproducer.streampropertiesproducer.StreamPropertiesProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately publishes the properties yet to be sent instead of waiting for the flush timer (20ms).

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer"></a>

# quixstreams.models.streamproducer.timeseriesbufferproducer

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer"></a>

## TimeseriesBufferProducer Objects

```python
@nativedecorator
class TimeseriesBufferProducer(TimeseriesBuffer)
```

A class for producing timeseries data to a StreamProducer in a buffered manner.

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_producer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TimeseriesBufferProducer.
NOTE: Do not initialize this class manually, use StreamTimeseriesProducer.buffer to access an instance of it

**Arguments**:

- `stream_producer` - The Stream producer which owns this timeseries buffer producer
- `net_pointer` - Pointer to an instance of a .net TimeseriesBufferProducer
  

**Raises**:

- `Exception` - If TimeseriesBufferProducer is None

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.default_tags"></a>

#### default\_tags

```python
@property
def default_tags() -> Dict[str, str]
```

Get default tags injected for all parameters values sent by this buffer.

**Returns**:

  Dict[str, str]: A dictionary containing the default tags

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.epoch"></a>

#### epoch

```python
@property
def epoch() -> datetime
```

Get the default epoch used for parameter values.

**Returns**:

- `datetime` - The default epoch used for parameter values

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.epoch"></a>

#### epoch

```python
@epoch.setter
def epoch(value: datetime)
```

Set the default epoch used for parameter values. Datetime added on top of all the Timestamps.

**Arguments**:

- `value` - The default epoch to set for parameter values

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.add_timestamp"></a>

#### add\_timestamp

```python
def add_timestamp(time: Union[datetime, timedelta]) -> TimeseriesDataBuilder
```

Start adding a new set of parameter values at the given timestamp.

**Arguments**:

- `time` - The time to use for adding new parameter values.
  - datetime: The datetime to use for adding new parameter values. NOTE, epoch is not used
  - timedelta: The time since the default epoch to add the parameter values at
  

**Returns**:

- `TimeseriesDataBuilder` - A TimeseriesDataBuilder instance for adding parameter values
  

**Raises**:

- `ValueError` - If 'time' is None or not an instance of datetime or timedelta

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.add_timestamp_nanoseconds"></a>

#### add\_timestamp\_nanoseconds

```python
def add_timestamp_nanoseconds(nanoseconds: int) -> TimeseriesDataBuilder
```

Start adding a new set of parameter values at the given timestamp.

**Arguments**:

- `nanoseconds` - The time in nanoseconds since the default epoch to add the parameter values at
  

**Returns**:

- `TimeseriesDataBuilder` - A TimeseriesDataBuilder instance for adding parameter values

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.flush"></a>

#### flush

```python
def flush()
```

Immediately publishes the data from the buffer without waiting for the buffer condition to be fulfilled.

<a id="quixstreams.models.streamproducer.timeseriesbufferproducer.TimeseriesBufferProducer.publish"></a>

#### publish

```python
def publish(
    packet: Union[TimeseriesData, pd.DataFrame, TimeseriesDataRaw,
                  TimeseriesDataTimestamp]
) -> None
```

Publish the provided timeseries packet to the buffer.

**Arguments**:

- `packet` - The packet containing TimeseriesData, TimeseriesDataRaw, TimeseriesDataTimestamp, or pandas DataFrame.
  - packet type panda.DataFrame:
  * Note 1: panda data frame should contain 'time' label, else the first integer label will be taken as time.
  * Note 2: Tags should be prefixed by TAG__ or they will be treated as timeseries parameters
  

**Examples**:

  Send a panda data frame:
  pdf = panda.DataFrame({'time': [1, 5],
- `'panda_param'` - [123.2, 5]})
  
  instance.publish(pdf)
  
  Send a panda data frame with multiple values:
  pdf = panda.DataFrame({'time': [1, 5, 10],
- `'panda_param'` - [123.2, None, 12],
- `'panda_param2'` - ["val1", "val2", None]})
  
  instance.publish(pdf)
  
  Send a panda data frame with tags:
  pdf = panda.DataFrame({'time': [1, 5, 10],
- `'panda_param'` - [123.2, 5, 12],,
- `'TAG__Tag1'` - ["v1", 2, None],
- `'TAG__Tag2'` - [1, None, 3]})
  
  instance.publish(pdf)
  

**Raises**:

- `Exception` - If the packet type is not supported

<a id="quixstreams.models.streamproducer"></a>

# quixstreams.models.streamproducer

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

<a id="quixstreams.models.parametervalue"></a>

# quixstreams.models.parametervalue

<a id="quixstreams.models.parametervalue.ParameterValue"></a>

## ParameterValue Objects

```python
@nativedecorator
class ParameterValue(object)
```

Represents a single parameter value of either numeric, string, or binary type.

<a id="quixstreams.models.parametervalue.ParameterValue.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of ParameterValue.

**Arguments**:

- `net_pointer` - The .net object pointer representing ParameterValue.

<a id="quixstreams.models.parametervalue.ParameterValue.numeric_value"></a>

#### numeric\_value

```python
@property
def numeric_value() -> float
```

Gets the numeric value of the parameter if the underlying parameter is of numeric type.

<a id="quixstreams.models.parametervalue.ParameterValue.numeric_value"></a>

#### numeric\_value

```python
@numeric_value.setter
def numeric_value(value: float)
```

Sets the numeric value of the parameter and updates the type to numeric.

**Arguments**:

- `value` - The numeric value to set.

<a id="quixstreams.models.parametervalue.ParameterValue.string_value"></a>

#### string\_value

```python
@property
def string_value() -> str
```

Gets the string value of the parameter if the underlying parameter is of string type.

<a id="quixstreams.models.parametervalue.ParameterValue.string_value"></a>

#### string\_value

```python
@string_value.setter
def string_value(value: str)
```

Sets the string value of the parameter and updates the type to string.

**Arguments**:

- `value` - The string value to set.

<a id="quixstreams.models.parametervalue.ParameterValue.binary_value"></a>

#### binary\_value

```python
@property
def binary_value() -> bytes
```

Gets the binary value of the parameter if the underlying parameter is of binary type.

<a id="quixstreams.models.parametervalue.ParameterValue.binary_value"></a>

#### binary\_value

```python
@binary_value.setter
def binary_value(value: Union[bytearray, bytes])
```

Sets the binary value of the parameter and updates the type to binary.

**Arguments**:

- `value` - The binary value to set.

<a id="quixstreams.models.parametervalue.ParameterValue.type"></a>

#### type

```python
@property
def type() -> ParameterValueType
```

Gets the type of value, which is numeric, string, binary if set, otherwise empty

<a id="quixstreams.models.parametervalue.ParameterValue.value"></a>

#### value

```python
@property
def value()
```

Gets the underlying value.

<a id="quixstreams.models.parametervalue.ParameterValue.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .net object pointer.

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

- `net_pointer` - Pointer to an instance of a .net TimeseriesDataRaw. Defaults to None.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.to_dataframe"></a>

#### to\_dataframe

```python
def to_dataframe() -> pd.DataFrame
```

Converts TimeseriesDataRaw to pandas DataFrame.

**Returns**:

- `pd.DataFrame` - Converted pandas DataFrame.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.from_dataframe"></a>

#### from\_dataframe

```python
@staticmethod
def from_dataframe(data_frame: pd.DataFrame,
                   epoch: int = 0) -> 'TimeseriesDataRaw'
```

Converts from pandas DataFrame to TimeseriesDataRaw.

**Arguments**:

- `data_frame` - The pandas DataFrame to convert to TimeseriesData.
- `epoch` - The epoch to add to each time value when converting to TimeseriesData. Defaults to 0.
  

**Returns**:

- `TimeseriesDataRaw` - Converted TimeseriesDataRaw.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.set_values"></a>

#### set\_values

```python
def set_values(epoch: int, timestamps: List[int],
               numeric_values: Dict[str, List[float]],
               string_values: Dict[str, List[str]],
               binary_values: Dict[str,
                                   List[bytes]], tag_values: Dict[str,
                                                                  List[str]])
```

Sets the values of the timeseries data from the provided dictionaries.
Dictionary values are matched by index to the provided timestamps.

**Arguments**:

- `epoch` - The time from which all timestamps are measured from.
- `timestamps` - The timestamps of values in nanoseconds since epoch as an array.
- `numeric_values` - The numeric values where the dictionary key is the parameter name and the value is the array of values.
- `string_values` - The string values where the dictionary key is the parameter name and the value is the array of values.
- `binary_values` - The binary values where the dictionary key is the parameter name and the value is the array of values.
- `tag_values` - The tag values where the dictionary key is the parameter name and the value is the array of values.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.epoch"></a>

#### epoch

```python
@property
def epoch() -> int
```

The Unix epoch from which all other timestamps in this model are measured, in nanoseconds.

**Returns**:

- `int` - The Unix epoch (01/01/1970) in nanoseconds.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.timestamps"></a>

#### timestamps

```python
@property
def timestamps() -> List[int]
```

The timestamps of values in nanoseconds since the epoch.
Timestamps are matched by index to numeric_values, string_values, binary_values, and tag_values.

**Returns**:

- `List[int]` - A list of timestamps in nanoseconds since the epoch.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.numeric_values"></a>

#### numeric\_values

```python
@property
def numeric_values() -> Dict[str, List[Optional[float]]]
```

The numeric values for parameters.
The key is the parameter ID the values belong to. The value is the numerical values of the parameter.
Values are matched by index to timestamps.

**Returns**:

  Dict[str, List[Optional[float]]]: A dictionary mapping parameter IDs to lists of numerical values.

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.string_values"></a>

#### string\_values

```python
@property
def string_values() -> Dict[str, List[str]]
```

The string values for parameters.
The key is the parameter ID the values belong to. The value is the string values of the parameter.
Values are matched by index to timestamps.

**Returns**:

  Dict[str, List[str]]: A dictionary mapping parameter IDs to lists of string values

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.binary_values"></a>

#### binary\_values

```python
@property
def binary_values() -> Dict[str, List[bytes]]
```

The binary values for parameters.
The key is the parameter ID the values belong to.
The value is the binary values of the parameter. Values are matched by index to timestamps.

**Returns**:

  Dict[str, List[bytes]]: A dictionary mapping parameter IDs to lists of bytes values

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.tag_values"></a>

#### tag\_values

```python
@property
def tag_values() -> Dict[str, List[str]]
```

The tag values for parameters.
The key is the parameter ID the values belong to. The value is the tag values of the parameter.
Values are matched by index to timestamps.

**Returns**:

  Dict[str, List[str]]: A dictionary mapping parameter IDs to lists of string values

<a id="quixstreams.models.timeseriesdataraw.TimeseriesDataRaw.convert_to_timeseriesdata"></a>

#### convert\_to\_timeseriesdata

```python
def convert_to_timeseriesdata() -> TimeseriesData
```

Converts TimeseriesDataRaw to TimeseriesData

<a id="quixstreams.models.timeseriesbuffer"></a>

# quixstreams.models.timeseriesbuffer

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer"></a>

## TimeseriesBuffer Objects

```python
@nativedecorator
class TimeseriesBuffer(object)
```

Represents a class used to consume and produce stream messages in a buffered manner.
When buffer conditions are not configured, it acts a pass-through, raising each message as arrives.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TimeseriesBuffer.

NOTE: Do not initialize this class manually, use StreamProducer.timeseries.buffer to create it.

**Arguments**:

- `stream` - The stream the buffer is created for.
- `net_pointer` - Pointer to a .net TimeseriesBuffer object.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_data_released"></a>

#### on\_data\_released

```python
@property
def on_data_released() -> Callable[
    [Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]
```

Gets the handler for when the stream receives data.

**Returns**:

  Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]: The event handler.
  The first parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_data_released"></a>

#### on\_data\_released

```python
@on_data_released.setter
def on_data_released(
    value: Callable[
        [Union['StreamConsumer', 'StreamProducer'], TimeseriesData], None]
) -> None
```

Sets the handler for when the stream receives data.

**Arguments**:

- `value` - The event handler. The first parameter is the stream the data is received for, second is the data in TimeseriesData format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_raw_released"></a>

#### on\_raw\_released

```python
@property
def on_raw_released() -> Callable[
    [Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]
```

Gets the handler for when the stream receives raw data.

**Returns**:

  Callable[[Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]: The event handler.
  The first parameter is the stream the data is received for, second is the data in TimeseriesDataRaw format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_raw_released"></a>

#### on\_raw\_released

```python
@on_raw_released.setter
def on_raw_released(
    value: Callable[
        [Union['StreamConsumer', 'StreamProducer'], TimeseriesDataRaw], None]
) -> None
```

Sets the handler for when the stream receives raw data.

**Arguments**:

- `value` - The event handler. The first parameter is the stream the data is received for, second is the data in TimeseriesDataRaw format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_dataframe_released"></a>

#### on\_dataframe\_released

```python
@property
def on_dataframe_released() -> Callable[
    [Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]
```

Gets the handler for when the stream receives data as a pandas DataFrame.

**Returns**:

  Callable[[Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]: The event handler.
  The first parameter is the stream the data is received for, second is the data in pandas.DataFrame format.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.on_dataframe_released"></a>

#### on\_dataframe\_released

```python
@on_dataframe_released.setter
def on_dataframe_released(
    value: Callable[
        [Union['StreamConsumer', 'StreamProducer'], pandas.DataFrame], None]
) -> None
```

Sets the handler for when the stream receives data as a pandas DataFrame.

**Arguments**:

- `value` - The event handler. The first parameter is the stream the data is received for, second is the data in pandas.DataFrame format.

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

Sets the custom function to filter incoming data before adding to the buffer.

The custom function takes a TimeseriesDataTimestamp object as input and returns
a boolean value. If the function returns True, the data is added to the buffer,
otherwise not. By default, this feature is disabled (None).

**Arguments**:

- `value` - Custom filter function.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.custom_trigger"></a>

#### custom\_trigger

```python
@property
def custom_trigger() -> Callable[[TimeseriesData], bool]
```

Gets the custom trigger function, which is invoked after adding a new timestamp to the buffer.

If the custom trigger function returns True, the buffer releases content and triggers relevant callbacks.
By default, this feature is disabled (None).

**Returns**:

  Callable[[TimeseriesData], bool]: Custom trigger function.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.custom_trigger"></a>

#### custom\_trigger

```python
@custom_trigger.setter
def custom_trigger(value: Callable[[TimeseriesData], bool])
```

Sets the custom trigger function, which is invoked after adding a new timestamp to the buffer.

If the custom trigger function returns True, the buffer releases content and triggers relevant callbacks.
By default, this feature is disabled (None).

**Arguments**:

- `value` - Custom trigger function.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.packet_size"></a>

#### packet\_size

```python
@property
def packet_size() -> Optional[int]
```

Gets the maximum packet size in terms of values for the buffer.

Each time the buffer has this amount of data, a callback method, such as `on_data_released`,
is invoked, and the data is cleared from the buffer. By default, this feature
is disabled (None).

**Returns**:

- `Optional[int]` - Maximum packet size for the buffer.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.packet_size"></a>

#### packet\_size

```python
@packet_size.setter
def packet_size(value: Optional[int])
```

Sets the maximum packet size in terms of values for the buffer.

Each time the buffer has this amount of data, a callback method, such as `on_data_released`,
is invoked, and the data is cleared from the buffer. By default, this feature
is disabled (None).

**Arguments**:

- `value` - Maximum packet size for the buffer.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@property
def time_span_in_nanoseconds() -> Optional[int]
```

Gets the maximum time between timestamps for the buffer in nanoseconds.

When the difference between the earliest and latest buffered timestamp surpasses
this number, a callback method, such as `on_data_released`, is invoked, and the data is cleared
from the buffer. By default, this feature is disabled (None).

**Returns**:

- `Optional[int]` - Maximum time between timestamps in nanoseconds.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@time_span_in_nanoseconds.setter
def time_span_in_nanoseconds(value: Optional[int])
```

Sets the maximum time between timestamps for the buffer in nanoseconds.

When the difference between the earliest and latest buffered timestamp surpasses
this number, a callback method, such as `on_data_released`, is invoked, and the data is cleared
from the buffer. By default, this feature is disabled (None).

**Arguments**:

- `value` - Maximum time between timestamps in nanoseconds.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@property
def time_span_in_milliseconds() -> Optional[int]
```

Gets the maximum time between timestamps for the buffer in milliseconds.

This property retrieves the maximum time between the earliest and latest buffered
timestamp in milliseconds. If the difference surpasses this number, a callback method,
such as on_data_released, is invoked, and the data is cleared from the buffer. Note that
this property is a millisecond converter on top of time_span_in_nanoseconds, and both
work with the same underlying value. Defaults to None (disabled).

**Returns**:

- `Optional[int]` - The maximum time difference between timestamps in milliseconds, or None if disabled.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@time_span_in_milliseconds.setter
def time_span_in_milliseconds(value: Optional[int])
```

Sets the maximum time between timestamps for the buffer in milliseconds.

This property sets the maximum time between the earliest and latest buffered
timestamp in milliseconds. If the difference surpasses this number, a callback method,
such as on_data_released, is invoked, and the data is cleared from the buffer. Note that
this property is a millisecond converter on top of time_span_in_nanoseconds, and both
work with the same underlying value. Defaults to None (disabled).

**Arguments**:

- `value` - The maximum time difference between timestamps in milliseconds, or None to disable.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.buffer_timeout"></a>

#### buffer\_timeout

```python
@property
def buffer_timeout() -> Optional[int]
```

Gets the maximum duration in milliseconds for which the buffer will be held. When the configured value has elapsed
or other buffer conditions are met, a callback method, such as on_data_released, is invoked.
Defaults to None (disabled).

**Returns**:

- `Optional[int]` - The maximum duration in milliseconds before invoking a callback method, or None if disabled.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.buffer_timeout"></a>

#### buffer\_timeout

```python
@buffer_timeout.setter
def buffer_timeout(value: Optional[int])
```

Sets the maximum duration in milliseconds for which the buffer will be held. When the configured value has elapsed
or other buffer conditions are met, a callback method, such as on_data_released, is invoked.
Defaults to None (disabled).

**Arguments**:

- `value` - The maximum duration in milliseconds before invoking a callback method, or None to disable.

<a id="quixstreams.models.timeseriesbuffer.TimeseriesBuffer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .net object pointer.

**Returns**:

- `ctypes.c_void_p` - The .net object pointer.

<a id="quixstreams.models.streampackage"></a>

# quixstreams.models.streampackage

<a id="quixstreams.models.streampackage.StreamPackage"></a>

## StreamPackage Objects

```python
@nativedecorator
class StreamPackage(object)
```

Default model implementation for non-typed message packages of the Telemetry layer. It holds a value and its type.

<a id="quixstreams.models.streampackage.StreamPackage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamPackage.

**Notes**:

  Do not initialize this class manually. Will be initialized by StreamConsumer.on_package_received.
  

**Arguments**:

- `net_pointer` - Pointer to an instance of a .net StreamPackage.

<a id="quixstreams.models.streampackage.StreamPackage.transport_context"></a>

#### transport\_context

```python
@property
def transport_context() -> Dict[str, str]
```

Context holder for package when transporting through the pipeline.

<a id="quixstreams.models.streampackage.StreamPackage.to_json"></a>

#### to\_json

```python
def to_json() -> str
```

Serialize the package into JSON.

**Returns**:

- `str` - The serialized JSON string of the package.

<a id="quixstreams.models.streampackage.StreamPackage.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .net object pointer.

**Returns**:

- `ctypes.c_void_p` - The .net object pointer.

<a id="quixstreams.models.codecsettings"></a>

# quixstreams.models.codecsettings

<a id="quixstreams.models.codecsettings.CodecSettings"></a>

## CodecSettings Objects

```python
class CodecSettings(object)
```

Global Codec settings for streams.

<a id="quixstreams.models.codecsettings.CodecSettings.set_global_codec_type"></a>

#### set\_global\_codec\_type

```python
@staticmethod
def set_global_codec_type(codec_type: CodecType)
```

Sets the codec type to be used by producers and transfer package value serialization.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer"></a>

# quixstreams.models.streamconsumer.streamtimeseriesconsumer

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer"></a>

## StreamTimeseriesConsumer Objects

```python
@nativedecorator
class StreamTimeseriesConsumer(object)
```

Consumer for streams, which raises TimeseriesData and ParameterDefinitions related messages

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamTimeseriesConsumer.
NOTE: Do not initialize this class manually. Use StreamConsumer.timeseries to access an instance of it.

**Arguments**:

- `stream_consumer` - The Stream consumer which owns this stream event consumer.
- `net_pointer` _.net object_ - Pointer to an instance of a .net StreamTimeseriesConsumer.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_data_received"></a>

#### on\_data\_received

```python
@property
def on_data_received() -> Callable[['StreamConsumer', TimeseriesData], None]
```

Gets the handler for when data is received (without buffering).

**Returns**:

  Callable[['StreamConsumer', TimeseriesData], None]: The function that handles the data received.
  The first parameter is the stream that receives the data, and the second is the data in TimeseriesData format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_data_received"></a>

#### on\_data\_received

```python
@on_data_received.setter
def on_data_received(
        value: Callable[['StreamConsumer', TimeseriesData], None]) -> None
```

Sets the handler for when data is received (without buffering).

**Arguments**:

- `value` - The function that handles the data received.
  The first parameter is the stream that receives the data, and the second is the data in TimeseriesData format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_raw_received"></a>

#### on\_raw\_received

```python
@property
def on_raw_received() -> Callable[['StreamConsumer', TimeseriesDataRaw], None]
```

Gets the handler for when data is received (without buffering) in raw transport format.

**Returns**:

  Callable[['StreamConsumer', TimeseriesDataRaw], None]: The function that handles the data received.
  The first parameter is the stream that receives the data, and the second is the data in TimeseriesDataRaw format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_raw_received"></a>

#### on\_raw\_received

```python
@on_raw_received.setter
def on_raw_received(
        value: Callable[['StreamConsumer', TimeseriesDataRaw], None]) -> None
```

Sets the handler for when data is received (without buffering) in raw transport format.

**Arguments**:

- `value` - The function that handles the data received.
  The first parameter is the stream that receives the data, and the second is the data in TimeseriesDataRaw format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_dataframe_received"></a>

#### on\_dataframe\_received

```python
@property
def on_dataframe_received(
) -> Callable[['StreamConsumer', pandas.DataFrame], None]
```

Gets the handler for when data is received (without buffering) in pandas DataFrame format.

**Returns**:

  Callable[['StreamConsumer', pandas.DataFrame], None]: The function that handles the data received.
  The first parameter is the stream that receives the data, and the second is the data in pandas DataFrame format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_dataframe_received"></a>

#### on\_dataframe\_received

```python
@on_dataframe_received.setter
def on_dataframe_received(
        value: Callable[['StreamConsumer', pandas.DataFrame], None]) -> None
```

Sets the handler for when data is received (without buffering) in pandas DataFrame format.

**Arguments**:

- `value` - The function that handles the data received.
  The first parameter is the stream that receives the data, and the second is the data in pandas DataFrame format.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@property
def on_definitions_changed() -> Callable[['StreamConsumer'], None]
```

Gets the handler for when the parameter definitions have changed for the stream.

**Returns**:

  Callable[['StreamConsumer'], None]: The function that handles the parameter definitions change.
  The first parameter is the stream for which the parameter definitions changed.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@on_definitions_changed.setter
def on_definitions_changed(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for when the parameter definitions have changed for the stream.

**Arguments**:

- `value` - The function that handles the parameter definitions change.
  The first parameter is the stream for which the parameter definitions changed.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.definitions"></a>

#### definitions

```python
@property
def definitions() -> List[ParameterDefinition]
```

Gets the latest set of parameter definitions.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.create_buffer"></a>

#### create\_buffer

```python
def create_buffer(
    *parameter_filter: str,
    buffer_configuration: TimeseriesBufferConfiguration = None
) -> TimeseriesBufferConsumer
```

Creates a new buffer for consuming data according to the provided parameter_filter and buffer_configuration.

**Arguments**:

- `parameter_filter` - Zero or more parameter identifiers to filter as a whitelist. If provided, only those
  parameters will be available through this buffer.
- `buffer_configuration` - An optional TimeseriesBufferConfiguration.
  

**Returns**:

- `TimeseriesBufferConsumer` - An consumer that will raise new data consumed via the on_data_released event.

<a id="quixstreams.models.streamconsumer.streamtimeseriesconsumer.StreamTimeseriesConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the .NET pointer for the StreamTimeseriesConsumer instance.

**Returns**:

- `ctypes.c_void_p` - .NET pointer for the StreamTimeseriesConsumer instance.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer"></a>

# quixstreams.models.streamconsumer.streampropertiesconsumer

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer"></a>

## StreamPropertiesConsumer Objects

```python
@nativedecorator
class StreamPropertiesConsumer(object)
```

Represents properties and metadata of the stream.
All changes to these properties are automatically populated to this class.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer: 'StreamConsumer', net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamPropertiesConsumer.
NOTE: Do not initialize this class manually, use StreamConsumer.properties to access an instance of it.

**Arguments**:

- `stream_consumer` - The Stream consumer that owns this stream event consumer.
- `net_pointer` - Pointer to an instance of a .NET StreamPropertiesConsumer.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.on_changed"></a>

#### on\_changed

```python
@property
def on_changed() -> Callable[['StreamConsumer'], None]
```

Gets the handler for when the stream properties change.

**Returns**:

  Callable[[StreamConsumer], None]: The event handler for stream property changes.
  The first parameter is the StreamConsumer instance for which the change is invoked.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.on_changed"></a>

#### on\_changed

```python
@on_changed.setter
def on_changed(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for when the stream properties change.

**Arguments**:

- `value` - The first parameter is the stream it is invoked for.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.name"></a>

#### name

```python
@property
def name() -> str
```

Gets the name of the stream.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.location"></a>

#### location

```python
@property
def location() -> str
```

Gets the location of the stream.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.time_of_recording"></a>

#### time\_of\_recording

```python
@property
def time_of_recording() -> datetime
```

Gets the datetime of the recording.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.metadata"></a>

#### metadata

```python
@property
def metadata() -> Dict[str, str]
```

Gets the metadata of the stream.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.parents"></a>

#### parents

```python
@property
def parents() -> List[str]
```

Gets the list of Stream IDs for the parent streams.

<a id="quixstreams.models.streamconsumer.streampropertiesconsumer.StreamPropertiesConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the .NET pointer for the StreamPropertiesConsumer instance.

**Returns**:

- `ctypes.c_void_p` - .NET pointer for the StreamPropertiesConsumer instance.

<a id="quixstreams.models.streamconsumer.streameventsconsumer"></a>

# quixstreams.models.streamconsumer.streameventsconsumer

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer"></a>

## StreamEventsConsumer Objects

```python
@nativedecorator
class StreamEventsConsumer(object)
```

Consumer for streams, which raises EventData and EventDefinitions related messages

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer, net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamEventsConsumer.
NOTE: Do not initialize this class manually, use StreamConsumer.events to access an instance of it

**Arguments**:

- `stream_consumer` - The Stream consumer which owns this stream event consumer
- `net_pointer` - Pointer to an instance of a .net StreamEventsConsumer

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received"></a>

#### on\_data\_received

```python
@property
def on_data_received() -> Callable[['StreamConsumer', EventData], None]
```

Gets the handler for when an events data package is received for the stream.

**Returns**:

  Callable[['StreamConsumer', EventData], None]:
  The first parameter is the stream the event is received for. The second is the event.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received"></a>

#### on\_data\_received

```python
@on_data_received.setter
def on_data_received(
        value: Callable[['StreamConsumer', EventData], None]) -> None
```

Sets the handler for when an events data package is received for the stream.

**Arguments**:

- `value` - The first parameter is the stream the event is received for. The second is the event.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@property
def on_definitions_changed() -> Callable[['StreamConsumer'], None]
```

Gets the handler for event definitions have changed for the stream.

**Returns**:

  Callable[['StreamConsumer'], None]:
  The first parameter is the stream the event definitions changed for.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed"></a>

#### on\_definitions\_changed

```python
@on_definitions_changed.setter
def on_definitions_changed(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the handler for event definitions have changed for the stream.

**Arguments**:

- `value` - The first parameter is the stream the event definitions changed for.

<a id="quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.definitions"></a>

#### definitions

```python
@property
def definitions() -> List[EventDefinition]
```

Gets the latest set of event definitions.

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer"></a>

# quixstreams.models.streamconsumer.timeseriesbufferconsumer

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer"></a>

## TimeseriesBufferConsumer Objects

```python
class TimeseriesBufferConsumer(TimeseriesBuffer)
```

Represents a class for consuming data from a stream in a buffered manner.

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(stream_consumer, net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesBufferConsumer.

NOTE: Do not initialize this class manually,
use StreamTimeseriesConsumer.create_buffer to create it.

**Arguments**:

- `stream_consumer` - The Stream consumer which owns this timeseries buffer consumer.
- `net_pointer` - Pointer to an instance of a .net TimeseriesBufferConsumer.
  Defaults to None.
  

**Raises**:

- `Exception` - If net_pointer is None.

<a id="quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Retrieves the pointer to the .net TimeseriesBufferConsumer instance.

**Returns**:

- `ctypes.c_void_p` - The pointer to the .net TimeseriesBufferConsumer instance.

<a id="quixstreams.models.streamconsumer"></a>

# quixstreams.models.streamconsumer

<a id="quixstreams.models.commitmode"></a>

# quixstreams.models.commitmode

<a id="quixstreams.models.codectype"></a>

# quixstreams.models.codectype

<a id="quixstreams.models.codectype.CodecType"></a>

## CodecType Objects

```python
class CodecType(Enum)
```

Codecs available for serialization and deserialization of streams.

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

<a id="quixstreams.models.timeseriesbufferconfiguration"></a>

# quixstreams.models.timeseriesbufferconfiguration

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration"></a>

## TimeseriesBufferConfiguration Objects

```python
@nativedecorator
class TimeseriesBufferConfiguration(object)
```

Describes the configuration for timeseries buffers
When buffer conditions are not configured, it acts a pass-through, raising each message as arrives.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of TimeseriesBufferConfiguration.

**Arguments**:

- `net_pointer` - Can be ignored, here for internal purposes .net object: The .net object representing a TimeseriesBufferConfiguration. Defaults to None.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.packet_size"></a>

#### packet\_size

```python
@property
def packet_size() -> Optional[int]
```

Gets the maximum packet size in terms of values for the buffer.

When the buffer reaches this number of values, a callback method, such as on_data_released, is invoked and the buffer is cleared.
If not set, defaults to None (disabled).

**Returns**:

- `Optional[int]` - The maximum packet size in values or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.packet_size"></a>

#### packet\_size

```python
@packet_size.setter
def packet_size(value: Optional[int])
```

Sets the maximum packet size in terms of values for the buffer.

When the buffer reaches this number of values, a callback method, such as on_data_released, is invoked and the buffer is cleared.
If not set, defaults to None (disabled).

**Arguments**:

- `value` - The maximum packet size in values or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@property
def time_span_in_nanoseconds() -> Optional[int]
```

Gets the maximum time difference between timestamps in the buffer, in nanoseconds.

When the difference between the earliest and latest buffered timestamp exceeds this value, a callback
method, such as on_data_released, is invoked and the data is cleared from the buffer. If not set, defaults to None (disabled).

**Returns**:

- `Optional[int]` - The maximum time span in nanoseconds or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_nanoseconds"></a>

#### time\_span\_in\_nanoseconds

```python
@time_span_in_nanoseconds.setter
def time_span_in_nanoseconds(value: Optional[int])
```

Sets the maximum time difference between timestamps in the buffer, in nanoseconds.

When the difference between the earliest and latest buffered timestamp exceeds this value, a callback method,
such as on_data_released, is invoked and the data is cleared from the buffer. If not set, defaults to None (disabled).

**Arguments**:

- `value` - The maximum time span in nanoseconds or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@property
def time_span_in_milliseconds() -> Optional[int]
```

Gets the maximum time difference between timestamps in the buffer, in milliseconds.

When the difference between the earliest and latest buffered timestamp exceeds this value, a callback method,
such as on_data_released, is invoked and the data is cleared from the buffer. If not set, defaults to None (disabled).

Note: This is a millisecond converter on top of time_span_in_nanoseconds. They both work with the same underlying value.

**Returns**:

- `Optional[int]` - The maximum time span in milliseconds or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.time_span_in_milliseconds"></a>

#### time\_span\_in\_milliseconds

```python
@time_span_in_milliseconds.setter
def time_span_in_milliseconds(value: Optional[int])
```

Sets the maximum time difference between timestamps in the buffer, in milliseconds.

When the difference between the earliest and latest buffered timestamp exceeds this value, a callback method,
such as on_data_released, is invoked and the data is cleared from the buffer. If not set, defaults to None (disabled).

**Arguments**:

- `value` - The maximum time span in nanoseconds or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.buffer_timeout"></a>

#### buffer\_timeout

```python
@property
def buffer_timeout() -> Optional[int]
```

Gets the maximum duration for which the buffer will be held before releasing the events through callbacks, such as on_data_released.

A callback will be invoked once the configured value has elapsed or other buffer conditions are met.
If not set, defaults to None (disabled).

**Returns**:

- `Optional[int]` - The maximum buffer timeout in milliseconds or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.buffer_timeout"></a>

#### buffer\_timeout

```python
@buffer_timeout.setter
def buffer_timeout(value: Optional[int])
```

Sets the maximum duration for which the buffer will be held before releasing the events through callbacks, such as on_data_released.

A callback will be invoked once the configured value has elapsed or other buffer conditions are met.
If not set, defaults to None (disabled).

**Arguments**:

- `value` - The maximum buffer timeout in milliseconds or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger_before_enqueue"></a>

#### custom\_trigger\_before\_enqueue

```python
@property
def custom_trigger_before_enqueue(
) -> Callable[[TimeseriesDataTimestamp], bool]
```

Gets the custom function that is called before adding a timestamp to the buffer.

If the function returns True, the buffer releases content and triggers relevant callbacks before adding the timestamp to
the buffer. If not set, defaults to None (disabled).

**Returns**:

  Callable[[TimeseriesDataTimestamp], bool]: The custom function or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger_before_enqueue"></a>

#### custom\_trigger\_before\_enqueue

```python
@custom_trigger_before_enqueue.setter
def custom_trigger_before_enqueue(value: Callable[[TimeseriesDataTimestamp],
                                                  bool])
```

Sets the custom function that is called before adding a timestamp to the buffer.

If the function returns True, the buffer releases content and triggers relevant callbacks before adding the timestamp
to the buffer. If not set, defaults to None (disabled).

**Arguments**:

- `value` - The custom function or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.filter"></a>

#### filter

```python
@property
def filter() -> Callable[[TimeseriesDataTimestamp], bool]
```

Gets the custom function used to filter incoming data before adding it to the buffer.

If the function returns True, the data is added to the buffer; otherwise, it is not. If not set, defaults to None
(disabled).

**Returns**:

  Callable[[TimeseriesDataTimestamp], bool]: The custom filter function or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.filter"></a>

#### filter

```python
@filter.setter
def filter(value: Callable[[TimeseriesDataTimestamp], bool])
```

Sets the custom function used to filter incoming data before adding it to the buffer.

If the function returns True, the data is added to the buffer; otherwise, it is not. If not set, defaults to None
(disabled).

**Arguments**:

- `value` - The custom filter function or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger"></a>

#### custom\_trigger

```python
@property
def custom_trigger() -> Callable[[TimeseriesData], bool]
```

Gets the custom function that is called after adding a new timestamp to the buffer.

If the function returns True, the buffer releases content and triggers relevant callbacks.
If not set, defaults to None (disabled).

**Returns**:

  Callable[[TimeseriesData], bool]: The custom trigger function or None if disabled.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.custom_trigger"></a>

#### custom\_trigger

```python
@custom_trigger.setter
def custom_trigger(value: Callable[[TimeseriesData], bool])
```

Sets the custom function that is called after adding a new timestamp to the buffer.

If the function returns True, the buffer releases content and triggers relevant callbacks.
If not set, defaults to None (disabled).

**Arguments**:

- `value` - The custom trigger function or None to disable.

<a id="quixstreams.models.timeseriesbufferconfiguration.TimeseriesBufferConfiguration.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Returns the .net pointer for the TimeseriesBufferConfiguration object.

**Returns**:

- `ctypes.c_void_p` - The .net pointer for the TimeseriesBufferConfiguration object.

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

<a id="quixstreams.models.timeseriesdatatimestamp"></a>

# quixstreams.models.timeseriesdatatimestamp

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp"></a>

## TimeseriesDataTimestamp Objects

```python
@nativedecorator
class TimeseriesDataTimestamp()
```

Represents a single point in time with parameter values and tags attached to that time.

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

Gets the parameter values for the timestamp as a dictionary. If a key is not found, returns an empty ParameterValue.

**Returns**:

  Dict[str, ParameterValue]: A dictionary with parameter id as key and ParameterValue as value.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.tags"></a>

#### tags

```python
@property
def tags() -> Dict[str, str]
```

Gets the tags for the timestamp as a dictionary.

**Returns**:

  Dict[str, str]: A dictionary with tag id as key and tag value as value.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_nanoseconds"></a>

#### timestamp\_nanoseconds

```python
@property
def timestamp_nanoseconds() -> int
```

Gets the timestamp in nanoseconds.

**Returns**:

- `int` - The timestamp in nanoseconds.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_milliseconds"></a>

#### timestamp\_milliseconds

```python
@property
def timestamp_milliseconds() -> int
```

Gets the timestamp in milliseconds.

**Returns**:

- `int` - The timestamp in milliseconds.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp"></a>

#### timestamp

```python
@property
def timestamp() -> datetime
```

Gets the timestamp in datetime format.

**Returns**:

- `datetime` - The timestamp in datetime format.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.timestamp_as_time_span"></a>

#### timestamp\_as\_time\_span

```python
@property
def timestamp_as_time_span() -> timedelta
```

Gets the timestamp in timespan format.

**Returns**:

- `timedelta` - The timestamp in timespan format.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_value"></a>

#### add\_value

```python
def add_value(
    parameter_id: str, value: Union[numbers.Number, str, bytearray, bytes]
) -> 'TimeseriesDataTimestamp'
```

Adds a new value for the specified parameter.

**Arguments**:

- `parameter_id` - The parameter id to add the value for.
- `value` - The value to add. Can be a number, string, bytearray, or bytes.
  

**Returns**:

- `TimeseriesDataTimestamp` - The updated TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.remove_value"></a>

#### remove\_value

```python
def remove_value(parameter_id: str) -> 'TimeseriesDataTimestamp'
```

Removes the value for the specified parameter.

**Arguments**:

- `parameter_id` - The parameter id to remove the value for.
  

**Returns**:

- `TimeseriesDataTimestamp` - The updated TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, tag_value: str) -> 'TimeseriesDataTimestamp'
```

Adds a tag to the timestamp.

**Arguments**:

- `tag_id` - The id of the tag to add.
- `tag_value` - The value of the tag to add.
  

**Returns**:

- `TimeseriesDataTimestamp` - The updated TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.remove_tag"></a>

#### remove\_tag

```python
def remove_tag(tag_id: str) -> 'TimeseriesDataTimestamp'
```

Removes a tag from the timestamp.

**Arguments**:

- `tag_id` - The id of the tag to remove.
  

**Returns**:

- `TimeseriesDataTimestamp` - The updated TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'TimeseriesDataTimestamp'
```

Copies the tags from the specified dictionary. Conflicting tags will be overwritten.

**Arguments**:

- `tags` - The dictionary of tags to add, with tag id as key and tag value as value.
  

**Returns**:

- `TimeseriesDataTimestamp` - The updated TimeseriesDataTimestamp instance.

<a id="quixstreams.models.timeseriesdatatimestamp.TimeseriesDataTimestamp.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the .net pointer of the TimeseriesDataTimestamp instance.

**Returns**:

- `ctypes.c_void_p` - The .net pointer of the TimeseriesDataTimestamp instance.

<a id="quixstreams.models.streamendtype"></a>

# quixstreams.models.streamendtype

<a id="quixstreams.models.eventdata"></a>

# quixstreams.models.eventdata

<a id="quixstreams.models.eventdata.EventData"></a>

## EventData Objects

```python
@nativedecorator
class EventData(object)
```

Represents a single point in time with event value and tags attached to it.

<a id="quixstreams.models.eventdata.EventData.__init__"></a>

#### \_\_init\_\_

```python
def __init__(event_id: str = None,
             time: Union[int, str, datetime, pd.Timestamp] = None,
             value: str = None,
             net_pointer: ctypes.c_void_p = None)
```

Initializes a new instance of EventData.

**Arguments**:

- `event_id` - The unique id of the event the value belongs to.
- `time` - The time at which the event has occurred in nanoseconds since epoch or as a datetime.
- `value` - The value of the event.
- `net_pointer` - Pointer to an instance of a .net EventData.

<a id="quixstreams.models.eventdata.EventData.id"></a>

#### id

```python
@property
def id() -> str
```

Gets the globally unique identifier of the event.

<a id="quixstreams.models.eventdata.EventData.id"></a>

#### id

```python
@id.setter
def id(value: str) -> None
```

Sets the globally unique identifier of the event.

<a id="quixstreams.models.eventdata.EventData.value"></a>

#### value

```python
@property
def value() -> str
```

Gets the value of the event.

<a id="quixstreams.models.eventdata.EventData.value"></a>

#### value

```python
@value.setter
def value(value: str) -> None
```

Sets the value of the event.

<a id="quixstreams.models.eventdata.EventData.tags"></a>

#### tags

```python
@property
def tags() -> Dict[str, str]
```

Gets the tags for the timestamp.

If a key is not found, it returns None.
The dictionary key is the tag id.
The dictionary value is the tag value.

<a id="quixstreams.models.eventdata.EventData.timestamp_nanoseconds"></a>

#### timestamp\_nanoseconds

```python
@property
def timestamp_nanoseconds() -> int
```

Gets timestamp in nanoseconds.

<a id="quixstreams.models.eventdata.EventData.timestamp_milliseconds"></a>

#### timestamp\_milliseconds

```python
@property
def timestamp_milliseconds() -> int
```

Gets timestamp in milliseconds.

<a id="quixstreams.models.eventdata.EventData.timestamp"></a>

#### timestamp

```python
@property
def timestamp() -> datetime
```

Gets the timestamp in datetime format.

<a id="quixstreams.models.eventdata.EventData.timestamp_as_time_span"></a>

#### timestamp\_as\_time\_span

```python
@property
def timestamp_as_time_span() -> timedelta
```

Gets the timestamp in timespan format.

<a id="quixstreams.models.eventdata.EventData.clone"></a>

#### clone

```python
def clone()
```

Clones the event data.

**Returns**:

- `EventData` - Cloned EventData object.

<a id="quixstreams.models.eventdata.EventData.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, tag_value: str) -> 'EventData'
```

Adds a tag to the event.

**Arguments**:

- `tag_id` - The id of the tag.
- `tag_value` - The value to set.
  

**Returns**:

- `EventData` - The updated EventData object.

<a id="quixstreams.models.eventdata.EventData.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'EventData'
```

Adds tags from the specified dictionary. Conflicting tags will be overwritten.

**Arguments**:

- `tags` - The tags to add.
  

**Returns**:

- `EventData` - The updated EventData object.

<a id="quixstreams.models.eventdata.EventData.remove_tag"></a>

#### remove\_tag

```python
def remove_tag(tag_id: str) -> 'EventData'
```

Removes a tag from the event.

**Arguments**:

- `tag_id` - The id of the tag to remove.
  

**Returns**:

- `EventData` - The updated EventData object.

<a id="quixstreams.models.eventdata.EventData.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer()
```

Gets the associated .net object pointer.

**Returns**:

  The .net object pointer.

<a id="quixstreams.models"></a>

# quixstreams.models

<a id="quixstreams.models.autooffsetreset"></a>

# quixstreams.models.autooffsetreset

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset"></a>

## AutoOffsetReset Objects

```python
class AutoOffsetReset(Enum)
```

Enum representing the policy on how a consumer should behave when consuming from a topic partition when there is no initial offset.

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset.Latest"></a>

#### Latest

Latest: Starts from the newest message if there is no stored offset.

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset.Earliest"></a>

#### Earliest

Earliest: Starts from the oldest message if there is no stored offset.

<a id="quixstreams.models.autooffsetreset.AutoOffsetReset.Error"></a>

#### Error

Error: Throws an exception if there is no stored offset.

<a id="quixstreams.app"></a>

# quixstreams.app

<a id="quixstreams.app.CancellationTokenSource"></a>

## CancellationTokenSource Objects

```python
class CancellationTokenSource()
```

Represents a token source that can signal a cancellation System.Threading.CancellationToken

<a id="quixstreams.app.CancellationTokenSource.__init__"></a>

#### \_\_init\_\_

```python
def __init__()
```

Initializes a new instance of the CancellationTokenSource class.

<a id="quixstreams.app.CancellationTokenSource.is_cancellation_requested"></a>

#### is\_cancellation\_requested

```python
def is_cancellation_requested()
```

Checks if a cancellation has been requested.

**Returns**:

- `bool` - True if the cancellation has been requested, False otherwise.

<a id="quixstreams.app.CancellationTokenSource.cancel"></a>

#### cancel

```python
def cancel() -> 'CancellationToken'
```

Signals a cancellation to the CancellationToken.

<a id="quixstreams.app.CancellationTokenSource.token"></a>

#### token

```python
@property
def token() -> 'CancellationToken'
```

Gets the associated CancellationToken.

**Returns**:

- `CancellationToken` - The CancellationToken associated with this CancellationTokenSource.

<a id="quixstreams.app.CancellationTokenSource.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the interop pointer of the CancellationTokenSource object.

**Returns**:

- `ctypes.c_void_p` - The interop pointer of the CancellationTokenSource object.

<a id="quixstreams.app.App"></a>

## App Objects

```python
class App()
```

Provides utilities to handle default streaming behaviors and automatic resource cleanup on shutdown.

<a id="quixstreams.app.App.run"></a>

#### run

```python
@staticmethod
def run(cancellation_token: CancellationToken = None,
        before_shutdown: Callable[[], None] = None)
```

Runs the application, managing streaming behaviors and automatic resource cleanup on shutdown.

**Arguments**:

- `cancellation_token` - An optional CancellationToken to abort the application run with.
- `before_shutdown` - An optional function to call before shutting down the application.

<a id="quixstreams.app.App.get_state_manager"></a>

#### get\_state\_manager

```python
@staticmethod
def get_state_manager() -> AppStateManager
```

Retrieves the state manager for the application

**Returns**:

- `AppStateManager` - the app's state manager

<a id="quixstreams.app.App.set_state_storage"></a>

#### set\_state\_storage

```python
@staticmethod
def set_state_storage(storage: IStateStorage) -> None
```

Sets the state storage for the app

**Arguments**:

- `storage` - The state storage to use for app's state manager

<a id="quixstreams.states.topicstatemanager"></a>

# quixstreams.states.topicstatemanager

<a id="quixstreams.states.topicstatemanager.TopicStateManager"></a>

## TopicStateManager Objects

```python
@nativedecorator
class TopicStateManager(object)
```

Manages the states of a topic.

<a id="quixstreams.states.topicstatemanager.TopicStateManager.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TopicStateManager.

NOTE: Do not initialize this class manually, use TopicConsumer.get_state_manager

**Arguments**:

- `net_pointer` - The .net object representing a TopicStateManager.

<a id="quixstreams.states.topicstatemanager.TopicStateManager.get_stream_states"></a>

#### get\_stream\_states

```python
def get_stream_states() -> List[str]
```

Returns a collection of all available stream state ids for the current topic.

**Returns**:

- `List[str]` - All available stream state ids for the current topic.

<a id="quixstreams.states.topicstatemanager.TopicStateManager.get_stream_state_manager"></a>

#### get\_stream\_state\_manager

```python
def get_stream_state_manager(stream_id: str) -> StreamStateManager
```

Gets an instance of the StreamStateManager for the specified stream_id.

**Arguments**:

- `stream_id` - The ID of the stream

<a id="quixstreams.states.topicstatemanager.TopicStateManager.delete_stream_state"></a>

#### delete\_stream\_state

```python
def delete_stream_state(stream_id: str) -> bool
```

Deletes the stream state for the specified stream

**Arguments**:

- `stream_id` - The ID of the stream
  

**Returns**:

- `bool` - Whether the stream state was deleted

<a id="quixstreams.states.topicstatemanager.TopicStateManager.delete_stream_states"></a>

#### delete\_stream\_states

```python
def delete_stream_states() -> int
```

Deletes all stream states for the current topic.

**Returns**:

- `int` - The number of stream states that were deleted

<a id="quixstreams.states.streamstatemanager"></a>

# quixstreams.states.streamstatemanager

<a id="quixstreams.states.streamstatemanager.StreamStateManager"></a>

## StreamStateManager Objects

```python
@nativedecorator
class StreamStateManager(object)
```

Manages the states of a stream.

<a id="quixstreams.states.streamstatemanager.StreamStateManager.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamStateManager.

NOTE: Do not initialize this class manually, use StreamConsumer.get_state_manager

**Arguments**:

- `net_pointer` - The .net object representing a StreamStateManager.

<a id="quixstreams.states.streamstatemanager.StreamStateManager.get_dict_state"></a>

#### get\_dict\_state

```python
def get_dict_state(
        state_name: str,
        default_value_factory: Callable[[str], StreamStateType] = None,
        state_type: StreamStateType = None
) -> DictStreamState[StreamStateType]
```

Creates a new application state of dictionary type with automatically managed lifecycle for the stream

**Arguments**:

- `state_name` - The name of the state
- `state_type` - The type of the state
- `default_value_factory` - The default value factory to create value when the key is not yet present in the state
  

**Example**:

  >>> state_manager.get_dict_state('some_state')
  This will return a state where type is 'Any'
  
  >>> state_manager.get_dict_state('some_state', lambda missing_key: return {})
  this will return a state where type is a generic dictionary, with an empty dictionary as default value when
  key is not available. The lambda function will be invoked with 'get_state_type_check' key to determine type
  
  >>> state_manager.get_dict_state('some_state', lambda missing_key: return {}, Dict[str, float])
  this will return a state where type is a specific dictionary type, with default value
  
  >>> state_manager.get_dict_state('some_state', state_type=float)
  this will return a state where type is a float without default value, resulting in KeyError when not found

<a id="quixstreams.states.streamstatemanager.StreamStateManager.get_scalar_state"></a>

#### get\_scalar\_state

```python
def get_scalar_state(
        state_name: str,
        default_value_factory: Callable[[], StreamStateType] = None,
        state_type: StreamStateType = None
) -> ScalarStreamState[StreamStateType]
```

Creates a new application state of scalar type with automatically managed lifecycle for the stream

**Arguments**:

- `state_name` - The name of the state
- `default_value_factory` - The default value factory to create value when it has not been set yet
- `state_type` - The type of the state
  

**Example**:

  >>> stream_consumer.get_scalar_state('some_state')
  This will return a state where type is 'Any'
  
  >>> stream_consumer.get_scalar_state('some_state', lambda missing_key: return 1)
  this will return a state where type is 'Any', with an integer 1 (zero) as default when
  value has not been set yet. The lambda function will be invoked with 'get_state_type_check' key to determine type
  
  >>> stream_consumer.get_scalar_state('some_state', lambda missing_key: return 0, float)
  this will return a state where type is a specific type, with default value
  
  >>> stream_consumer.get_scalar_state('some_state', state_type=float)
  this will return a state where type is a float with a default value of that type

<a id="quixstreams.states.streamstatemanager.StreamStateManager.get_states"></a>

#### get\_states

```python
def get_states() -> List[str]
```

Returns an enumerable collection of all available state names for the current stream.

**Returns**:

- `List[str]` - All available stream state names for this state

<a id="quixstreams.states.streamstatemanager.StreamStateManager.delete_state"></a>

#### delete\_state

```python
def delete_state(state_name: str) -> bool
```

Deletes the state with the specified name

**Arguments**:

- `state_name` - The state to delete
  

**Returns**:

- `bool` - Whether the state was deleted<

<a id="quixstreams.states.streamstatemanager.StreamStateManager.delete_states"></a>

#### delete\_states

```python
def delete_states() -> int
```

Deletes all states for the current stream.

**Returns**:

- `int` - The number of states that were deleted

<a id="quixstreams.states.scalarstreamstate"></a>

# quixstreams.states.scalarstreamstate

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState"></a>

## ScalarStreamState Objects

```python
@nativedecorator
class ScalarStreamState(Generic[StreamStateType])
```

Represents a state container that stores a scalar value with the ability to flush changes to a specified storage.

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p, state_type: StreamStateType,
             default_value_factory: Callable[[], StreamStateType])
```

Initializes a new instance of ScalarStreamState.

NOTE: Do not initialize this class manually, use StreamStateManager.get_scalar_state

**Arguments**:

- `net_pointer` - The .net object representing a ScalarStreamState.
- `state_type` - The type of the state
- `default_value_factory` - A function that returns a default value of type T when the value has not been set yet

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.type"></a>

#### type

```python
@property
def type() -> type
```

Gets the type of the ScalarStreamState

**Returns**:

- `StreamStateType` - type of the state

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushed"></a>

#### on\_flushed

```python
@property
def on_flushed() -> Callable[[], None]
```

Gets the handler for when flush operation is completed.

**Returns**:

  Callable[[], None]: The event handler for after flush.

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushed"></a>

#### on\_flushed

```python
@on_flushed.setter
def on_flushed(value: Callable[[], None]) -> None
```

Sets the handler for when flush operation is completed.

**Arguments**:

- `value` - The parameterless callback to invoke

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushing"></a>

#### on\_flushing

```python
@property
def on_flushing() -> Callable[[], None]
```

Gets the handler for when flush operation begins.

**Returns**:

  Callable[[], None]: The event handler for after flush.

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.on_flushing"></a>

#### on\_flushing

```python
@on_flushing.setter
def on_flushing(value: Callable[[], None]) -> None
```

Sets the handler for when flush operation begins.

**Arguments**:

- `value` - The parameterless callback to invoke

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.flush"></a>

#### flush

```python
def flush()
```

Flushes the changes made to the in-memory state to the specified storage.

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.reset"></a>

#### reset

```python
def reset()
```

Reset the state to before in-memory modifications

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.value"></a>

#### value

```python
@property
def value()
```

Gets the value of the state.

**Returns**:

- `StreamStateType` - The value of the state.

<a id="quixstreams.states.scalarstreamstate.ScalarStreamState.value"></a>

#### value

```python
@value.setter
def value(val: StreamStateType)
```

Sets the value of the state.

**Arguments**:

- `val` - The value of the state.

<a id="quixstreams.states.dictstreamstate"></a>

# quixstreams.states.dictstreamstate

<a id="quixstreams.states.dictstreamstate.DictStreamState"></a>

## DictStreamState Objects

```python
@nativedecorator
class DictStreamState(Generic[StreamStateType])
```

Represents a state container that stores key-value pairs with the ability to flush changes to a specified storage.

<a id="quixstreams.states.dictstreamstate.DictStreamState.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p, state_type: StreamStateType,
             default_value_factory: Callable[[str], StreamStateType])
```

Initializes a new instance of DictStreamState.

NOTE: Do not initialize this class manually, use StreamStateManager.get_dict_state

**Arguments**:

- `net_pointer` - The .net object representing a DictStreamState.
- `state_type` - The type of the state
- `default_value_factory` - The default value factory to create value when the key is not yet present in the state

<a id="quixstreams.states.dictstreamstate.DictStreamState.type"></a>

#### type

```python
@property
def type() -> type
```

Gets the type of the StreamState

**Returns**:

- `StreamStateType` - type of the state

<a id="quixstreams.states.dictstreamstate.DictStreamState.on_flushed"></a>

#### on\_flushed

```python
@property
def on_flushed() -> Callable[[], None]
```

Gets the handler for when flush operation is completed.

**Returns**:

  Callable[[], None]: The event handler for after flush.

<a id="quixstreams.states.dictstreamstate.DictStreamState.on_flushed"></a>

#### on\_flushed

```python
@on_flushed.setter
def on_flushed(value: Callable[[], None]) -> None
```

Sets the handler for when flush operation is completed.

**Arguments**:

- `value` - The parameterless callback to invoke

<a id="quixstreams.states.dictstreamstate.DictStreamState.on_flushing"></a>

#### on\_flushing

```python
@property
def on_flushing() -> Callable[[], None]
```

Gets the handler for when flush operation begins.

**Returns**:

  Callable[[], None]: The event handler for after flush.

<a id="quixstreams.states.dictstreamstate.DictStreamState.on_flushing"></a>

#### on\_flushing

```python
@on_flushing.setter
def on_flushing(value: Callable[[], None]) -> None
```

Sets the handler for when flush operation begins.

**Arguments**:

- `value` - The parameterless callback to invoke

<a id="quixstreams.states.dictstreamstate.DictStreamState.flush"></a>

#### flush

```python
def flush()
```

Flushes the changes made to the in-memory state to the specified storage.

<a id="quixstreams.states.dictstreamstate.DictStreamState.reset"></a>

#### reset

```python
def reset()
```

Reset the state to before in-memory modifications

<a id="quixstreams.states"></a>

# quixstreams.states

<a id="quixstreams.states.appstatemanager"></a>

# quixstreams.states.appstatemanager

<a id="quixstreams.states.appstatemanager.AppStateManager"></a>

## AppStateManager Objects

```python
@nativedecorator
class AppStateManager(object)
```

Manages the states of an app.

<a id="quixstreams.states.appstatemanager.AppStateManager.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of AppStateManager.

NOTE: Do not initialize this class manually, use App.get_state_manager

**Arguments**:

- `net_pointer` - The .net object representing a AppStateManager.

<a id="quixstreams.states.appstatemanager.AppStateManager.get_topic_states"></a>

#### get\_topic\_states

```python
def get_topic_states() -> List[str]
```

Returns a collection of all available topic states for the app.

**Returns**:

- `List[str]` - All available app topic states for the app.

<a id="quixstreams.states.appstatemanager.AppStateManager.get_topic_state_manager"></a>

#### get\_topic\_state\_manager

```python
def get_topic_state_manager(topic_name: str) -> TopicStateManager
```

Gets an instance of the TopicStateManager for the specified topic.

**Arguments**:

- `topic_name` - The name of the topic

<a id="quixstreams.states.appstatemanager.AppStateManager.delete_topic_state"></a>

#### delete\_topic\_state

```python
def delete_topic_state(topic_name: str) -> bool
```

Deletes the specified topic state

**Arguments**:

- `topic_name` - The name of the topic
  

**Returns**:

- `bool` - Whether the topic state was deleted

<a id="quixstreams.states.appstatemanager.AppStateManager.delete_topic_states"></a>

#### delete\_topic\_states

```python
def delete_topic_states() -> int
```

Deletes all topic states for the app.

**Returns**:

- `int` - The number of topic states that were deleted

<a id="quixstreams.topicconsumer"></a>

# quixstreams.topicconsumer

<a id="quixstreams.topicconsumer.TopicConsumer"></a>

## TopicConsumer Objects

```python
@nativedecorator
class TopicConsumer(object)
```

Interface to operate with the streaming platform for consuming messages

<a id="quixstreams.topicconsumer.TopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p)
```

Initializes a new instance of TopicConsumer.

NOTE: Do not initialize this class manually, use KafkaStreamingClient.get_topic_consumer to create it.

**Arguments**:

- `net_pointer` - The .net pointer to TopicConsumer instance.

<a id="quixstreams.topicconsumer.TopicConsumer.on_stream_received"></a>

#### on\_stream\_received

```python
@property
def on_stream_received() -> Callable[['StreamConsumer'], None]
```

Gets the event handler for when a stream is received for the topic.

**Returns**:

  Callable[[StreamConsumer], None]: The event handler for when a stream is received for the topic.
  The first parameter is the StreamConsumer instance that was received.

<a id="quixstreams.topicconsumer.TopicConsumer.on_stream_received"></a>

#### on\_stream\_received

```python
@on_stream_received.setter
def on_stream_received(value: Callable[['StreamConsumer'], None]) -> None
```

Sets the event handler for when a stream is received for the topic.

**Arguments**:

- `value` - The new event handler for when a stream is received for the topic.
  The first parameter is the StreamConsumer instance that was received.

<a id="quixstreams.topicconsumer.TopicConsumer.on_streams_revoked"></a>

#### on\_streams\_revoked

```python
@property
def on_streams_revoked(
) -> Callable[['TopicConsumer', List['StreamConsumer']], None]
```

Gets the event handler for when streams are revoked for the topic.

**Returns**:

  Callable[[TopicConsumer, List[StreamConsumer]], None]: The event handler for when streams are revoked for the topic.
  The first parameter is the TopicConsumer instance for which the streams were revoked, and the second parameter is a list of StreamConsumer instances that were revoked.

<a id="quixstreams.topicconsumer.TopicConsumer.on_streams_revoked"></a>

#### on\_streams\_revoked

```python
@on_streams_revoked.setter
def on_streams_revoked(
        value: Callable[['TopicConsumer', List['StreamConsumer']],
                        None]) -> None
```

Sets the event handler for when streams are revoked for the topic.

**Arguments**:

- `value` - The new event handler for when streams are revoked for the topic.
  The first parameter is the TopicConsumer instance for which the streams were revoked, and the second parameter is a list of StreamConsumer instances that were revoked.

<a id="quixstreams.topicconsumer.TopicConsumer.on_revoking"></a>

#### on\_revoking

```python
@property
def on_revoking() -> Callable[['TopicConsumer'], None]
```

Gets the event handler for when the topic is being revoked.

**Returns**:

  Callable[[TopicConsumer], None]: The event handler for when the topic is being revoked.
  The first parameter is the TopicConsumer instance for which the revocation is happening.

<a id="quixstreams.topicconsumer.TopicConsumer.on_revoking"></a>

#### on\_revoking

```python
@on_revoking.setter
def on_revoking(value: Callable[['TopicConsumer'], None]) -> None
```

Sets the event handler for when the topic is being revoked.

**Arguments**:

- `value` - The new event handler for when the topic is being revoked.
  The first parameter is the TopicConsumer instance for which the revocation is happening.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committed"></a>

#### on\_committed

```python
@property
def on_committed() -> Callable[['TopicConsumer'], None]
```

Gets the event handler for when the topic finishes committing consumed data up to this point.

**Returns**:

  Callable[[TopicConsumer], None]: The event handler for when the topic finishes committing consumed data up to this point.
  The first parameter is the TopicConsumer instance for which the commit happened.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committed"></a>

#### on\_committed

```python
@on_committed.setter
def on_committed(value: Callable[['TopicConsumer'], None]) -> None
```

Sets the event handler for when the topic finishes committing consumed data up to this point.

**Arguments**:

- `value` - The new event handler for when the topic finishes committing consumed data up to this point.
  The first parameter is the TopicConsumer instance for which the commit happened.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committing"></a>

#### on\_committing

```python
@property
def on_committing() -> Callable[['TopicConsumer'], None]
```

Gets the event handler for when the topic begins committing consumed data up to this point.

**Returns**:

  Callable[[TopicConsumer], None]: The event handler for when the topic begins committing consumed data up to this point.
  The first parameter is the TopicConsumer instance for which the commit is happening.

<a id="quixstreams.topicconsumer.TopicConsumer.on_committing"></a>

#### on\_committing

```python
@on_committing.setter
def on_committing(value: Callable[['TopicConsumer'], None]) -> None
```

Sets the event handler for when the topic begins committing consumed data up to this point.

**Arguments**:

- `value` - The new event handler for when the topic begins committing consumed data up to this point.
  The first parameter is the TopicConsumer instance for which the commit is happening.

<a id="quixstreams.topicconsumer.TopicConsumer.subscribe"></a>

#### subscribe

```python
def subscribe()
```

Subscribes to streams in the topic.
Use 'on_stream_received' event to consume incoming streams.

<a id="quixstreams.topicconsumer.TopicConsumer.commit"></a>

#### commit

```python
def commit()
```

Commit packages consumed up until now

<a id="quixstreams.topicconsumer.TopicConsumer.get_state_manager"></a>

#### get\_state\_manager

```python
def get_state_manager() -> TopicStateManager
```

Gets the manager for the topic states.

**Returns**:

- `TopicStateManager` - The topic state manager

<a id="quixstreams.topicconsumer.TopicConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Retrieves the .net pointer to TopicConsumer instance.

**Returns**:

- `ctypes.c_void_p` - The .net pointer to TopicConsumer instance.

<a id="quixstreams.streamconsumer"></a>

# quixstreams.streamconsumer

<a id="quixstreams.streamconsumer.StreamConsumer"></a>

## StreamConsumer Objects

```python
@nativedecorator
class StreamConsumer(object)
```

Handles consuming stream from a topic.

<a id="quixstreams.streamconsumer.StreamConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: ctypes.c_void_p, topic_consumer: 'TopicConsumer',
             on_close_cb_always: Callable[['StreamConsumer'], None])
```

Initializes a new instance of StreamConsumer.

NOTE: Do not initialize this class manually, TopicProducer automatically creates this when a new stream is received.

**Arguments**:

- `net_pointer` - Pointer to an instance of a .NET StreamConsumer.
- `topic_consumer` - The topic consumer which owns the stream consumer.
- `on_close_cb_always` - The callback function to be executed when the stream is closed.

<a id="quixstreams.streamconsumer.StreamConsumer.topic"></a>

#### topic

```python
@property
def topic() -> 'TopicConsumer'
```

Gets the topic the stream was raised for.

**Returns**:

- `TopicConsumer` - The topic consumer instance associated with the stream.

<a id="quixstreams.streamconsumer.StreamConsumer.on_stream_closed"></a>

#### on\_stream\_closed

```python
@property
def on_stream_closed() -> Callable[['StreamConsumer', 'StreamEndType'], None]
```

Gets the handler for when the stream closes.

**Returns**:

  Callable[['StreamConsumer', 'StreamEndType'], None]: The callback function to be executed when the stream closes.
  The first parameter is the stream that closes, and the second is the close type.

<a id="quixstreams.streamconsumer.StreamConsumer.on_stream_closed"></a>

#### on\_stream\_closed

```python
@on_stream_closed.setter
def on_stream_closed(
        value: Callable[['StreamConsumer', 'StreamEndType'], None]) -> None
```

Sets the handler for when the stream closes.

**Arguments**:

- `value` - The new callback function to be executed when the stream closes.
  The first parameter is the stream that closes, and the second is the close type.

<a id="quixstreams.streamconsumer.StreamConsumer.on_package_received"></a>

#### on\_package\_received

```python
@property
def on_package_received() -> Callable[['StreamConsumer', Any], None]
```

Gets the handler for when the stream receives a package of any type.

**Returns**:

  Callable[['StreamConsumer', Any], None]: The callback function to be executed when the stream receives a package.
  The first parameter is the stream that receives the package, and the second is the package itself.

<a id="quixstreams.streamconsumer.StreamConsumer.on_package_received"></a>

#### on\_package\_received

```python
@on_package_received.setter
def on_package_received(
        value: Callable[['StreamConsumer', Any], None]) -> None
```

Sets the handler for when the stream receives a package of any type.

**Arguments**:

- `value` - The new callback function to be executed when the stream receives a package.
  The first parameter is the stream that receives the package, and the second is the package itself.

<a id="quixstreams.streamconsumer.StreamConsumer.stream_id"></a>

#### stream\_id

```python
@property
def stream_id() -> str
```

Get the ID of the stream being consumed.

**Returns**:

- `str` - The ID of the stream being consumed.

<a id="quixstreams.streamconsumer.StreamConsumer.properties"></a>

#### properties

```python
@property
def properties() -> StreamPropertiesConsumer
```

Gets the consumer for accessing the properties and metadata of the stream.

**Returns**:

- `StreamPropertiesConsumer` - The stream properties consumer instance.

<a id="quixstreams.streamconsumer.StreamConsumer.events"></a>

#### events

```python
@property
def events() -> StreamEventsConsumer
```

Gets the consumer for accessing event related information of the stream such as event definitions and values.

**Returns**:

- `StreamEventsConsumer` - The stream events consumer instance.

<a id="quixstreams.streamconsumer.StreamConsumer.timeseries"></a>

#### timeseries

```python
@property
def timeseries() -> StreamTimeseriesConsumer
```

Gets the consumer for accessing timeseries related information of the stream such as parameter definitions and values.

**Returns**:

- `StreamTimeseriesConsumer` - The stream timeseries consumer instance.

<a id="quixstreams.streamconsumer.StreamConsumer.get_dict_state"></a>

#### get\_dict\_state

```python
def get_dict_state(
        state_name: str,
        default_value_factory: Callable[[str], StreamStateType] = None,
        state_type: StreamStateType = None
) -> DictStreamState[StreamStateType]
```

Creates a new application state of dictionary type with automatically managed lifecycle for the stream

**Arguments**:

- `state_name` - The name of the state
- `default_value_factory` - The default value factory to create value when the key is not yet present in the state
- `state_type` - The type of the state
  

**Returns**:

- `DictStreamState` - The stream state
  

**Example**:

  >>> stream_consumer.get_dict_state('some_state')
  This will return a state where type is 'Any'
  
  >>> stream_consumer.get_dict_state('some_state', lambda missing_key: return {})
  this will return a state where type is a generic dictionary, with an empty dictionary as default value when
  key is not available. The lambda function will be invoked with 'get_state_type_check' key to determine type
  
  >>> stream_consumer.get_dict_state('some_state', lambda missing_key: return {}, Dict[str, float])
  this will return a state where type is a specific dictionary type, with default value
  
  >>> stream_consumer.get_dict_state('some_state', state_type=float)
  this will return a state where type is a float without default value, resulting in KeyError when not found

<a id="quixstreams.streamconsumer.StreamConsumer.get_scalar_state"></a>

#### get\_scalar\_state

```python
def get_scalar_state(
        state_name: str,
        default_value_factory: Callable[[], StreamStateType] = None,
        state_type: StreamStateType = None
) -> ScalarStreamState[StreamStateType]
```

Creates a new application state of scalar type with automatically managed lifecycle for the stream

**Arguments**:

- `state_name` - The name of the state
- `default_value_factory` - The default value factory to create value when it has not been set yet
- `state_type` - The type of the state
  

**Returns**:

- `ScalarStreamState` - The stream state
  

**Example**:

  >>> stream_consumer.get_scalar_state('some_state')
  This will return a state where type is 'Any'
  
  >>> stream_consumer.get_scalar_state('some_state', lambda missing_key: return 1)
  this will return a state where type is 'Any', with an integer 1 (zero) as default when
  value has not been set yet. The lambda function will be invoked with 'get_state_type_check' key to determine type
  
  >>> stream_consumer.get_scalar_state('some_state', lambda missing_key: return 0, float)
  this will return a state where type is a specific type, with default value
  
  >>> stream_consumer.get_scalar_state('some_state', state_type=float)
  this will return a state where type is a float with a default value of that type

<a id="quixstreams.streamconsumer.StreamConsumer.get_state_manager"></a>

#### get\_state\_manager

```python
def get_state_manager() -> StreamStateManager
```

Gets the manager for the stream states.

**Returns**:

- `StreamStateManager` - The stream state manager

<a id="quixstreams.streamconsumer.StreamConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .NET object pointer.

**Returns**:

- `ctypes.c_void_p` - The .NET pointer

<a id="quixstreams.state.localfilestorage"></a>

# quixstreams.state.localfilestorage

<a id="quixstreams.state.localfilestorage.LocalFileStorage"></a>

## LocalFileStorage Objects

```python
class LocalFileStorage(IStateStorage)
```

A directory storage containing the file storage for single process access purposes.
Locking is implemented via in-memory mutex.

<a id="quixstreams.state.localfilestorage.LocalFileStorage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(storage_directory=None, auto_create_dir=True)
```

Initializes the LocalFileStorage instance.

**Arguments**:

- `storage_directory` - The path to the storage directory.
- `auto_create_dir` - If True, automatically creates the storage directory if it doesn't exist.

<a id="quixstreams.state.inmemorystorage"></a>

# quixstreams.state.inmemorystorage

<a id="quixstreams.state.inmemorystorage.InMemoryStorage"></a>

## InMemoryStorage Objects

```python
class InMemoryStorage(IStateStorage)
```

Basic non-thread safe in-memory storage implementing IStateStorage

<a id="quixstreams.state.inmemorystorage.InMemoryStorage.__init__"></a>

#### \_\_init\_\_

```python
def __init__()
```

Initializes the InMemoryStorage instance.

<a id="quixstreams.state.statetype"></a>

# quixstreams.state.statetype

<a id="quixstreams.state.statevalue"></a>

# quixstreams.state.statevalue

<a id="quixstreams.state.statevalue.StateValue"></a>

## StateValue Objects

```python
class StateValue(object)
```

A wrapper class for values that can be stored inside the storage.

<a id="quixstreams.state.statevalue.StateValue.__init__"></a>

#### \_\_init\_\_

```python
def __init__(value: Any)
```

Initializes the wrapped value inside the store.

**Arguments**:

- `value` - The value to be wrapped, which can be one of the following types:
  StateValue, str, int, float, bool, bytes, bytearray, or object (via pickle).

<a id="quixstreams.state.statevalue.StateValue.type"></a>

#### type

```python
@property
def type()
```

Gets the type of the wrapped value.

**Returns**:

- `StateType` - The type of the wrapped value.

<a id="quixstreams.state.statevalue.StateValue.value"></a>

#### value

```python
@property
def value()
```

Gets the wrapped value.

**Returns**:

  The wrapped value.

<a id="quixstreams.state.statevalue.StateValue.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the .NET pointer of the wrapped value.

**Returns**:

- `ctypes.c_void_p` - The .NET pointer of the wrapped value.

<a id="quixstreams.state.istatestorage"></a>

# quixstreams.state.istatestorage

<a id="quixstreams.state.istatestorage.IStateStorage"></a>

## IStateStorage Objects

```python
class IStateStorage(object)
```

The minimum definition for a state storage

<a id="quixstreams.state.istatestorage.IStateStorage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer=ctypes.c_void_p)
```

Initializes a new instance of IStateStorage.

NOTE: Do not initialize this class manually, it can be returned by classes for certain calls

**Arguments**:

- `net_pointer` - The .net object representing a StreamStateManager.

<a id="quixstreams.state.istatestorage.IStateStorage.is_case_sensitive"></a>

#### is\_case\_sensitive

```python
@property
def is_case_sensitive() -> bool
```

Returns whether the storage is case-sensitive

<a id="quixstreams.state.istatestorage.IStateStorage.get_or_create_sub_storage"></a>

#### get\_or\_create\_sub\_storage

```python
def get_or_create_sub_storage(sub_storage_name: str) -> 'IStateStorage'
```

Creates or retrieves the existing storage under this in hierarchy.

**Arguments**:

- `sub_storage_name` - The name of the sub storage
  

**Returns**:

- `IStateStorage` - The state storage for the given storage name

<a id="quixstreams.state.istatestorage.IStateStorage.delete_sub_storages"></a>

#### delete\_sub\_storages

```python
def delete_sub_storages() -> int
```

Deletes the storages under this in hierarchy.

**Returns**:

- `int` - The number of state storage deleted

<a id="quixstreams.state.istatestorage.IStateStorage.delete_sub_storage"></a>

#### delete\_sub\_storage

```python
def delete_sub_storage(sub_storage_name: str) -> bool
```

Deletes a storage under this in hierarchy.

**Arguments**:

- `sub_storage_name` - The name of the sub storage
  

**Returns**:

- `bool` - Whether the state storage for the given storage name was deleted

<a id="quixstreams.state.istatestorage.IStateStorage.get_sub_storages"></a>

#### get\_sub\_storages

```python
def get_sub_storages() -> List[str]
```

Gets the storages under this in hierarchy.

**Returns**:

- `IStateStorage` - The storage names this store contains

<a id="quixstreams.state.istatestorage.IStateStorage.get"></a>

#### get

```python
def get(key: str) -> Any
```

Gets the value at the specified key.

**Arguments**:

- `key` - The key to retrieve the value for.
  

**Returns**:

- `Any` - The value at the specified key, which can be one of the following types:
  str, int, float, bool, bytes, bytearray, or object (via pickle).

<a id="quixstreams.state.istatestorage.IStateStorage.set"></a>

#### set

```python
def set(key: str, value: Any)
```

Sets the value at the specified key.

**Arguments**:

- `key` - The key to set the value for.
- `value` - The value to be set, which can be one of the following types:
  StateValue, str, int, float, bool, bytes, bytearray, or object (via pickle).

<a id="quixstreams.state.istatestorage.IStateStorage.contains_key"></a>

#### contains\_key

```python
def contains_key(key: str) -> bool
```

Checks if the storage contains the specified key.

**Arguments**:

- `key` - The key to check for.
  

**Returns**:

- `bool` - True if the storage contains the key, False otherwise.

<a id="quixstreams.state.istatestorage.IStateStorage.get_all_keys"></a>

#### get\_all\_keys

```python
def get_all_keys()
```

Retrieves a set containing all the keys in the storage.

**Returns**:

- `set[str]` - A set of all keys in the storage.

<a id="quixstreams.state.istatestorage.IStateStorage.remove"></a>

#### remove

```python
def remove(key) -> None
```

Removes the specified key from the storage.

**Arguments**:

- `key` - The key to be removed.

<a id="quixstreams.state.istatestorage.IStateStorage.clear"></a>

#### clear

```python
def clear()
```

Clears the storage by removing all keys and their associated values.

<a id="quixstreams.state"></a>

# quixstreams.state

<a id="quixstreams.streamproducer"></a>

# quixstreams.streamproducer

<a id="quixstreams.streamproducer.StreamProducer"></a>

## StreamProducer Objects

```python
@nativedecorator
class StreamProducer(object)
```

Handles publishing stream to a topic

<a id="quixstreams.streamproducer.StreamProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(topic_producer: 'TopicProducer', net_pointer: ctypes.c_void_p)
```

Initializes a new instance of StreamProducer.

NOTE: Do not initialize this class manually, use TopicProducer.get_or_create_stream or create_stream

**Arguments**:

- `topic_producer` - The topic producer the stream producer publishes to.
- `net_pointer` - The .net object representing a StreamProducer.

<a id="quixstreams.streamproducer.StreamProducer.topic"></a>

#### topic

```python
@property
def topic() -> 'TopicProducer'
```

Gets the topic the stream is producing to.

**Returns**:

- `TopicProducer` - The topic the stream is producing to.

<a id="quixstreams.streamproducer.StreamProducer.on_write_exception"></a>

#### on\_write\_exception

```python
@property
def on_write_exception() -> Callable[['StreamProducer', BaseException], None]
```

Gets the handler for when a stream experiences exception during the asynchronous write process.

**Returns**:

  Callable[['StreamProducer', BaseException], None]: The handler for exceptions during the asynchronous write process.
  The first parameter is the stream is received for, second is the exception.

<a id="quixstreams.streamproducer.StreamProducer.on_write_exception"></a>

#### on\_write\_exception

```python
@on_write_exception.setter
def on_write_exception(
        value: Callable[['StreamProducer', BaseException], None]) -> None
```

Sets the handler for when a stream experiences exception during the asynchronous write process.

**Arguments**:

- `value` - The handler for exceptions during the asynchronous write process.
  The first parameter is the stream is received for, second is the exception.

<a id="quixstreams.streamproducer.StreamProducer.stream_id"></a>

#### stream\_id

```python
@property
def stream_id() -> str
```

Gets the unique id of the stream being produced.

**Returns**:

- `str` - The unique id of the stream being produced.

<a id="quixstreams.streamproducer.StreamProducer.epoch"></a>

#### epoch

```python
@property
def epoch() -> datetime
```

Gets the default Epoch used for Timeseries and Events.

**Returns**:

- `datetime` - The default Epoch used for Timeseries and Events.

<a id="quixstreams.streamproducer.StreamProducer.epoch"></a>

#### epoch

```python
@epoch.setter
def epoch(value: datetime)
```

Set the default Epoch used for Timeseries and Events.

**Arguments**:

- `value` - The default Epoch value to set.

<a id="quixstreams.streamproducer.StreamProducer.properties"></a>

#### properties

```python
@property
def properties() -> StreamPropertiesProducer
```

Gets the properties of the stream. The changes will automatically be sent after a slight delay.

**Returns**:

- `StreamPropertiesProducer` - The properties of the stream.

<a id="quixstreams.streamproducer.StreamProducer.timeseries"></a>

#### timeseries

```python
@property
def timeseries() -> StreamTimeseriesProducer
```

Gets the producer for publishing timeseries related information of the stream such as parameter definitions and values.

**Returns**:

- `StreamTimeseriesProducer` - The producer for publishing timeseries related information of the stream.

<a id="quixstreams.streamproducer.StreamProducer.events"></a>

#### events

```python
@property
def events() -> StreamEventsProducer
```

Gets the producer for publishing event related information of the stream such as event definitions and values.

**Returns**:

- `StreamEventsProducer` - The producer for publishing event related information of the stream.

<a id="quixstreams.streamproducer.StreamProducer.flush"></a>

#### flush

```python
def flush()
```

Flushes the pending data to stream.

<a id="quixstreams.streamproducer.StreamProducer.close"></a>

#### close

```python
def close(end_type: StreamEndType = StreamEndType.Closed)
```

Closes the stream and flushes the pending data to stream.

**Arguments**:

- `end_type` - The type of stream end. Defaults to StreamEndType.Closed.

<a id="quixstreams.builders.parameterdefinitionbuilder"></a>

# quixstreams.builders.parameterdefinitionbuilder

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder"></a>

## ParameterDefinitionBuilder Objects

```python
@nativedecorator
class ParameterDefinitionBuilder(object)
```

Builder for creating ParameterDefinition for StreamTimeseriesProducer.

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

Set the minimum and maximum range of the parameter.

**Arguments**:

- `minimum_value` - The minimum value.
- `maximum_value` - The maximum value.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_unit"></a>

#### set\_unit

```python
def set_unit(unit: str) -> 'ParameterDefinitionBuilder'
```

Set the unit of the parameter.

**Arguments**:

- `unit` - The unit of the parameter.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_format"></a>

#### set\_format

```python
def set_format(format: str) -> 'ParameterDefinitionBuilder'
```

Set the format of the parameter.

**Arguments**:

- `format` - The format of the parameter.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_custom_properties"></a>

#### set\_custom\_properties

```python
def set_custom_properties(
        custom_properties: str) -> 'ParameterDefinitionBuilder'
```

Set the custom properties of the parameter.

**Arguments**:

- `custom_properties` - The custom properties of the parameter.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.add_definition"></a>

#### add\_definition

```python
def add_definition(parameter_id: str,
                   name: str = None,
                   description: str = None) -> 'ParameterDefinitionBuilder'
```

Add new parameter definition to the StreamTimeseriesProducer. Configure it with the builder methods.

**Arguments**:

- `parameter_id` - The id of the parameter. Must match the parameter id used to send data.
- `name` - The human friendly display name of the parameter.
- `description` - The description of the parameter.
  

**Returns**:

  Parameter definition builder to define the parameter properties

<a id="quixstreams.builders.eventdatabuilder"></a>

# quixstreams.builders.eventdatabuilder

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder"></a>

## EventDataBuilder Objects

```python
@nativedecorator
class EventDataBuilder(object)
```

Builder for creating event data packages for StreamEventsProducer.

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

Adds new event at the time the builder is created for.

**Arguments**:

- `event_id` - The id of the event to set the value for.
- `value` - The string value.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, value: str) -> 'EventDataBuilder'
```

Sets tag value for the values.

**Arguments**:

- `tag_id` - The id of the tag.
- `value` - The value of the tag.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'EventDataBuilder'
```

Copies the tags from the specified dictionary. Conflicting tags will be overwritten.

**Arguments**:

- `tags` - The tags to add.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.eventdatabuilder.EventDataBuilder.publish"></a>

#### publish

```python
def publish()
```

Publishes the values to the StreamEventsProducer buffer.

See StreamEventsProducer buffer settings for more information on when the values are sent to the broker.

<a id="quixstreams.builders.timeseriesdatabuilder"></a>

# quixstreams.builders.timeseriesdatabuilder

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder"></a>

## TimeseriesDataBuilder Objects

```python
@nativedecorator
class TimeseriesDataBuilder(object)
```

Builder for managing TimeseriesDataTimestamp instances on TimeseriesBufferProducer.

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
    parameter_id: str, value: Union[numbers.Number, str, bytearray, bytes]
) -> 'TimeseriesDataBuilder'
```

Adds new parameter value at the time the builder is created for.

**Arguments**:

- `parameter_id` - The id of the parameter to set the value for.
- `value` - The value to add. Can be a number, string, bytearray, or bytes.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tag"></a>

#### add\_tag

```python
def add_tag(tag_id: str, value: str) -> 'TimeseriesDataBuilder'
```

Adds a tag to the values.

**Arguments**:

- `tag_id` - The id of the tag.
- `value` - The value of the tag.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tags"></a>

#### add\_tags

```python
def add_tags(tags: Dict[str, str]) -> 'TimeseriesDataBuilder'
```

Copies the tags from the specified dictionary. Conflicting tags will be overwritten.

**Arguments**:

- `tags` - The tags to add.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.publish"></a>

#### publish

```python
def publish()
```

Publish the values.

<a id="quixstreams.builders.eventdefinitionbuilder"></a>

# quixstreams.builders.eventdefinitionbuilder

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder"></a>

## EventDefinitionBuilder Objects

```python
@nativedecorator
class EventDefinitionBuilder(object)
```

Builder for creating EventDefinitions within StreamPropertiesProducer.

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

Set severity level of the Event.

**Arguments**:

- `level` - The severity level of the event.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_custom_properties"></a>

#### set\_custom\_properties

```python
def set_custom_properties(custom_properties: str) -> 'EventDefinitionBuilder'
```

Set custom properties of the Event.

**Arguments**:

- `custom_properties` - The custom properties of the event.
  

**Returns**:

  The builder.

<a id="quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.add_definition"></a>

#### add\_definition

```python
def add_definition(event_id: str,
                   name: str = None,
                   description: str = None) -> 'EventDefinitionBuilder'
```

Add new Event definition, to define properties like Name or Level, among others.

**Arguments**:

- `event_id` - Event id. This must match the event id you use to publish event values.
- `name` - Human friendly display name of the event.
- `description` - Description of the event.
  

**Returns**:

  Event definition builder to define the event properties.

<a id="quixstreams.builders"></a>

# quixstreams.builders

<a id="quixstreams.kafkastreamingclient"></a>

# quixstreams.kafkastreamingclient

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient"></a>

## KafkaStreamingClient Objects

```python
@nativedecorator
class KafkaStreamingClient(object)
```

A Kafka streaming client capable of creating topic consumer and producers.

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(broker_address: str,
             security_options: SecurityOptions = None,
             properties: Dict[str, str] = None,
             debug: bool = False)
```

Initializes a new instance of the KafkaStreamingClient.

**Arguments**:

- `broker_address` - The address of the Kafka cluster.
- `security_options` - Optional security options for the Kafka client.
- `properties` - Optional extra properties for broker configuration.
- `debug` - Whether debugging should be enabled. Defaults to False.

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

Gets a topic consumer capable of subscribing to receive incoming streams.

**Arguments**:

- `topic` - The name of the topic.
- `consumer_group` - The consumer group ID to use for consuming messages. Defaults to None.
- `commit_settings` - The settings to use for committing. If not provided, defaults to committing every 5000 messages or 5 seconds, whichever is sooner.
- `auto_offset_reset` - The offset to use when there is no saved offset for the consumer group. Defaults to AutoOffsetReset.Latest.
  

**Returns**:

- `TopicConsumer` - An instance of TopicConsumer for the specified topic.

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_producer"></a>

#### get\_topic\_producer

```python
def get_topic_producer(topic: str) -> TopicProducer
```

Gets a topic producer capable of publishing stream messages.

**Arguments**:

- `topic` - The name of the topic.
  

**Returns**:

- `TopicProducer` - An instance of TopicProducer for the specified topic.

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_consumer"></a>

#### get\_raw\_topic\_consumer

```python
def get_raw_topic_consumer(
    topic: str,
    consumer_group: str = None,
    auto_offset_reset: Union[AutoOffsetReset,
                             None] = None) -> RawTopicConsumer
```

Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages.

**Arguments**:

- `topic` - The name of the topic.
- `consumer_group` - The consumer group ID to use for consuming messages. Defaults to None.
- `auto_offset_reset` - The offset to use when there is no saved offset for the consumer group. Defaults to None.
  

**Returns**:

- `RawTopicConsumer` - An instance of RawTopicConsumer for the specified topic.

<a id="quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_producer"></a>

#### get\_raw\_topic\_producer

```python
def get_raw_topic_producer(topic: str) -> RawTopicProducer
```

Gets a topic producer capable of publishing non-quixstreams messages.

**Arguments**:

- `topic` - The name of the topic.
  

**Returns**:

- `RawTopicProducer` - An instance of RawTopicProducer for the specified topic.

<a id="quixstreams.exceptions.quixapiexception"></a>

# quixstreams.exceptions.quixapiexception

<a id="quixstreams.logging"></a>

# quixstreams.logging

