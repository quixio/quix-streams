# Table of Contents

* [quixstreams](#quixstreams)
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
    * [get\_net\_pointer](#quixstreams.streamconsumer.StreamConsumer.get_net_pointer)
* [quixstreams.logging](#quixstreams.logging)
* [quixstreams.configuration.securityoptions](#quixstreams.configuration.securityoptions)
  * [SecurityOptions](#quixstreams.configuration.securityoptions.SecurityOptions)
    * [\_\_init\_\_](#quixstreams.configuration.securityoptions.SecurityOptions.__init__)
    * [get\_net\_pointer](#quixstreams.configuration.securityoptions.SecurityOptions.get_net_pointer)
* [quixstreams.configuration.saslmechanism](#quixstreams.configuration.saslmechanism)
* [quixstreams.configuration](#quixstreams.configuration)
* [quixstreams.native.Python.SystemPrivateUri.System.Uri](#quixstreams.native.Python.SystemPrivateUri.System.Uri)
  * [Uri](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.__init__)
    * [Constructor](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor)
    * [Constructor2](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor2)
    * [Constructor3](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor3)
    * [Constructor6](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor6)
    * [Constructor7](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor7)
    * [get\_AbsolutePath](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_AbsolutePath)
    * [get\_AbsoluteUri](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_AbsoluteUri)
    * [get\_LocalPath](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_LocalPath)
    * [get\_Authority](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Authority)
    * [get\_IsDefaultPort](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsDefaultPort)
    * [get\_IsFile](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsFile)
    * [get\_IsLoopback](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsLoopback)
    * [get\_PathAndQuery](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_PathAndQuery)
    * [get\_Segments](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Segments)
    * [get\_IsUnc](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsUnc)
    * [get\_Host](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Host)
    * [get\_Port](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Port)
    * [get\_Query](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Query)
    * [get\_Fragment](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Fragment)
    * [get\_Scheme](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Scheme)
    * [get\_OriginalString](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_OriginalString)
    * [get\_DnsSafeHost](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_DnsSafeHost)
    * [get\_IdnHost](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IdnHost)
    * [get\_IsAbsoluteUri](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsAbsoluteUri)
    * [get\_UserEscaped](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UserEscaped)
    * [get\_UserInfo](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UserInfo)
    * [IsHexEncoding](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.IsHexEncoding)
    * [CheckSchemeName](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.CheckSchemeName)
    * [GetHashCode](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.GetHashCode)
    * [ToString](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.ToString)
    * [op\_Equality](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.op_Equality)
    * [op\_Inequality](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.op_Inequality)
    * [Equals](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Equals)
    * [MakeRelativeUri](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.MakeRelativeUri)
    * [MakeRelative](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.MakeRelative)
    * [TryCreate3](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.TryCreate3)
    * [TryCreate4](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.TryCreate4)
    * [IsWellFormedOriginalString](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.IsWellFormedOriginalString)
    * [UnescapeDataString](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.UnescapeDataString)
    * [EscapeUriString](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.EscapeUriString)
    * [EscapeDataString](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.EscapeDataString)
    * [IsBaseOf](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.IsBaseOf)
    * [get\_UriSchemeFile](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeFile)
    * [get\_UriSchemeFtp](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeFtp)
    * [get\_UriSchemeSftp](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeSftp)
    * [get\_UriSchemeFtps](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeFtps)
    * [get\_UriSchemeGopher](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeGopher)
    * [get\_UriSchemeHttp](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeHttp)
    * [get\_UriSchemeHttps](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeHttps)
    * [get\_UriSchemeWs](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeWs)
    * [get\_UriSchemeWss](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeWss)
    * [get\_UriSchemeMailto](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeMailto)
    * [get\_UriSchemeNews](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNews)
    * [get\_UriSchemeNntp](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNntp)
    * [get\_UriSchemeSsh](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeSsh)
    * [get\_UriSchemeTelnet](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeTelnet)
    * [get\_UriSchemeNetTcp](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNetTcp)
    * [get\_UriSchemeNetPipe](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNetPipe)
    * [get\_SchemeDelimiter](#quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_SchemeDelimiter)
* [quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline)
  * [IStreamPipeline](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.__init__)
    * [get\_StreamId](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.get_StreamId)
    * [get\_SourceMetadata](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.get_SourceMetadata)
    * [set\_SourceMetadata](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.set_SourceMetadata)
    * [Subscribe3](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.Subscribe3)
    * [Close](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.Close)
    * [add\_OnClosing](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.add_OnClosing)
    * [remove\_OnClosing](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.remove_OnClosing)
    * [add\_OnClosed](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.add_OnClosed)
    * [remove\_OnClosed](#quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.remove_OnClosed)
* [quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamEndType](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamEndType)
* [quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage)
  * [StreamPackage](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.Constructor)
    * [get\_Type](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.get_Type)
    * [set\_Type](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.set_Type)
    * [get\_TransportContext](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.get_TransportContext)
    * [set\_TransportContext](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.set_TransportContext)
    * [get\_Value](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.get_Value)
    * [set\_Value](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.set_Value)
    * [ToJson](#quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.ToJson)
* [quixstreams.native.Python.QuixStreamsTelemetry.Models.EventLevel](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventLevel)
* [quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition)
  * [EventDefinition](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.Constructor)
    * [get\_Id](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Id)
    * [set\_Id](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Id)
    * [get\_Name](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Name)
    * [set\_Name](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Name)
    * [get\_Description](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Description)
    * [set\_Description](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Description)
    * [get\_CustomProperties](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_CustomProperties)
    * [set\_CustomProperties](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_CustomProperties)
    * [get\_Level](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Level)
    * [set\_Level](#quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Level)
* [quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw)
  * [TimeseriesDataRaw](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.Constructor2)
    * [ToJson](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.ToJson)
    * [get\_Epoch](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_Epoch)
    * [set\_Epoch](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_Epoch)
    * [get\_Timestamps](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_Timestamps)
    * [set\_Timestamps](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_Timestamps)
    * [get\_NumericValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_NumericValues)
    * [set\_NumericValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_NumericValues)
    * [get\_StringValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_StringValues)
    * [set\_StringValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_StringValues)
    * [get\_BinaryValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_BinaryValues)
    * [set\_BinaryValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_BinaryValues)
    * [get\_TagValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_TagValues)
    * [set\_TagValues](#quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_TagValues)
* [quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition)
  * [ParameterDefinition](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.Constructor)
    * [get\_Id](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Id)
    * [set\_Id](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Id)
    * [get\_Name](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Name)
    * [set\_Name](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Name)
    * [get\_Description](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Description)
    * [set\_Description](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Description)
    * [get\_MinimumValue](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_MinimumValue)
    * [set\_MinimumValue](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_MinimumValue)
    * [get\_MaximumValue](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_MaximumValue)
    * [set\_MaximumValue](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_MaximumValue)
    * [get\_Unit](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Unit)
    * [set\_Unit](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Unit)
    * [get\_Format](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Format)
    * [set\_Format](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Format)
    * [get\_CustomProperties](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_CustomProperties)
    * [set\_CustomProperties](#quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_CustomProperties)
* [quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration)
  * [KafkaProducerConfiguration](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.Constructor)
    * [get\_BrokerList](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_BrokerList)
    * [get\_MaxMessageSize](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_MaxMessageSize)
    * [get\_MaxKeySize](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_MaxKeySize)
    * [get\_Properties](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_Properties)
* [quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer)
  * [TelemetryKafkaConsumer](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Constructor)
    * [add\_OnReceiveException](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnReceiveException)
    * [remove\_OnReceiveException](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnReceiveException)
    * [add\_OnStreamsRevoked](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnStreamsRevoked)
    * [remove\_OnStreamsRevoked](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnStreamsRevoked)
    * [add\_OnRevoking](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnRevoking)
    * [remove\_OnRevoking](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnRevoking)
    * [add\_OnCommitted](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnCommitted)
    * [remove\_OnCommitted](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnCommitted)
    * [add\_OnCommitting](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnCommitting)
    * [remove\_OnCommitting](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnCommitting)
    * [Start](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Start)
    * [ForEach](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.ForEach)
    * [Stop](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Stop)
    * [Dispose](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Dispose)
    * [Commit](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Commit)
* [quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration)
  * [TelemetryKafkaConsumerConfiguration](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.Constructor)
    * [get\_BrokerList](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_BrokerList)
    * [get\_ConsumerGroupId](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_ConsumerGroupId)
    * [get\_CommitOptions](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_CommitOptions)
    * [set\_CommitOptions](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.set_CommitOptions)
    * [get\_Properties](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_Properties)
* [quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer)
  * [TelemetryKafkaProducer](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.Constructor)
    * [get\_StreamId](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.get_StreamId)
    * [add\_OnWriteException](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.add_OnWriteException)
    * [remove\_OnWriteException](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.remove_OnWriteException)
    * [Dispose](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.Dispose)
    * [get\_OnStreamPipelineAssigned](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.get_OnStreamPipelineAssigned)
    * [set\_OnStreamPipelineAssigned](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.set_OnStreamPipelineAssigned)
* [quixstreams.native.Python.QuixStreamsTelemetry.Kafka.AutoOffsetReset](#quixstreams.native.Python.QuixStreamsTelemetry.Kafka.AutoOffsetReset)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.Type](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type)
  * [Type](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.__init__)
    * [get\_IsInterface](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsInterface)
    * [GetType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType)
    * [GetType2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType2)
    * [GetType3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType3)
    * [GetType7](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType7)
    * [get\_Namespace](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_Namespace)
    * [get\_AssemblyQualifiedName](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_AssemblyQualifiedName)
    * [get\_FullName](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_FullName)
    * [get\_IsNested](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNested)
    * [get\_DeclaringType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_DeclaringType)
    * [get\_ReflectedType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_ReflectedType)
    * [get\_UnderlyingSystemType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_UnderlyingSystemType)
    * [get\_IsTypeDefinition](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsTypeDefinition)
    * [get\_IsArray](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsArray)
    * [get\_IsByRef](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsByRef)
    * [get\_IsPointer](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsPointer)
    * [get\_IsConstructedGenericType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsConstructedGenericType)
    * [get\_IsGenericParameter](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericParameter)
    * [get\_IsGenericTypeParameter](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericTypeParameter)
    * [get\_IsGenericMethodParameter](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericMethodParameter)
    * [get\_IsGenericType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericType)
    * [get\_IsGenericTypeDefinition](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericTypeDefinition)
    * [get\_IsSZArray](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSZArray)
    * [get\_IsVariableBoundArray](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsVariableBoundArray)
    * [get\_IsByRefLike](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsByRefLike)
    * [get\_IsFunctionPointer](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsFunctionPointer)
    * [get\_IsUnmanagedFunctionPointer](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsUnmanagedFunctionPointer)
    * [get\_HasElementType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_HasElementType)
    * [GetElementType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetElementType)
    * [GetArrayRank](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetArrayRank)
    * [GetGenericTypeDefinition](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetGenericTypeDefinition)
    * [get\_GenericTypeArguments](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_GenericTypeArguments)
    * [GetGenericArguments](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetGenericArguments)
    * [GetOptionalCustomModifiers](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetOptionalCustomModifiers)
    * [GetRequiredCustomModifiers](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetRequiredCustomModifiers)
    * [get\_GenericParameterPosition](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_GenericParameterPosition)
    * [GetGenericParameterConstraints](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetGenericParameterConstraints)
    * [get\_IsAbstract](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAbstract)
    * [get\_IsImport](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsImport)
    * [get\_IsSealed](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSealed)
    * [get\_IsSpecialName](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSpecialName)
    * [get\_IsClass](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsClass)
    * [get\_IsNestedAssembly](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedAssembly)
    * [get\_IsNestedFamANDAssem](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedFamANDAssem)
    * [get\_IsNestedFamily](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedFamily)
    * [get\_IsNestedFamORAssem](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedFamORAssem)
    * [get\_IsNestedPrivate](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedPrivate)
    * [get\_IsNestedPublic](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedPublic)
    * [get\_IsNotPublic](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNotPublic)
    * [get\_IsPublic](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsPublic)
    * [get\_IsAutoLayout](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAutoLayout)
    * [get\_IsExplicitLayout](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsExplicitLayout)
    * [get\_IsLayoutSequential](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsLayoutSequential)
    * [get\_IsAnsiClass](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAnsiClass)
    * [get\_IsAutoClass](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAutoClass)
    * [get\_IsUnicodeClass](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsUnicodeClass)
    * [get\_IsCOMObject](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsCOMObject)
    * [get\_IsContextful](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsContextful)
    * [get\_IsEnum](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsEnum)
    * [get\_IsMarshalByRef](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsMarshalByRef)
    * [get\_IsPrimitive](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsPrimitive)
    * [get\_IsValueType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsValueType)
    * [IsAssignableTo](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsAssignableTo)
    * [get\_IsSignatureType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSignatureType)
    * [get\_IsSecurityCritical](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSecurityCritical)
    * [get\_IsSecuritySafeCritical](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSecuritySafeCritical)
    * [get\_IsSecurityTransparent](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSecurityTransparent)
    * [GetFunctionPointerCallingConventions](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetFunctionPointerCallingConventions)
    * [GetFunctionPointerReturnType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetFunctionPointerReturnType)
    * [GetFunctionPointerParameterTypes](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetFunctionPointerParameterTypes)
    * [GetNestedType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetNestedType)
    * [GetNestedTypes](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetNestedTypes)
    * [GetTypeArray](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeArray)
    * [GetTypeCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeCode)
    * [GetTypeFromProgID](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID)
    * [GetTypeFromProgID2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID2)
    * [GetTypeFromProgID3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID3)
    * [GetTypeFromProgID4](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID4)
    * [get\_BaseType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_BaseType)
    * [GetInterface](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetInterface)
    * [GetInterface2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetInterface2)
    * [GetInterfaces](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetInterfaces)
    * [IsInstanceOfType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsInstanceOfType)
    * [IsEquivalentTo](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsEquivalentTo)
    * [GetEnumUnderlyingType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumUnderlyingType)
    * [GetEnumValues](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumValues)
    * [GetEnumValuesAsUnderlyingType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumValuesAsUnderlyingType)
    * [MakeArrayType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeArrayType)
    * [MakeArrayType2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeArrayType2)
    * [MakeByRefType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeByRefType)
    * [MakeGenericType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeGenericType)
    * [MakePointerType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakePointerType)
    * [MakeGenericSignatureType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeGenericSignatureType)
    * [MakeGenericMethodParameter](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeGenericMethodParameter)
    * [ToString](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.ToString)
    * [Equals](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.Equals)
    * [GetHashCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetHashCode)
    * [Equals2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.Equals2)
    * [op\_Equality](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.op_Equality)
    * [op\_Inequality](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.op_Inequality)
    * [ReflectionOnlyGetType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.ReflectionOnlyGetType)
    * [IsEnumDefined](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsEnumDefined)
    * [GetEnumName](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumName)
    * [GetEnumNames](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumNames)
    * [get\_IsSerializable](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSerializable)
    * [get\_ContainsGenericParameters](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_ContainsGenericParameters)
    * [get\_IsVisible](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsVisible)
    * [IsSubclassOf](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsSubclassOf)
    * [IsAssignableFrom](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsAssignableFrom)
    * [get\_Name](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_Name)
    * [IsDefined](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsDefined)
    * [GetCustomAttributes](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetCustomAttributes)
    * [GetCustomAttributes2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetCustomAttributes2)
    * [get\_IsCollectible](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsCollectible)
    * [get\_MetadataToken](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_MetadataToken)
    * [get\_EmptyTypes](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_EmptyTypes)
    * [get\_Missing](#quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_Missing)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan)
  * [TimeSpan](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.__init__)
    * [Constructor](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor)
    * [Constructor2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor2)
    * [Constructor3](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor3)
    * [Constructor4](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor4)
    * [Constructor5](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor5)
    * [get\_Ticks](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Ticks)
    * [get\_Days](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Days)
    * [get\_Hours](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Hours)
    * [get\_Milliseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Milliseconds)
    * [get\_Microseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Microseconds)
    * [get\_Nanoseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Nanoseconds)
    * [get\_Minutes](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Minutes)
    * [get\_Seconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Seconds)
    * [get\_TotalDays](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalDays)
    * [get\_TotalHours](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalHours)
    * [get\_TotalMilliseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalMilliseconds)
    * [get\_TotalMicroseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalMicroseconds)
    * [get\_TotalNanoseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalNanoseconds)
    * [get\_TotalMinutes](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalMinutes)
    * [get\_TotalSeconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalSeconds)
    * [Add](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Add)
    * [Compare](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Compare)
    * [CompareTo](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.CompareTo)
    * [CompareTo2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.CompareTo2)
    * [FromDays](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromDays)
    * [Duration](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Duration)
    * [Equals](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Equals)
    * [Equals2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Equals2)
    * [Equals3](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Equals3)
    * [GetHashCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.GetHashCode)
    * [FromHours](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromHours)
    * [FromMilliseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromMilliseconds)
    * [FromMicroseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromMicroseconds)
    * [FromMinutes](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromMinutes)
    * [Negate](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Negate)
    * [FromSeconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromSeconds)
    * [Subtract](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Subtract)
    * [Multiply](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Multiply)
    * [Divide](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Divide)
    * [Divide2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Divide2)
    * [FromTicks](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromTicks)
    * [Parse](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Parse)
    * [Parse2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Parse2)
    * [ParseExact](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ParseExact)
    * [ParseExact2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ParseExact2)
    * [TryParse](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParse)
    * [TryParse3](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParse3)
    * [TryParseExact](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParseExact)
    * [TryParseExact3](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParseExact3)
    * [ToString](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ToString)
    * [ToString2](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ToString2)
    * [ToString3](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ToString3)
    * [op\_Equality](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.op_Equality)
    * [op\_Inequality](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.op_Inequality)
    * [get\_Zero](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Zero)
    * [get\_MaxValue](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_MaxValue)
    * [get\_MinValue](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_MinValue)
    * [get\_NanosecondsPerTick](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_NanosecondsPerTick)
    * [get\_TicksPerMicrosecond](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerMicrosecond)
    * [get\_TicksPerMillisecond](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerMillisecond)
    * [get\_TicksPerSecond](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerSecond)
    * [get\_TicksPerMinute](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerMinute)
    * [get\_TicksPerHour](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerHour)
    * [get\_TicksPerDay](#quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerDay)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider](#quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider)
  * [IFormatProvider](#quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider.__init__)
    * [GetFormat](#quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider.GetFormat)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.TypeCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.TypeCode)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.Enum](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum)
  * [Enum](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.__init__)
    * [GetName2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetName2)
    * [GetNames](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetNames)
    * [GetUnderlyingType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetUnderlyingType)
    * [GetValues](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetValues)
    * [GetValuesAsUnderlyingType](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetValuesAsUnderlyingType)
    * [HasFlag](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.HasFlag)
    * [IsDefined2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.IsDefined2)
    * [Parse](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Parse)
    * [Parse3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Parse3)
    * [TryParse](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.TryParse)
    * [TryParse3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.TryParse3)
    * [Equals](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Equals)
    * [GetHashCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetHashCode)
    * [CompareTo](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.CompareTo)
    * [ToString](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString)
    * [ToString2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString2)
    * [ToString3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString3)
    * [ToString4](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString4)
    * [Format](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Format)
    * [GetTypeCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetTypeCode)
    * [ToObject](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject)
    * [ToObject4](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject4)
    * [ToObject5](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject5)
    * [ToObject8](#quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject8)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime)
  * [DateTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.__init__)
    * [Constructor](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor)
    * [Constructor5](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor5)
    * [Constructor8](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor8)
    * [Constructor11](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor11)
    * [Constructor14](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor14)
    * [Add](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Add)
    * [AddDays](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddDays)
    * [AddHours](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddHours)
    * [AddMilliseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMilliseconds)
    * [AddMicroseconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMicroseconds)
    * [AddMinutes](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMinutes)
    * [AddMonths](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMonths)
    * [AddSeconds](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddSeconds)
    * [AddTicks](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddTicks)
    * [AddYears](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddYears)
    * [Compare](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Compare)
    * [CompareTo](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.CompareTo)
    * [CompareTo2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.CompareTo2)
    * [DaysInMonth](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.DaysInMonth)
    * [Equals](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Equals)
    * [Equals2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Equals2)
    * [Equals3](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Equals3)
    * [FromBinary](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromBinary)
    * [FromFileTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromFileTime)
    * [FromFileTimeUtc](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromFileTimeUtc)
    * [FromOADate](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromOADate)
    * [IsDaylightSavingTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.IsDaylightSavingTime)
    * [ToBinary](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToBinary)
    * [get\_Date](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Date)
    * [get\_Day](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Day)
    * [get\_DayOfYear](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_DayOfYear)
    * [GetHashCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetHashCode)
    * [get\_Hour](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Hour)
    * [get\_Millisecond](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Millisecond)
    * [get\_Microsecond](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Microsecond)
    * [get\_Nanosecond](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Nanosecond)
    * [get\_Minute](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Minute)
    * [get\_Month](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Month)
    * [get\_Now](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Now)
    * [get\_Second](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Second)
    * [get\_Ticks](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Ticks)
    * [get\_TimeOfDay](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_TimeOfDay)
    * [get\_Today](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Today)
    * [get\_Year](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Year)
    * [IsLeapYear](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.IsLeapYear)
    * [Parse](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Parse)
    * [Parse2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Parse2)
    * [ParseExact](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ParseExact)
    * [Subtract](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Subtract)
    * [Subtract2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Subtract2)
    * [ToOADate](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToOADate)
    * [ToFileTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToFileTime)
    * [ToFileTimeUtc](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToFileTimeUtc)
    * [ToLocalTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToLocalTime)
    * [ToLongDateString](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToLongDateString)
    * [ToLongTimeString](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToLongTimeString)
    * [ToShortDateString](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToShortDateString)
    * [ToShortTimeString](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToShortTimeString)
    * [ToString](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString)
    * [ToString2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString2)
    * [ToString3](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString3)
    * [ToString4](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString4)
    * [ToUniversalTime](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToUniversalTime)
    * [TryParse](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.TryParse)
    * [op\_Equality](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.op_Equality)
    * [op\_Inequality](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.op_Inequality)
    * [Deconstruct2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Deconstruct2)
    * [GetDateTimeFormats](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetDateTimeFormats)
    * [GetDateTimeFormats2](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetDateTimeFormats2)
    * [GetTypeCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetTypeCode)
    * [TryParse5](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.TryParse5)
    * [get\_UtcNow](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_UtcNow)
    * [get\_MinValue](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_MinValue)
    * [get\_MaxValue](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_MaxValue)
    * [get\_UnixEpoch](#quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_UnixEpoch)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken)
  * [CancellationToken](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.__init__)
    * [Constructor](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.Constructor)
    * [get\_None](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.get_None)
    * [get\_IsCancellationRequested](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.get_IsCancellationRequested)
    * [get\_CanBeCanceled](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.get_CanBeCanceled)
    * [Equals](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.Equals)
    * [Equals2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.Equals2)
    * [GetHashCode](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.GetHashCode)
    * [op\_Equality](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.op_Equality)
    * [op\_Inequality](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.op_Inequality)
    * [ThrowIfCancellationRequested](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.ThrowIfCancellationRequested)
    * [ToString](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.ToString)
* [quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource)
  * [CancellationTokenSource](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.__init__)
    * [Constructor](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Constructor)
    * [Constructor2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Constructor2)
    * [Constructor3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Constructor3)
    * [get\_IsCancellationRequested](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.get_IsCancellationRequested)
    * [get\_Token](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.get_Token)
    * [Cancel](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Cancel)
    * [Cancel2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Cancel2)
    * [CancelAfter](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CancelAfter)
    * [CancelAfter2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CancelAfter2)
    * [TryReset](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.TryReset)
    * [Dispose](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Dispose)
    * [CreateLinkedTokenSource](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CreateLinkedTokenSource)
    * [CreateLinkedTokenSource2](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CreateLinkedTokenSource2)
    * [CreateLinkedTokenSource3](#quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CreateLinkedTokenSource3)
* [quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions)
  * [SecurityOptions](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.Constructor2)
    * [get\_SaslMechanism](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_SaslMechanism)
    * [set\_SaslMechanism](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_SaslMechanism)
    * [get\_Username](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_Username)
    * [set\_Username](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_Username)
    * [get\_Password](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_Password)
    * [set\_Password](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_Password)
    * [get\_SslCertificates](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_SslCertificates)
    * [set\_SslCertificates](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_SslCertificates)
    * [get\_UseSsl](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_UseSsl)
    * [set\_UseSsl](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_UseSsl)
    * [get\_UseSasl](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_UseSasl)
    * [set\_UseSasl](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_UseSasl)
* [quixstreams.native.Python.QuixStreamsStreaming.Configuration.SaslMechanism](#quixstreams.native.Python.QuixStreamsStreaming.Configuration.SaslMechanism)
* [quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions)
  * [QuixStreamingClientExtensions](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions.QuixStreamingClientExtensions)
    * [GetTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions.QuixStreamingClientExtensions.GetTopicConsumer)
* [quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs)
  * [PackageReceivedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.Constructor)
    * [get\_TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.get_TopicConsumer)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.get_Stream)
    * [get\_Package](#quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.get_Package)
* [quixstreams.native.Python.QuixStreamsStreaming.TopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer)
  * [TopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.Constructor2)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.remove_OnDisposed)
    * [CreateStream](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.CreateStream)
    * [CreateStream2](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.CreateStream2)
    * [GetStream](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.GetStream)
    * [GetOrCreateStream](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.GetOrCreateStream)
    * [RemoveStream](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.RemoveStream)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer)
  * [IStreamConsumer](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.__init__)
    * [get\_StreamId](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_StreamId)
    * [get\_Properties](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_Properties)
    * [get\_Timeseries](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_Timeseries)
    * [get\_Events](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_Events)
    * [add\_OnPackageReceived](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.add_OnPackageReceived)
    * [remove\_OnPackageReceived](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.remove_OnPackageReceived)
    * [add\_OnStreamClosed](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.add_OnStreamClosed)
    * [remove\_OnStreamClosed](#quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.remove_OnStreamClosed)
* [quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs)
  * [StreamClosedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.Constructor)
    * [get\_TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.get_TopicConsumer)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.get_Stream)
    * [get\_EndType](#quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.get_EndType)
* [quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags)
  * [TimeseriesDataTimestampTags](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.__init__)
    * [GetEnumerator](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.GetEnumerator)
    * [get\_Item](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Item)
    * [get\_Keys](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Keys)
    * [get\_Values](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Values)
    * [get\_Count](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Count)
    * [ContainsKey](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.ContainsKey)
    * [TryGetValue](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.TryGetValue)
    * [Equals](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.Equals)
    * [GetHashCode](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.GetHashCode)
    * [ToString](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.ToString)
* [quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils](#quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils)
  * [QuixUtils](#quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils.QuixUtils)
    * [TryGetWorkspaceIdPrefix](#quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils.QuixUtils.TryGetWorkspaceIdPrefix)
* [quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps)
  * [TimeseriesDataTimestamps](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.__init__)
    * [GetEnumerator](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.GetEnumerator)
    * [get\_Item](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.get_Item)
    * [get\_Count](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.get_Count)
    * [Equals](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.Equals)
    * [GetHashCode](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.GetHashCode)
    * [ToString](#quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.ToString)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder)
  * [EventDefinitionBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.Constructor)
    * [SetLevel](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.SetLevel)
    * [SetCustomProperties](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.SetCustomProperties)
    * [AddDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.AddDefinition)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer)
  * [StreamEventsProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.__init__)
    * [get\_DefaultTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.get_DefaultTags)
    * [set\_DefaultTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.set_DefaultTags)
    * [get\_DefaultLocation](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.get_DefaultLocation)
    * [set\_DefaultLocation](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.set_DefaultLocation)
    * [get\_Epoch](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.get_Epoch)
    * [set\_Epoch](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.set_Epoch)
    * [AddTimestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestamp)
    * [AddTimestamp2](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestamp2)
    * [AddTimestampMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestampMilliseconds)
    * [AddTimestampNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestampNanoseconds)
    * [AddDefinitions](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddDefinitions)
    * [AddDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddDefinition)
    * [AddLocation](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddLocation)
    * [Flush](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Flush)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Publish)
    * [Publish2](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Publish2)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder)
  * [TimeseriesDataBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.Constructor)
    * [AddValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddValue)
    * [AddValue2](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddValue2)
    * [AddValue3](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddValue3)
    * [AddTag](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddTag)
    * [AddTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddTags)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.Publish)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer)
  * [StreamTimeseriesProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.__init__)
    * [get\_Buffer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.get_Buffer)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Publish)
    * [Publish2](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Publish2)
    * [get\_DefaultLocation](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.get_DefaultLocation)
    * [set\_DefaultLocation](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.set_DefaultLocation)
    * [AddDefinitions](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.AddDefinitions)
    * [AddDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.AddDefinition)
    * [AddLocation](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.AddLocation)
    * [Flush](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Flush)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer)
  * [StreamPropertiesProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.__init__)
    * [get\_FlushInterval](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_FlushInterval)
    * [set\_FlushInterval](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_FlushInterval)
    * [get\_Name](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Name)
    * [set\_Name](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_Name)
    * [get\_Location](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Location)
    * [set\_Location](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_Location)
    * [get\_TimeOfRecording](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_TimeOfRecording)
    * [set\_TimeOfRecording](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_TimeOfRecording)
    * [get\_Metadata](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Metadata)
    * [get\_Parents](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Parents)
    * [AddParent](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.AddParent)
    * [RemoveParent](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.RemoveParent)
    * [Flush](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.Flush)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder)
  * [EventDataBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.Constructor)
    * [AddValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.AddValue)
    * [AddTag](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.AddTag)
    * [AddTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.AddTags)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.Publish)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder)
  * [ParameterDefinitionBuilder](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.Constructor)
    * [SetRange](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetRange)
    * [SetUnit](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetUnit)
    * [SetFormat](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetFormat)
    * [SetCustomProperties](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetCustomProperties)
    * [AddDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.AddDefinition)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer)
  * [TimeseriesBufferProducer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.__init__)
    * [get\_Epoch](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.get_Epoch)
    * [set\_Epoch](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.set_Epoch)
    * [AddTimestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestamp)
    * [AddTimestamp2](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestamp2)
    * [AddTimestampMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestampMilliseconds)
    * [AddTimestampNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestampNanoseconds)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.Publish)
    * [get\_DefaultTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.get_DefaultTags)
    * [set\_DefaultTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.set_DefaultTags)
    * [Flush](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.Flush)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData)
  * [TimeseriesData](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Constructor2)
    * [Constructor3](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Constructor3)
    * [Clone](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Clone)
    * [get\_Timestamps](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.get_Timestamps)
    * [AddTimestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestamp)
    * [AddTimestamp2](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestamp2)
    * [AddTimestampMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestampMilliseconds)
    * [AddTimestampNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestampNanoseconds)
    * [Equals](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Equals)
    * [GetHashCode](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.GetHashCode)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue)
  * [ParameterValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.__init__)
    * [get\_ParameterId](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_ParameterId)
    * [get\_NumericValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_NumericValue)
    * [set\_NumericValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.set_NumericValue)
    * [get\_StringValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_StringValue)
    * [set\_StringValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.set_StringValue)
    * [get\_BinaryValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_BinaryValue)
    * [set\_BinaryValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.set_BinaryValue)
    * [get\_Value](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_Value)
    * [op\_Equality](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.op_Equality)
    * [op\_Inequality](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.op_Inequality)
    * [Equals](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.Equals)
    * [GetHashCode](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.GetHashCode)
    * [ToString](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.ToString)
    * [get\_Type](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_Type)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.EventData](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData)
  * [EventData](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Constructor2)
    * [Constructor3](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Constructor3)
    * [Clone](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Clone)
    * [get\_Id](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Id)
    * [set\_Id](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.set_Id)
    * [get\_Value](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Value)
    * [set\_Value](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.set_Value)
    * [get\_Tags](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Tags)
    * [AddTag](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.AddTag)
    * [AddTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.AddTags)
    * [RemoveTag](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.RemoveTag)
    * [get\_TimestampNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_TimestampNanoseconds)
    * [get\_TimestampMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_TimestampMilliseconds)
    * [get\_Timestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Timestamp)
    * [get\_TimestampAsTimeSpan](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_TimestampAsTimeSpan)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValueType](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValueType)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.CommitMode](#quixstreams.native.Python.QuixStreamsStreaming.Models.CommitMode)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration)
  * [TimeseriesBufferConfiguration](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.Constructor)
    * [get\_PacketSize](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_PacketSize)
    * [set\_PacketSize](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_PacketSize)
    * [get\_TimeSpanInNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_TimeSpanInNanoseconds)
    * [set\_TimeSpanInNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_TimeSpanInNanoseconds)
    * [get\_TimeSpanInMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_TimeSpanInMilliseconds)
    * [set\_TimeSpanInMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_TimeSpanInMilliseconds)
    * [get\_CustomTriggerBeforeEnqueue](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_CustomTriggerBeforeEnqueue)
    * [set\_CustomTriggerBeforeEnqueue](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_CustomTriggerBeforeEnqueue)
    * [get\_CustomTrigger](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_CustomTrigger)
    * [set\_CustomTrigger](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_CustomTrigger)
    * [get\_Filter](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_Filter)
    * [set\_Filter](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_Filter)
    * [get\_BufferTimeout](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_BufferTimeout)
    * [set\_BufferTimeout](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_BufferTimeout)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition)
  * [EventDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.Constructor)
    * [get\_Id](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Id)
    * [get\_Name](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Name)
    * [get\_Description](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Description)
    * [get\_Location](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Location)
    * [get\_CustomProperties](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_CustomProperties)
    * [get\_Level](#quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Level)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer)
  * [TimeseriesBufferConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer.__init__)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer)
  * [StreamPropertiesConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.__init__)
    * [add\_OnChanged](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.add_OnChanged)
    * [remove\_OnChanged](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.remove_OnChanged)
    * [get\_Name](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Name)
    * [get\_Location](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Location)
    * [get\_TimeOfRecording](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_TimeOfRecording)
    * [get\_Metadata](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Metadata)
    * [get\_Parents](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Parents)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer)
  * [StreamTimeseriesConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.__init__)
    * [CreateBuffer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.CreateBuffer)
    * [CreateBuffer2](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.CreateBuffer2)
    * [add\_OnDefinitionsChanged](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.add_OnDefinitionsChanged)
    * [remove\_OnDefinitionsChanged](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.remove_OnDefinitionsChanged)
    * [add\_OnDataReceived](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.add_OnDataReceived)
    * [remove\_OnDataReceived](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.remove_OnDataReceived)
    * [add\_OnRawReceived](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.add_OnRawReceived)
    * [remove\_OnRawReceived](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.remove_OnRawReceived)
    * [get\_Definitions](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.get_Definitions)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs)
  * [EventDataReadEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.Constructor)
    * [get\_TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.get_TopicConsumer)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.get_Stream)
    * [get\_Data](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.get_Data)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs)
  * [EventDefinitionsChangedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.Constructor)
    * [get\_TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.get_TopicConsumer)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.get_Stream)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs)
  * [ParameterDefinitionsChangedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.Constructor)
    * [get\_TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.get_TopicConsumer)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.get_Stream)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer)
  * [StreamEventsConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.__init__)
    * [add\_OnDataReceived](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.add_OnDataReceived)
    * [remove\_OnDataReceived](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.remove_OnDataReceived)
    * [add\_OnDefinitionsChanged](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.add_OnDefinitionsChanged)
    * [remove\_OnDefinitionsChanged](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.remove_OnDefinitionsChanged)
    * [get\_Definitions](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.get_Definitions)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs)
  * [StreamPropertiesChangedEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.Constructor)
    * [get\_TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.get_TopicConsumer)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.get_Stream)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs)
  * [TimeseriesDataReadEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.Constructor)
    * [get\_Topic](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.get_Topic)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.get_Stream)
    * [get\_Data](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.get_Data)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs)
  * [TimeseriesDataRawReadEventArgs](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.Constructor)
    * [get\_Topic](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.get_Topic)
    * [get\_Stream](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.get_Stream)
    * [get\_Data](#quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.get_Data)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer)
  * [TimeseriesBuffer](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.__init__)
    * [add\_OnDataReleased](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.add_OnDataReleased)
    * [remove\_OnDataReleased](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.remove_OnDataReleased)
    * [add\_OnRawReleased](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.add_OnRawReleased)
    * [remove\_OnRawReleased](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.remove_OnRawReleased)
    * [get\_PacketSize](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_PacketSize)
    * [set\_PacketSize](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_PacketSize)
    * [get\_BufferTimeout](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_BufferTimeout)
    * [set\_BufferTimeout](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_BufferTimeout)
    * [get\_TimeSpanInNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_TimeSpanInNanoseconds)
    * [set\_TimeSpanInNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_TimeSpanInNanoseconds)
    * [get\_TimeSpanInMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_TimeSpanInMilliseconds)
    * [set\_TimeSpanInMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_TimeSpanInMilliseconds)
    * [get\_Filter](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_Filter)
    * [set\_Filter](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_Filter)
    * [get\_CustomTriggerBeforeEnqueue](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_CustomTriggerBeforeEnqueue)
    * [set\_CustomTriggerBeforeEnqueue](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_CustomTriggerBeforeEnqueue)
    * [get\_CustomTrigger](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_CustomTrigger)
    * [set\_CustomTrigger](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_CustomTrigger)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition)
  * [ParameterDefinition](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.Constructor)
    * [get\_Id](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Id)
    * [get\_Name](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Name)
    * [get\_Description](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Description)
    * [get\_Location](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Location)
    * [get\_MinimumValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_MinimumValue)
    * [get\_MaximumValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_MaximumValue)
    * [get\_Unit](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Unit)
    * [get\_Format](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Format)
    * [get\_CustomProperties](#quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_CustomProperties)
* [quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp)
  * [TimeseriesDataTimestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.__init__)
    * [get\_Parameters](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_Parameters)
    * [get\_Tags](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_Tags)
    * [get\_TimestampNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_TimestampNanoseconds)
    * [set\_TimestampNanoseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.set_TimestampNanoseconds)
    * [get\_TimestampMilliseconds](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_TimestampMilliseconds)
    * [get\_Timestamp](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_Timestamp)
    * [get\_TimestampAsTimeSpan](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_TimestampAsTimeSpan)
    * [AddValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue)
    * [AddValue2](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue2)
    * [AddValue3](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue3)
    * [AddValue4](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue4)
    * [RemoveValue](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.RemoveValue)
    * [AddTag](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddTag)
    * [AddTags](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddTags)
    * [RemoveTag](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.RemoveTag)
    * [Equals](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.Equals)
    * [GetHashCode](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.GetHashCode)
    * [ToString](#quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.ToString)
* [quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer)
  * [ITopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.__init__)
    * [CreateStream](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.CreateStream)
    * [CreateStream2](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.CreateStream2)
    * [GetStream](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.GetStream)
    * [GetOrCreateStream](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.GetOrCreateStream)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.remove_OnDisposed)
* [quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer)
  * [IStreamProducer](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.__init__)
    * [get\_StreamId](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_StreamId)
    * [get\_Epoch](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Epoch)
    * [set\_Epoch](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.set_Epoch)
    * [get\_Properties](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Properties)
    * [get\_Timeseries](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Timeseries)
    * [get\_Events](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Events)
    * [Close](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.Close)
    * [add\_OnWriteException](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.add_OnWriteException)
    * [remove\_OnWriteException](#quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.remove_OnWriteException)
* [quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer)
  * [ITopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.__init__)
    * [Subscribe](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.Subscribe)
    * [add\_OnStreamReceived](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnStreamReceived)
    * [remove\_OnStreamReceived](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnStreamReceived)
    * [add\_OnRevoking](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnRevoking)
    * [remove\_OnRevoking](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnRevoking)
    * [add\_OnStreamsRevoked](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnStreamsRevoked)
    * [remove\_OnStreamsRevoked](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnStreamsRevoked)
    * [add\_OnCommitted](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnCommitted)
    * [remove\_OnCommitted](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnCommitted)
    * [add\_OnCommitting](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnCommitting)
    * [remove\_OnCommitting](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnCommitting)
    * [Commit](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.Commit)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnDisposed)
* [quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer)
  * [TopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Constructor)
    * [add\_OnStreamReceived](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnStreamReceived)
    * [remove\_OnStreamReceived](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnStreamReceived)
    * [add\_OnRevoking](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnRevoking)
    * [remove\_OnRevoking](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnRevoking)
    * [add\_OnStreamsRevoked](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnStreamsRevoked)
    * [remove\_OnStreamsRevoked](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnStreamsRevoked)
    * [add\_OnCommitted](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnCommitted)
    * [remove\_OnCommitted](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnCommitted)
    * [add\_OnCommitting](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnCommitting)
    * [remove\_OnCommitting](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnCommitting)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnDisposed)
    * [Commit](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Commit)
    * [Subscribe](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Subscribe)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.App](#quixstreams.native.Python.QuixStreamsStreaming.App)
  * [App](#quixstreams.native.Python.QuixStreamsStreaming.App.App)
    * [Run](#quixstreams.native.Python.QuixStreamsStreaming.App.App.Run)
* [quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues)
  * [TimeseriesDataTimestampValues](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.__init__)
    * [get\_Values](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Values)
    * [ContainsKey](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.ContainsKey)
    * [TryGetValue](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.TryGetValue)
    * [get\_Count](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Count)
    * [get\_Keys](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Keys)
    * [get\_Item](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Item)
    * [Equals](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.Equals)
    * [GetHashCode](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.GetHashCode)
    * [ToString](#quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.ToString)
* [quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient)
  * [QuixStreamingClient](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.Constructor)
    * [get\_TokenValidationConfig](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.get_TokenValidationConfig)
    * [set\_TokenValidationConfig](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.set_TokenValidationConfig)
    * [GetTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetTopicConsumer)
    * [GetRawTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetRawTopicConsumer)
    * [GetRawTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetRawTopicProducer)
    * [GetTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetTopicProducer)
    * [get\_ApiUrl](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.get_ApiUrl)
    * [set\_ApiUrl](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.set_ApiUrl)
    * [get\_CachePeriod](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.get_CachePeriod)
    * [set\_CachePeriod](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.set_CachePeriod)
* [quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient\_TokenValidationConfiguration](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration)
  * [TokenValidationConfiguration](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.Constructor)
    * [get\_Enabled](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.get_Enabled)
    * [set\_Enabled](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.set_Enabled)
    * [get\_WarningBeforeExpiry](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.get_WarningBeforeExpiry)
    * [set\_WarningBeforeExpiry](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.set_WarningBeforeExpiry)
    * [get\_WarnAboutNonPatToken](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.get_WarnAboutNonPatToken)
    * [set\_WarnAboutNonPatToken](#quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.set_WarnAboutNonPatToken)
* [quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage)
  * [RawMessage](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.Constructor2)
    * [get\_Metadata](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.get_Metadata)
    * [get\_Key](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.get_Key)
    * [set\_Key](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.set_Key)
    * [get\_Value](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.get_Value)
    * [set\_Value](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.set_Value)
* [quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer)
  * [IRawTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.__init__)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.Publish)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.remove_OnDisposed)
* [quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer)
  * [RawTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.Constructor)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.remove_OnDisposed)
    * [Publish](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.Publish)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer)
  * [RawTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.Constructor)
    * [add\_OnMessageReceived](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.add_OnMessageReceived)
    * [remove\_OnMessageReceived](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.remove_OnMessageReceived)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.remove_OnDisposed)
    * [add\_OnErrorOccurred](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.add_OnErrorOccurred)
    * [remove\_OnErrorOccurred](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.remove_OnErrorOccurred)
    * [Subscribe](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.Subscribe)
    * [Dispose](#quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.Dispose)
* [quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer)
  * [IRawTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.__init__)
    * [Subscribe](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.Subscribe)
    * [add\_OnMessageReceived](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.add_OnMessageReceived)
    * [remove\_OnMessageReceived](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.remove_OnMessageReceived)
    * [add\_OnErrorOccurred](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.add_OnErrorOccurred)
    * [remove\_OnErrorOccurred](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.remove_OnErrorOccurred)
    * [add\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.add_OnDisposed)
    * [remove\_OnDisposed](#quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.remove_OnDisposed)
* [quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient)
  * [KafkaStreamingClient](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.Constructor)
    * [GetTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetTopicConsumer)
    * [GetRawTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetRawTopicConsumer)
    * [GetRawTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetRawTopicProducer)
    * [GetTopicProducer](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetTopicProducer)
* [quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions)
  * [KafkaStreamingClientExtensions](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions.KafkaStreamingClientExtensions)
    * [GetTopicConsumer](#quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions.KafkaStreamingClientExtensions.GetTopicConsumer)
* [quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage](#quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage)
  * [LocalFileStorage](#quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage.Constructor)
* [quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions)
  * [StorageExtensions](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions)
    * [Set](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set)
    * [Set2](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set2)
    * [Set3](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set3)
    * [Set4](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set4)
    * [Set5](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set5)
    * [Set6](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set6)
    * [Get](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Get)
    * [GetDouble](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetDouble)
    * [GetString](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetString)
    * [GetBool](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetBool)
    * [GetLong](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetLong)
    * [GetBinary](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetBinary)
    * [Remove](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Remove)
    * [ContainsKey](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.ContainsKey)
    * [GetAllKeys](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetAllKeys)
    * [Clear](#quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Clear)
* [quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage](#quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage)
  * [IStateStorage](#quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage.IStateStorage)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage.IStateStorage.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage.IStateStorage.__init__)
* [quixstreams.native.Python.QuixStreamsState.StateValue\_StateType](#quixstreams.native.Python.QuixStreamsState.StateValue_StateType)
* [quixstreams.native.Python.QuixStreamsState.StateValue](#quixstreams.native.Python.QuixStreamsState.StateValue)
  * [StateValue](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor2)
    * [Constructor3](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor3)
    * [Constructor4](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor4)
    * [Constructor5](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor5)
    * [Constructor6](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor6)
    * [get\_Type](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_Type)
    * [get\_DoubleValue](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_DoubleValue)
    * [get\_LongValue](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_LongValue)
    * [get\_StringValue](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_StringValue)
    * [get\_BoolValue](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_BoolValue)
    * [get\_BinaryValue](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_BinaryValue)
    * [Equals](#quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Equals)
* [quixstreams.native.Python.SystemNetPrimitives.System.Net.HttpStatusCode](#quixstreams.native.Python.SystemNetPrimitives.System.Net.HttpStatusCode)
* [quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter](#quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter)
  * [IByteSplitter](#quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter.__init__)
    * [Split](#quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter.Split)
* [quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions)
  * [CommitOptions](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.Constructor)
    * [get\_AutoCommitEnabled](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.get_AutoCommitEnabled)
    * [set\_AutoCommitEnabled](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.set_AutoCommitEnabled)
    * [get\_CommitInterval](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.get_CommitInterval)
    * [set\_CommitInterval](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.set_CommitInterval)
    * [get\_CommitEvery](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.get_CommitEvery)
    * [set\_CommitEvery](#quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.set_CommitEvery)
* [quixstreams.native.Python.QuixStreamsTransport.IO.IProducer](#quixstreams.native.Python.QuixStreamsTransport.IO.IProducer)
  * [IProducer](#quixstreams.native.Python.QuixStreamsTransport.IO.IProducer.IProducer)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTransport.IO.IProducer.IProducer.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTransport.IO.IProducer.IProducer.__init__)
* [quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext](#quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext)
  * [TransportContext](#quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext)
    * [\_\_new\_\_](#quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.__init__)
    * [Constructor](#quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.Constructor)
    * [Constructor2](#quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.Constructor2)
* [quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging](#quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging)
  * [Logging](#quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging.Logging)
    * [UpdateFactory](#quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging.Logging.UpdateFactory)
* [quixstreams.native.Python.MicrosoftExtensionsLoggingAbstractions.Microsoft.Extensions.Logging.LogLevel](#quixstreams.native.Python.MicrosoftExtensionsLoggingAbstractions.Microsoft.Extensions.Logging.LogLevel)
* [quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs](#quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs)
  * [PropertyChangedEventArgs](#quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.__init__)
    * [Constructor](#quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.Constructor)
    * [get\_PropertyName](#quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.get_PropertyName)
* [quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs](#quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs)
  * [NotifyCollectionChangedEventArgs](#quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.__init__)
    * [get\_NewStartingIndex](#quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.get_NewStartingIndex)
    * [get\_OldStartingIndex](#quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.get_OldStartingIndex)
* [quixstreams.native.Python.SystemNetHttp.HttpClient](#quixstreams.native.Python.SystemNetHttp.HttpClient)
  * [HttpClient](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient)
    * [\_\_new\_\_](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.__new__)
    * [\_\_init\_\_](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.__init__)
    * [Constructor](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.Constructor)
    * [get\_BaseAddress](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.get_BaseAddress)
    * [set\_BaseAddress](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.set_BaseAddress)
    * [get\_Timeout](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.get_Timeout)
    * [set\_Timeout](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.set_Timeout)
    * [get\_MaxResponseContentBufferSize](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.get_MaxResponseContentBufferSize)
    * [set\_MaxResponseContentBufferSize](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.set_MaxResponseContentBufferSize)
    * [CancelPendingRequests](#quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.CancelPendingRequests)
* [quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable)
  * [Enumerable](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable.Enumerable)
    * [ReadAny](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable.Enumerable.ReadAny)
* [quixstreams.native.Python.InteropHelpers.ExternalTypes.System.List](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.List)
* [quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Collection](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Collection)
* [quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary)
  * [Dictionary](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary)
    * [ReadAnyHPtrToUPtr](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadAnyHPtrToUPtr)
    * [WriteBlittables](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteBlittables)
    * [ReadBlittables](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadBlittables)
    * [WriteStringPointers](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringPointers)
    * [ReadStringPointers](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringPointers)
    * [WriteStringDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringDoublesArray)
    * [ReadStringDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringDoublesArray)
    * [WriteStringLongsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringLongsArray)
    * [ReadStringLongsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringLongsArray)
    * [WriteStringStrings](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringStrings)
    * [ReadStringStrings](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringStrings)
    * [WriteStringStringsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringStringsArray)
    * [ReadStringStringsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringStringsArray)
    * [WriteStringBytesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringBytesArray)
    * [ReadStringBytesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringBytesArray)
    * [WriteStringNullableDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringNullableDoublesArray)
    * [ReadStringNullableDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringNullableDoublesArray)
* [quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array)
  * [Array](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array)
    * [ReadBlittables](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadBlittables)
    * [WriteBlittables](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteBlittables)
    * [ReadArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadArray)
    * [WriteArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteArray)
    * [ReadNullables](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadNullables)
    * [ReadLongs](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadLongs)
    * [ReadLongsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadLongsArray)
    * [WriteLongs](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteLongs)
    * [WriteLongsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteLongsArray)
    * [ReadStrings](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadStrings)
    * [ReadStringsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadStringsArray)
    * [WriteStrings](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteStrings)
    * [WriteStringsArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteStringsArray)
    * [ReadDoubles](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadDoubles)
    * [ReadDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadDoublesArray)
    * [WriteDoubles](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteDoubles)
    * [WriteDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteDoublesArray)
    * [ReadPointers](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadPointers)
    * [ReadPointersArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadPointersArray)
    * [WritePointers](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WritePointers)
    * [WritePointersArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WritePointersArray)
    * [ReadBytes](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadBytes)
    * [ReadBytesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadBytesArray)
    * [WriteBytes](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteBytes)
    * [WriteBytesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteBytesArray)
    * [ReadNullableDoubles](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadNullableDoubles)
    * [ReadNullableDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadNullableDoublesArray)
    * [WriteNullableDoubles](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteNullableDoubles)
    * [WriteNullableDoublesArray](#quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteNullableDoublesArray)
* [quixstreams.native.Python.InteropHelpers.InteropUtils](#quixstreams.native.Python.InteropHelpers.InteropUtils)
  * [InteropUtils](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils)
    * [set\_exception\_callback](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.set_exception_callback)
    * [log\_debug](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.log_debug)
    * [enable\_debug](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.enable_debug)
    * [disable\_debug](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.disable_debug)
    * [pin\_hptr\_target](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.pin_hptr_target)
    * [get\_pin\_address](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.get_pin_address)
    * [free\_hptr](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.free_hptr)
    * [free\_uptr](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.free_uptr)
    * [allocate\_uptr](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.allocate_uptr)
    * [invoke\_and\_free](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.invoke_and_free)
    * [dict\_nullables](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.dict_nullables)
    * [create\_nullable](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.create_nullable)
    * [dict\_mem\_nullables](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.dict_mem_nullables)
    * [create\_mem\_nullable](#quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.create_mem_nullable)
* [quixstreams.builders](#quixstreams.builders)
* [quixstreams.builders.eventdefinitionbuilder](#quixstreams.builders.eventdefinitionbuilder)
  * [EventDefinitionBuilder](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder)
    * [\_\_init\_\_](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.__init__)
    * [set\_level](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_level)
    * [set\_custom\_properties](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.set_custom_properties)
    * [add\_definition](#quixstreams.builders.eventdefinitionbuilder.EventDefinitionBuilder.add_definition)
* [quixstreams.builders.timeseriesdatabuilder](#quixstreams.builders.timeseriesdatabuilder)
  * [TimeseriesDataBuilder](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder)
    * [\_\_init\_\_](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.__init__)
    * [add\_value](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_value)
    * [add\_tag](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tag)
    * [add\_tags](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.add_tags)
    * [publish](#quixstreams.builders.timeseriesdatabuilder.TimeseriesDataBuilder.publish)
* [quixstreams.builders.eventdatabuilder](#quixstreams.builders.eventdatabuilder)
  * [EventDataBuilder](#quixstreams.builders.eventdatabuilder.EventDataBuilder)
    * [\_\_init\_\_](#quixstreams.builders.eventdatabuilder.EventDataBuilder.__init__)
    * [add\_value](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_value)
    * [add\_tag](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tag)
    * [add\_tags](#quixstreams.builders.eventdatabuilder.EventDataBuilder.add_tags)
    * [publish](#quixstreams.builders.eventdatabuilder.EventDataBuilder.publish)
* [quixstreams.builders.parameterdefinitionbuilder](#quixstreams.builders.parameterdefinitionbuilder)
  * [ParameterDefinitionBuilder](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder)
    * [\_\_init\_\_](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.__init__)
    * [set\_range](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_range)
    * [set\_unit](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_unit)
    * [set\_format](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_format)
    * [set\_custom\_properties](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.set_custom_properties)
    * [add\_definition](#quixstreams.builders.parameterdefinitionbuilder.ParameterDefinitionBuilder.add_definition)
* [quixstreams.topicproducer](#quixstreams.topicproducer)
  * [TopicProducer](#quixstreams.topicproducer.TopicProducer)
    * [\_\_init\_\_](#quixstreams.topicproducer.TopicProducer.__init__)
    * [on\_disposed](#quixstreams.topicproducer.TopicProducer.on_disposed)
    * [on\_disposed](#quixstreams.topicproducer.TopicProducer.on_disposed)
    * [create\_stream](#quixstreams.topicproducer.TopicProducer.create_stream)
    * [get\_stream](#quixstreams.topicproducer.TopicProducer.get_stream)
    * [get\_or\_create\_stream](#quixstreams.topicproducer.TopicProducer.get_or_create_stream)
* [quixstreams.models.streamproducer](#quixstreams.models.streamproducer)
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
    * [get\_net\_pointer](#quixstreams.models.parametervalue.ParameterValue.get_net_pointer)
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
* [quixstreams.models.streamendtype](#quixstreams.models.streamendtype)
* [quixstreams.models.commitmode](#quixstreams.models.commitmode)
* [quixstreams.models](#quixstreams.models)
* [quixstreams.models.streampackage](#quixstreams.models.streampackage)
  * [StreamPackage](#quixstreams.models.streampackage.StreamPackage)
    * [\_\_init\_\_](#quixstreams.models.streampackage.StreamPackage.__init__)
    * [transport\_context](#quixstreams.models.streampackage.StreamPackage.transport_context)
    * [to\_json](#quixstreams.models.streampackage.StreamPackage.to_json)
    * [get\_net\_pointer](#quixstreams.models.streampackage.StreamPackage.get_net_pointer)
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
* [quixstreams.models.eventdefinition](#quixstreams.models.eventdefinition)
  * [EventDefinition](#quixstreams.models.eventdefinition.EventDefinition)
    * [\_\_init\_\_](#quixstreams.models.eventdefinition.EventDefinition.__init__)
* [quixstreams.models.streamconsumer.timeseriesbufferconsumer](#quixstreams.models.streamconsumer.timeseriesbufferconsumer)
  * [TimeseriesBufferConsumer](#quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer)
    * [\_\_init\_\_](#quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.__init__)
    * [get\_net\_pointer](#quixstreams.models.streamconsumer.timeseriesbufferconsumer.TimeseriesBufferConsumer.get_net_pointer)
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
* [quixstreams.models.streamconsumer](#quixstreams.models.streamconsumer)
* [quixstreams.models.streamconsumer.streameventsconsumer](#quixstreams.models.streamconsumer.streameventsconsumer)
  * [StreamEventsConsumer](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer)
    * [\_\_init\_\_](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.__init__)
    * [on\_data\_received](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received)
    * [on\_data\_received](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_data_received)
    * [on\_definitions\_changed](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed)
    * [on\_definitions\_changed](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.on_definitions_changed)
    * [definitions](#quixstreams.models.streamconsumer.streameventsconsumer.StreamEventsConsumer.definitions)
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
* [quixstreams.models.parameterdefinition](#quixstreams.models.parameterdefinition)
  * [ParameterDefinition](#quixstreams.models.parameterdefinition.ParameterDefinition)
    * [\_\_init\_\_](#quixstreams.models.parameterdefinition.ParameterDefinition.__init__)
* [quixstreams.models.autooffsetreset](#quixstreams.models.autooffsetreset)
  * [AutoOffsetReset](#quixstreams.models.autooffsetreset.AutoOffsetReset)
    * [Latest](#quixstreams.models.autooffsetreset.AutoOffsetReset.Latest)
    * [Earliest](#quixstreams.models.autooffsetreset.AutoOffsetReset.Earliest)
    * [Error](#quixstreams.models.autooffsetreset.AutoOffsetReset.Error)
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
* [quixstreams.models.netdict](#quixstreams.models.netdict)
  * [ReadOnlyNetDict](#quixstreams.models.netdict.ReadOnlyNetDict)
  * [NetDict](#quixstreams.models.netdict.NetDict)
    * [constructor\_for\_string\_string](#quixstreams.models.netdict.NetDict.constructor_for_string_string)
* [quixstreams.state.inmemorystorage](#quixstreams.state.inmemorystorage)
  * [InMemoryStorage](#quixstreams.state.inmemorystorage.InMemoryStorage)
* [quixstreams.state](#quixstreams.state)
* [quixstreams.state.statetype](#quixstreams.state.statetype)
* [quixstreams.state.statevalue](#quixstreams.state.statevalue)
  * [StateValue](#quixstreams.state.statevalue.StateValue)
    * [\_\_init\_\_](#quixstreams.state.statevalue.StateValue.__init__)
    * [type](#quixstreams.state.statevalue.StateValue.type)
    * [value](#quixstreams.state.statevalue.StateValue.value)
    * [get\_net\_pointer](#quixstreams.state.statevalue.StateValue.get_net_pointer)
* [quixstreams.state.localfilestorage](#quixstreams.state.localfilestorage)
  * [LocalFileStorage](#quixstreams.state.localfilestorage.LocalFileStorage)
    * [\_\_init\_\_](#quixstreams.state.localfilestorage.LocalFileStorage.__init__)
    * [get](#quixstreams.state.localfilestorage.LocalFileStorage.get)
    * [set](#quixstreams.state.localfilestorage.LocalFileStorage.set)
    * [contains\_key](#quixstreams.state.localfilestorage.LocalFileStorage.contains_key)
    * [get\_all\_keys](#quixstreams.state.localfilestorage.LocalFileStorage.get_all_keys)
    * [remove](#quixstreams.state.localfilestorage.LocalFileStorage.remove)
    * [clear](#quixstreams.state.localfilestorage.LocalFileStorage.clear)
* [quixstreams.exceptions.quixapiexception](#quixstreams.exceptions.quixapiexception)
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
    * [get\_net\_pointer](#quixstreams.topicconsumer.TopicConsumer.get_net_pointer)
* [quixstreams.app](#quixstreams.app)
  * [CancellationTokenSource](#quixstreams.app.CancellationTokenSource)
    * [\_\_init\_\_](#quixstreams.app.CancellationTokenSource.__init__)
    * [is\_cancellation\_requested](#quixstreams.app.CancellationTokenSource.is_cancellation_requested)
    * [cancel](#quixstreams.app.CancellationTokenSource.cancel)
    * [token](#quixstreams.app.CancellationTokenSource.token)
    * [get\_net\_pointer](#quixstreams.app.CancellationTokenSource.get_net_pointer)
  * [App](#quixstreams.app.App)
    * [run](#quixstreams.app.App.run)
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
* [quixstreams.raw.rawtopicproducer](#quixstreams.raw.rawtopicproducer)
  * [RawTopicProducer](#quixstreams.raw.rawtopicproducer.RawTopicProducer)
    * [\_\_init\_\_](#quixstreams.raw.rawtopicproducer.RawTopicProducer.__init__)
    * [publish](#quixstreams.raw.rawtopicproducer.RawTopicProducer.publish)
* [quixstreams.raw.rawtopicconsumer](#quixstreams.raw.rawtopicconsumer)
  * [RawTopicConsumer](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer)
    * [\_\_init\_\_](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.__init__)
    * [on\_message\_received](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received)
    * [on\_message\_received](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_message_received)
    * [on\_error\_occurred](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred)
    * [on\_error\_occurred](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.on_error_occurred)
    * [subscribe](#quixstreams.raw.rawtopicconsumer.RawTopicConsumer.subscribe)
* [quixstreams.helpers.nativedecorator](#quixstreams.helpers.nativedecorator)
* [quixstreams.helpers](#quixstreams.helpers)
* [quixstreams.helpers.timeconverter](#quixstreams.helpers.timeconverter)
  * [TimeConverter](#quixstreams.helpers.timeconverter.TimeConverter)
    * [offset\_from\_utc](#quixstreams.helpers.timeconverter.TimeConverter.offset_from_utc)
    * [to\_unix\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.to_unix_nanoseconds)
    * [to\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.to_nanoseconds)
    * [from\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.from_nanoseconds)
    * [from\_unix\_nanoseconds](#quixstreams.helpers.timeconverter.TimeConverter.from_unix_nanoseconds)
    * [from\_string](#quixstreams.helpers.timeconverter.TimeConverter.from_string)
* [quixstreams.helpers.exceptionconverter](#quixstreams.helpers.exceptionconverter)
* [quixstreams.helpers.enumconverter](#quixstreams.helpers.enumconverter)
* [quixstreams.helpers.dotnet.datetimeconverter](#quixstreams.helpers.dotnet.datetimeconverter)
  * [DateTimeConverter](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter)
    * [datetime\_to\_python](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_python)
    * [datetime\_to\_dotnet](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.datetime_to_dotnet)
    * [timespan\_to\_python](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timespan_to_python)
    * [timedelta\_to\_dotnet](#quixstreams.helpers.dotnet.datetimeconverter.DateTimeConverter.timedelta_to_dotnet)
* [quixstreams.kafkastreamingclient](#quixstreams.kafkastreamingclient)
  * [KafkaStreamingClient](#quixstreams.kafkastreamingclient.KafkaStreamingClient)
    * [\_\_init\_\_](#quixstreams.kafkastreamingclient.KafkaStreamingClient.__init__)
    * [get\_topic\_consumer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_consumer)
    * [get\_topic\_producer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_topic_producer)
    * [get\_raw\_topic\_consumer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_consumer)
    * [get\_raw\_topic\_producer](#quixstreams.kafkastreamingclient.KafkaStreamingClient.get_raw_topic_producer)

<a id="quixstreams"></a>

# quixstreams

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

<a id="quixstreams.streamconsumer.StreamConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Gets the associated .NET object pointer.

**Returns**:

- `ctypes.c_void_p` - The .NET pointer

<a id="quixstreams.logging"></a>

# quixstreams.logging

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

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri"></a>

# quixstreams.native.Python.SystemPrivateUri.System.Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri"></a>

## Uri Objects

```python
class Uri(object)
```

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
----------

Uri:
    Instance wrapping the .net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
----------

Uri:
    Instance wrapping the .net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(uriString: str) -> c_void_p
```

Parameters
----------

uriString: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(uriString: str, dontEscape: bool) -> c_void_p
```

Parameters
----------

uriString: str
    Underlying .Net type is string

dontEscape: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor3"></a>

#### Constructor3

```python
@staticmethod
def Constructor3(baseUri: c_void_p, relativeUri: str,
                 dontEscape: bool) -> c_void_p
```

Parameters
----------

baseUri: c_void_p
    GC Handle Pointer to .Net type Uri

relativeUri: str
    Underlying .Net type is string

dontEscape: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor6"></a>

#### Constructor6

```python
@staticmethod
def Constructor6(baseUri: c_void_p, relativeUri: str) -> c_void_p
```

Parameters
----------

baseUri: c_void_p
    GC Handle Pointer to .Net type Uri

relativeUri: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Constructor7"></a>

#### Constructor7

```python
@staticmethod
def Constructor7(baseUri: c_void_p, relativeUri: c_void_p) -> c_void_p
```

Parameters
----------

baseUri: c_void_p
    GC Handle Pointer to .Net type Uri

relativeUri: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_AbsolutePath"></a>

#### get\_AbsolutePath

```python
def get_AbsolutePath() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_AbsoluteUri"></a>

#### get\_AbsoluteUri

```python
def get_AbsoluteUri() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_LocalPath"></a>

#### get\_LocalPath

```python
def get_LocalPath() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Authority"></a>

#### get\_Authority

```python
def get_Authority() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsDefaultPort"></a>

#### get\_IsDefaultPort

```python
def get_IsDefaultPort() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsFile"></a>

#### get\_IsFile

```python
def get_IsFile() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsLoopback"></a>

#### get\_IsLoopback

```python
def get_IsLoopback() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_PathAndQuery"></a>

#### get\_PathAndQuery

```python
def get_PathAndQuery() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Segments"></a>

#### get\_Segments

```python
def get_Segments() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type string[]

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsUnc"></a>

#### get\_IsUnc

```python
def get_IsUnc() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Host"></a>

#### get\_Host

```python
def get_Host() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Port"></a>

#### get\_Port

```python
def get_Port() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Query"></a>

#### get\_Query

```python
def get_Query() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Fragment"></a>

#### get\_Fragment

```python
def get_Fragment() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_Scheme"></a>

#### get\_Scheme

```python
def get_Scheme() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_OriginalString"></a>

#### get\_OriginalString

```python
def get_OriginalString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_DnsSafeHost"></a>

#### get\_DnsSafeHost

```python
def get_DnsSafeHost() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IdnHost"></a>

#### get\_IdnHost

```python
def get_IdnHost() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_IsAbsoluteUri"></a>

#### get\_IsAbsoluteUri

```python
def get_IsAbsoluteUri() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UserEscaped"></a>

#### get\_UserEscaped

```python
def get_UserEscaped() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UserInfo"></a>

#### get\_UserInfo

```python
def get_UserInfo() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.IsHexEncoding"></a>

#### IsHexEncoding

```python
@staticmethod
def IsHexEncoding(pattern: str, index: int) -> bool
```

Parameters
----------

pattern: str
    Underlying .Net type is string

index: int
    Underlying .Net type is int

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.CheckSchemeName"></a>

#### CheckSchemeName

```python
@staticmethod
def CheckSchemeName(schemeName: str) -> bool
```

Parameters
----------

schemeName: str
    Underlying .Net type is string

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.op_Equality"></a>

#### op\_Equality

```python
@staticmethod
def op_Equality(uri1: c_void_p, uri2: c_void_p) -> bool
```

Parameters
----------

uri1: c_void_p
    GC Handle Pointer to .Net type Uri

uri2: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.op_Inequality"></a>

#### op\_Inequality

```python
@staticmethod
def op_Inequality(uri1: c_void_p, uri2: c_void_p) -> bool
```

Parameters
----------

uri1: c_void_p
    GC Handle Pointer to .Net type Uri

uri2: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.Equals"></a>

#### Equals

```python
def Equals(comparand: c_void_p) -> bool
```

Parameters
----------

comparand: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.MakeRelativeUri"></a>

#### MakeRelativeUri

```python
def MakeRelativeUri(uri: c_void_p) -> c_void_p
```

Parameters
----------

uri: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.MakeRelative"></a>

#### MakeRelative

```python
def MakeRelative(toUri: c_void_p) -> str
```

Parameters
----------

toUri: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.TryCreate3"></a>

#### TryCreate3

```python
@staticmethod
def TryCreate3(baseUri: c_void_p, relativeUri: str, result: c_void_p) -> bool
```

Parameters
----------

baseUri: c_void_p
    GC Handle Pointer to .Net type Uri

relativeUri: str
    Underlying .Net type is string

result: c_void_p
    GC Handle Pointer to .Net type Uri&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.TryCreate4"></a>

#### TryCreate4

```python
@staticmethod
def TryCreate4(baseUri: c_void_p, relativeUri: c_void_p,
               result: c_void_p) -> bool
```

Parameters
----------

baseUri: c_void_p
    GC Handle Pointer to .Net type Uri

relativeUri: c_void_p
    GC Handle Pointer to .Net type Uri

result: c_void_p
    GC Handle Pointer to .Net type Uri&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.IsWellFormedOriginalString"></a>

#### IsWellFormedOriginalString

```python
def IsWellFormedOriginalString() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.UnescapeDataString"></a>

#### UnescapeDataString

```python
@staticmethod
def UnescapeDataString(stringToUnescape: str) -> str
```

Parameters
----------

stringToUnescape: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.EscapeUriString"></a>

#### EscapeUriString

```python
@staticmethod
def EscapeUriString(stringToEscape: str) -> str
```

Parameters
----------

stringToEscape: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.EscapeDataString"></a>

#### EscapeDataString

```python
@staticmethod
def EscapeDataString(stringToEscape: str) -> str
```

Parameters
----------

stringToEscape: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.IsBaseOf"></a>

#### IsBaseOf

```python
def IsBaseOf(uri: c_void_p) -> bool
```

Parameters
----------

uri: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeFile"></a>

#### get\_UriSchemeFile

```python
@staticmethod
def get_UriSchemeFile() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeFtp"></a>

#### get\_UriSchemeFtp

```python
@staticmethod
def get_UriSchemeFtp() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeSftp"></a>

#### get\_UriSchemeSftp

```python
@staticmethod
def get_UriSchemeSftp() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeFtps"></a>

#### get\_UriSchemeFtps

```python
@staticmethod
def get_UriSchemeFtps() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeGopher"></a>

#### get\_UriSchemeGopher

```python
@staticmethod
def get_UriSchemeGopher() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeHttp"></a>

#### get\_UriSchemeHttp

```python
@staticmethod
def get_UriSchemeHttp() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeHttps"></a>

#### get\_UriSchemeHttps

```python
@staticmethod
def get_UriSchemeHttps() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeWs"></a>

#### get\_UriSchemeWs

```python
@staticmethod
def get_UriSchemeWs() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeWss"></a>

#### get\_UriSchemeWss

```python
@staticmethod
def get_UriSchemeWss() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeMailto"></a>

#### get\_UriSchemeMailto

```python
@staticmethod
def get_UriSchemeMailto() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNews"></a>

#### get\_UriSchemeNews

```python
@staticmethod
def get_UriSchemeNews() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNntp"></a>

#### get\_UriSchemeNntp

```python
@staticmethod
def get_UriSchemeNntp() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeSsh"></a>

#### get\_UriSchemeSsh

```python
@staticmethod
def get_UriSchemeSsh() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeTelnet"></a>

#### get\_UriSchemeTelnet

```python
@staticmethod
def get_UriSchemeTelnet() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNetTcp"></a>

#### get\_UriSchemeNetTcp

```python
@staticmethod
def get_UriSchemeNetTcp() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_UriSchemeNetPipe"></a>

#### get\_UriSchemeNetPipe

```python
@staticmethod
def get_UriSchemeNetPipe() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateUri.System.Uri.Uri.get_SchemeDelimiter"></a>

#### get\_SchemeDelimiter

```python
@staticmethod
def get_SchemeDelimiter() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline"></a>

## IStreamPipeline Objects

```python
class IStreamPipeline(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStreamPipeline

Returns
----------

IStreamPipeline:
    Instance wrapping the .net type IStreamPipeline

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStreamPipeline

Returns
----------

IStreamPipeline:
    Instance wrapping the .net type IStreamPipeline

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.get_StreamId"></a>

#### get\_StreamId

```python
def get_StreamId() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.get_SourceMetadata"></a>

#### get\_SourceMetadata

```python
def get_SourceMetadata() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.set_SourceMetadata"></a>

#### set\_SourceMetadata

```python
def set_SourceMetadata(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.Subscribe3"></a>

#### Subscribe3

```python
def Subscribe3(
        onStreamPackage: Callable[[c_void_p, c_void_p], None]) -> c_void_p
```

Parameters
----------

onStreamPackage: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is Action<IStreamPipeline, StreamPackage>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamPipeline

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.Close"></a>

#### Close

```python
def Close() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.add_OnClosing"></a>

#### add\_OnClosing

```python
def add_OnClosing(value: Callable[[], None]) -> None
```

Parameters
----------

value: Callable[[], None]
    Underlying .Net type is Action

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.remove_OnClosing"></a>

#### remove\_OnClosing

```python
def remove_OnClosing(value: Callable[[], None]) -> None
```

Parameters
----------

value: Callable[[], None]
    Underlying .Net type is Action

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.add_OnClosed"></a>

#### add\_OnClosed

```python
def add_OnClosed(value: Callable[[], None]) -> None
```

Parameters
----------

value: Callable[[], None]
    Underlying .Net type is Action

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.IStreamPipeline.IStreamPipeline.remove_OnClosed"></a>

#### remove\_OnClosed

```python
def remove_OnClosed(value: Callable[[], None]) -> None
```

Parameters
----------

value: Callable[[], None]
    Underlying .Net type is Action

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamEndType"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamEndType

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage"></a>

## StreamPackage Objects

```python
class StreamPackage(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamPackage

Returns
----------

StreamPackage:
    Instance wrapping the .net type StreamPackage

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamPackage

Returns
----------

StreamPackage:
    Instance wrapping the .net type StreamPackage

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(type: c_void_p, value: c_void_p) -> c_void_p
```

Parameters
----------

type: c_void_p
    GC Handle Pointer to .Net type Type

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamPackage

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.get_Type"></a>

#### get\_Type

```python
def get_Type() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.set_Type"></a>

#### set\_Type

```python
def set_Type(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.get_TransportContext"></a>

#### get\_TransportContext

```python
def get_TransportContext() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TransportContext

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.set_TransportContext"></a>

#### set\_TransportContext

```python
def set_TransportContext(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TransportContext

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.get_Value"></a>

#### get\_Value

```python
def get_Value() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.set_Value"></a>

#### set\_Value

```python
def set_Value(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.StreamPackage.StreamPackage.ToJson"></a>

#### ToJson

```python
def ToJson() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventLevel"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Models.EventLevel

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition"></a>

## EventDefinition Objects

```python
class EventDefinition(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDefinition

Returns
----------

EventDefinition:
    Instance wrapping the .net type EventDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDefinition

Returns
----------

EventDefinition:
    Instance wrapping the .net type EventDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Id"></a>

#### get\_Id

```python
def get_Id() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Id"></a>

#### set\_Id

```python
def set_Id(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Name"></a>

#### set\_Name

```python
def set_Name(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Description"></a>

#### get\_Description

```python
def get_Description() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Description"></a>

#### set\_Description

```python
def set_Description(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_CustomProperties"></a>

#### get\_CustomProperties

```python
def get_CustomProperties() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_CustomProperties"></a>

#### set\_CustomProperties

```python
def set_CustomProperties(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.get_Level"></a>

#### get\_Level

```python
def get_Level() -> EventLevel
```

Parameters
----------

Returns
-------

EventLevel:
    Underlying .Net type is EventLevel

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.EventDefinition.EventDefinition.set_Level"></a>

#### set\_Level

```python
def set_Level(value: EventLevel) -> None
```

Parameters
----------

value: EventLevel
    Underlying .Net type is EventLevel

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw"></a>

## TimeseriesDataRaw Objects

```python
class TimeseriesDataRaw(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataRaw

Returns
----------

TimeseriesDataRaw:
    Instance wrapping the .net type TimeseriesDataRaw

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataRaw

Returns
----------

TimeseriesDataRaw:
    Instance wrapping the .net type TimeseriesDataRaw

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataRaw

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(epoch: int, timestamps: c_void_p, numericValues: c_void_p,
                 stringValues: c_void_p, binaryValues: c_void_p,
                 tagValues: c_void_p) -> c_void_p
```

Parameters
----------

epoch: int
    Underlying .Net type is long

timestamps: c_void_p
    GC Handle Pointer to .Net type long[]

numericValues: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, double?[]>

stringValues: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string[]>

binaryValues: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, byte[][]>

tagValues: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string[]>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataRaw

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.ToJson"></a>

#### ToJson

```python
def ToJson() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_Epoch"></a>

#### get\_Epoch

```python
def get_Epoch() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_Epoch"></a>

#### set\_Epoch

```python
def set_Epoch(value: int) -> None
```

Parameters
----------

value: int
    Underlying .Net type is long

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_Timestamps"></a>

#### get\_Timestamps

```python
def get_Timestamps() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type long[]

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_Timestamps"></a>

#### set\_Timestamps

```python
def set_Timestamps(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type long[]

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_NumericValues"></a>

#### get\_NumericValues

```python
def get_NumericValues() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, double?[]>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_NumericValues"></a>

#### set\_NumericValues

```python
def set_NumericValues(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, double?[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_StringValues"></a>

#### get\_StringValues

```python
def get_StringValues() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, string[]>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_StringValues"></a>

#### set\_StringValues

```python
def set_StringValues(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_BinaryValues"></a>

#### get\_BinaryValues

```python
def get_BinaryValues() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, byte[][]>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_BinaryValues"></a>

#### set\_BinaryValues

```python
def set_BinaryValues(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, byte[][]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.get_TagValues"></a>

#### get\_TagValues

```python
def get_TagValues() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, string[]>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw.TimeseriesDataRaw.set_TagValues"></a>

#### set\_TagValues

```python
def set_TagValues(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition"></a>

## ParameterDefinition Objects

```python
class ParameterDefinition(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterDefinition

Returns
----------

ParameterDefinition:
    Instance wrapping the .net type ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterDefinition

Returns
----------

ParameterDefinition:
    Instance wrapping the .net type ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Id"></a>

#### get\_Id

```python
def get_Id() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Id"></a>

#### set\_Id

```python
def set_Id(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Name"></a>

#### set\_Name

```python
def set_Name(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Description"></a>

#### get\_Description

```python
def get_Description() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Description"></a>

#### set\_Description

```python
def set_Description(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_MinimumValue"></a>

#### get\_MinimumValue

```python
def get_MinimumValue() -> Optional[float]
```

Parameters
----------

Returns
-------

Optional[float]:
    Underlying .Net type is double?

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_MinimumValue"></a>

#### set\_MinimumValue

```python
def set_MinimumValue(value: Optional[float]) -> None
```

Parameters
----------

value: Optional[float]
    Underlying .Net type is double?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_MaximumValue"></a>

#### get\_MaximumValue

```python
def get_MaximumValue() -> Optional[float]
```

Parameters
----------

Returns
-------

Optional[float]:
    Underlying .Net type is double?

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_MaximumValue"></a>

#### set\_MaximumValue

```python
def set_MaximumValue(value: Optional[float]) -> None
```

Parameters
----------

value: Optional[float]
    Underlying .Net type is double?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Unit"></a>

#### get\_Unit

```python
def get_Unit() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Unit"></a>

#### set\_Unit

```python
def set_Unit(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_Format"></a>

#### get\_Format

```python
def get_Format() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_Format"></a>

#### set\_Format

```python
def set_Format(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.get_CustomProperties"></a>

#### get\_CustomProperties

```python
def get_CustomProperties() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Models.ParameterDefinition.ParameterDefinition.set_CustomProperties"></a>

#### set\_CustomProperties

```python
def set_CustomProperties(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration"></a>

## KafkaProducerConfiguration Objects

```python
class KafkaProducerConfiguration(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type KafkaProducerConfiguration

Returns
----------

KafkaProducerConfiguration:
    Instance wrapping the .net type KafkaProducerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type KafkaProducerConfiguration

Returns
----------

KafkaProducerConfiguration:
    Instance wrapping the .net type KafkaProducerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(brokerList: str, properties: c_void_p = None) -> c_void_p
```

Parameters
----------

brokerList: str
    Underlying .Net type is string

properties: c_void_p
    (Optional) GC Handle Pointer to .Net type IDictionary<string, string>. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type KafkaProducerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_BrokerList"></a>

#### get\_BrokerList

```python
def get_BrokerList() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_MaxMessageSize"></a>

#### get\_MaxMessageSize

```python
def get_MaxMessageSize() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_MaxKeySize"></a>

#### get\_MaxKeySize

```python
def get_MaxKeySize() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.KafkaProducerConfiguration.KafkaProducerConfiguration.get_Properties"></a>

#### get\_Properties

```python
def get_Properties() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IDictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer"></a>

## TelemetryKafkaConsumer Objects

```python
class TelemetryKafkaConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaConsumer

Returns
----------

TelemetryKafkaConsumer:
    Instance wrapping the .net type TelemetryKafkaConsumer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaConsumer

Returns
----------

TelemetryKafkaConsumer:
    Instance wrapping the .net type TelemetryKafkaConsumer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(telemetryKafkaConsumerConfiguration: c_void_p,
                topic: str) -> c_void_p
```

Parameters
----------

telemetryKafkaConsumerConfiguration: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaConsumerConfiguration

topic: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TelemetryKafkaConsumer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnReceiveException"></a>

#### add\_OnReceiveException

```python
def add_OnReceiveException(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnReceiveException"></a>

#### remove\_OnReceiveException

```python
def remove_OnReceiveException(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnStreamsRevoked"></a>

#### add\_OnStreamsRevoked

```python
def add_OnStreamsRevoked(value: Callable[[c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p], None]
    Underlying .Net type is Action<IStreamPipeline[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnStreamsRevoked"></a>

#### remove\_OnStreamsRevoked

```python
def remove_OnStreamsRevoked(value: Callable[[c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p], None]
    Underlying .Net type is Action<IStreamPipeline[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnRevoking"></a>

#### add\_OnRevoking

```python
def add_OnRevoking(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnRevoking"></a>

#### remove\_OnRevoking

```python
def remove_OnRevoking(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnCommitted"></a>

#### add\_OnCommitted

```python
def add_OnCommitted(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnCommitted"></a>

#### remove\_OnCommitted

```python
def remove_OnCommitted(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.add_OnCommitting"></a>

#### add\_OnCommitting

```python
def add_OnCommitting(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.remove_OnCommitting"></a>

#### remove\_OnCommitting

```python
def remove_OnCommitting(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Start"></a>

#### Start

```python
def Start() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.ForEach"></a>

#### ForEach

```python
def ForEach(streamPipelineFactoryHandler: Callable[[str], c_void_p]) -> None
```

Parameters
----------

streamPipelineFactoryHandler: Callable[[str], c_void_p]
    Underlying .Net type is Func<string, IStreamPipeline>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Stop"></a>

#### Stop

```python
def Stop() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumer.TelemetryKafkaConsumer.Commit"></a>

#### Commit

```python
def Commit() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration"></a>

## TelemetryKafkaConsumerConfiguration Objects

```python
class TelemetryKafkaConsumerConfiguration(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaConsumerConfiguration

Returns
----------

TelemetryKafkaConsumerConfiguration:
    Instance wrapping the .net type TelemetryKafkaConsumerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaConsumerConfiguration

Returns
----------

TelemetryKafkaConsumerConfiguration:
    Instance wrapping the .net type TelemetryKafkaConsumerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(brokerList: str,
                consumerGroupId: str = None,
                properties: c_void_p = None) -> c_void_p
```

Parameters
----------

brokerList: str
    Underlying .Net type is string

consumerGroupId: str
    (Optional) Underlying .Net type is string. Defaults to None

properties: c_void_p
    (Optional) GC Handle Pointer to .Net type IDictionary<string, string>. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TelemetryKafkaConsumerConfiguration

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_BrokerList"></a>

#### get\_BrokerList

```python
def get_BrokerList() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_ConsumerGroupId"></a>

#### get\_ConsumerGroupId

```python
def get_ConsumerGroupId() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_CommitOptions"></a>

#### get\_CommitOptions

```python
def get_CommitOptions() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CommitOptions

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.set_CommitOptions"></a>

#### set\_CommitOptions

```python
def set_CommitOptions(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type CommitOptions

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaConsumerConfiguration.TelemetryKafkaConsumerConfiguration.get_Properties"></a>

#### get\_Properties

```python
def get_Properties() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IDictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer"></a>

## TelemetryKafkaProducer Objects

```python
class TelemetryKafkaProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaProducer

Returns
----------

TelemetryKafkaProducer:
    Instance wrapping the .net type TelemetryKafkaProducer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaProducer

Returns
----------

TelemetryKafkaProducer:
    Instance wrapping the .net type TelemetryKafkaProducer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(producer: c_void_p,
                byteSplitter: c_void_p,
                streamId: str = None) -> c_void_p
```

Parameters
----------

producer: c_void_p
    GC Handle Pointer to .Net type IProducer

byteSplitter: c_void_p
    GC Handle Pointer to .Net type IByteSplitter

streamId: str
    (Optional) Underlying .Net type is string. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TelemetryKafkaProducer

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.get_StreamId"></a>

#### get\_StreamId

```python
def get_StreamId() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.add_OnWriteException"></a>

#### add\_OnWriteException

```python
def add_OnWriteException(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.remove_OnWriteException"></a>

#### remove\_OnWriteException

```python
def remove_OnWriteException(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.get_OnStreamPipelineAssigned"></a>

#### get\_OnStreamPipelineAssigned

```python
def get_OnStreamPipelineAssigned() -> Callable[[], None]
```

Parameters
----------

Returns
-------

Callable[[], None]:
    Underlying .Net type is Action

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.TelemetryKafkaProducer.TelemetryKafkaProducer.set_OnStreamPipelineAssigned"></a>

#### set\_OnStreamPipelineAssigned

```python
def set_OnStreamPipelineAssigned(value: Callable[[], None]) -> None
```

Parameters
----------

value: Callable[[], None]
    Underlying .Net type is Action

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTelemetry.Kafka.AutoOffsetReset"></a>

# quixstreams.native.Python.QuixStreamsTelemetry.Kafka.AutoOffsetReset

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type"></a>

## Type Objects

```python
class Type(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type Type

Returns
----------

Type:
    Instance wrapping the .net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type Type

Returns
----------

Type:
    Instance wrapping the .net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsInterface"></a>

#### get\_IsInterface

```python
def get_IsInterface() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType"></a>

#### GetType

```python
@staticmethod
def GetType(typeName: str, throwOnError: bool, ignoreCase: bool) -> c_void_p
```

Parameters
----------

typeName: str
    Underlying .Net type is string

throwOnError: bool
    Underlying .Net type is Boolean

ignoreCase: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType2"></a>

#### GetType2

```python
@staticmethod
def GetType2(typeName: str, throwOnError: bool) -> c_void_p
```

Parameters
----------

typeName: str
    Underlying .Net type is string

throwOnError: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType3"></a>

#### GetType3

```python
@staticmethod
def GetType3(typeName: str) -> c_void_p
```

Parameters
----------

typeName: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetType7"></a>

#### GetType7

```python
def GetType7() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_Namespace"></a>

#### get\_Namespace

```python
def get_Namespace() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_AssemblyQualifiedName"></a>

#### get\_AssemblyQualifiedName

```python
def get_AssemblyQualifiedName() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_FullName"></a>

#### get\_FullName

```python
def get_FullName() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNested"></a>

#### get\_IsNested

```python
def get_IsNested() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_DeclaringType"></a>

#### get\_DeclaringType

```python
def get_DeclaringType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_ReflectedType"></a>

#### get\_ReflectedType

```python
def get_ReflectedType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_UnderlyingSystemType"></a>

#### get\_UnderlyingSystemType

```python
def get_UnderlyingSystemType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsTypeDefinition"></a>

#### get\_IsTypeDefinition

```python
def get_IsTypeDefinition() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsArray"></a>

#### get\_IsArray

```python
def get_IsArray() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsByRef"></a>

#### get\_IsByRef

```python
def get_IsByRef() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsPointer"></a>

#### get\_IsPointer

```python
def get_IsPointer() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsConstructedGenericType"></a>

#### get\_IsConstructedGenericType

```python
def get_IsConstructedGenericType() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericParameter"></a>

#### get\_IsGenericParameter

```python
def get_IsGenericParameter() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericTypeParameter"></a>

#### get\_IsGenericTypeParameter

```python
def get_IsGenericTypeParameter() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericMethodParameter"></a>

#### get\_IsGenericMethodParameter

```python
def get_IsGenericMethodParameter() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericType"></a>

#### get\_IsGenericType

```python
def get_IsGenericType() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsGenericTypeDefinition"></a>

#### get\_IsGenericTypeDefinition

```python
def get_IsGenericTypeDefinition() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSZArray"></a>

#### get\_IsSZArray

```python
def get_IsSZArray() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsVariableBoundArray"></a>

#### get\_IsVariableBoundArray

```python
def get_IsVariableBoundArray() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsByRefLike"></a>

#### get\_IsByRefLike

```python
def get_IsByRefLike() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsFunctionPointer"></a>

#### get\_IsFunctionPointer

```python
def get_IsFunctionPointer() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsUnmanagedFunctionPointer"></a>

#### get\_IsUnmanagedFunctionPointer

```python
def get_IsUnmanagedFunctionPointer() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_HasElementType"></a>

#### get\_HasElementType

```python
def get_HasElementType() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetElementType"></a>

#### GetElementType

```python
def GetElementType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetArrayRank"></a>

#### GetArrayRank

```python
def GetArrayRank() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetGenericTypeDefinition"></a>

#### GetGenericTypeDefinition

```python
def GetGenericTypeDefinition() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_GenericTypeArguments"></a>

#### get\_GenericTypeArguments

```python
def get_GenericTypeArguments() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetGenericArguments"></a>

#### GetGenericArguments

```python
def GetGenericArguments() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetOptionalCustomModifiers"></a>

#### GetOptionalCustomModifiers

```python
def GetOptionalCustomModifiers() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetRequiredCustomModifiers"></a>

#### GetRequiredCustomModifiers

```python
def GetRequiredCustomModifiers() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_GenericParameterPosition"></a>

#### get\_GenericParameterPosition

```python
def get_GenericParameterPosition() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetGenericParameterConstraints"></a>

#### GetGenericParameterConstraints

```python
def GetGenericParameterConstraints() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAbstract"></a>

#### get\_IsAbstract

```python
def get_IsAbstract() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsImport"></a>

#### get\_IsImport

```python
def get_IsImport() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSealed"></a>

#### get\_IsSealed

```python
def get_IsSealed() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSpecialName"></a>

#### get\_IsSpecialName

```python
def get_IsSpecialName() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsClass"></a>

#### get\_IsClass

```python
def get_IsClass() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedAssembly"></a>

#### get\_IsNestedAssembly

```python
def get_IsNestedAssembly() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedFamANDAssem"></a>

#### get\_IsNestedFamANDAssem

```python
def get_IsNestedFamANDAssem() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedFamily"></a>

#### get\_IsNestedFamily

```python
def get_IsNestedFamily() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedFamORAssem"></a>

#### get\_IsNestedFamORAssem

```python
def get_IsNestedFamORAssem() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedPrivate"></a>

#### get\_IsNestedPrivate

```python
def get_IsNestedPrivate() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNestedPublic"></a>

#### get\_IsNestedPublic

```python
def get_IsNestedPublic() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsNotPublic"></a>

#### get\_IsNotPublic

```python
def get_IsNotPublic() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsPublic"></a>

#### get\_IsPublic

```python
def get_IsPublic() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAutoLayout"></a>

#### get\_IsAutoLayout

```python
def get_IsAutoLayout() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsExplicitLayout"></a>

#### get\_IsExplicitLayout

```python
def get_IsExplicitLayout() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsLayoutSequential"></a>

#### get\_IsLayoutSequential

```python
def get_IsLayoutSequential() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAnsiClass"></a>

#### get\_IsAnsiClass

```python
def get_IsAnsiClass() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsAutoClass"></a>

#### get\_IsAutoClass

```python
def get_IsAutoClass() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsUnicodeClass"></a>

#### get\_IsUnicodeClass

```python
def get_IsUnicodeClass() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsCOMObject"></a>

#### get\_IsCOMObject

```python
def get_IsCOMObject() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsContextful"></a>

#### get\_IsContextful

```python
def get_IsContextful() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsEnum"></a>

#### get\_IsEnum

```python
def get_IsEnum() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsMarshalByRef"></a>

#### get\_IsMarshalByRef

```python
def get_IsMarshalByRef() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsPrimitive"></a>

#### get\_IsPrimitive

```python
def get_IsPrimitive() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsValueType"></a>

#### get\_IsValueType

```python
def get_IsValueType() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsAssignableTo"></a>

#### IsAssignableTo

```python
def IsAssignableTo(targetType: c_void_p) -> bool
```

Parameters
----------

targetType: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSignatureType"></a>

#### get\_IsSignatureType

```python
def get_IsSignatureType() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSecurityCritical"></a>

#### get\_IsSecurityCritical

```python
def get_IsSecurityCritical() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSecuritySafeCritical"></a>

#### get\_IsSecuritySafeCritical

```python
def get_IsSecuritySafeCritical() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSecurityTransparent"></a>

#### get\_IsSecurityTransparent

```python
def get_IsSecurityTransparent() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetFunctionPointerCallingConventions"></a>

#### GetFunctionPointerCallingConventions

```python
def GetFunctionPointerCallingConventions() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetFunctionPointerReturnType"></a>

#### GetFunctionPointerReturnType

```python
def GetFunctionPointerReturnType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetFunctionPointerParameterTypes"></a>

#### GetFunctionPointerParameterTypes

```python
def GetFunctionPointerParameterTypes() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetNestedType"></a>

#### GetNestedType

```python
def GetNestedType(name: str) -> c_void_p
```

Parameters
----------

name: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetNestedTypes"></a>

#### GetNestedTypes

```python
def GetNestedTypes() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeArray"></a>

#### GetTypeArray

```python
@staticmethod
def GetTypeArray(args: c_void_p) -> c_void_p
```

Parameters
----------

args: c_void_p
    GC Handle Pointer to .Net type Object[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeCode"></a>

#### GetTypeCode

```python
@staticmethod
def GetTypeCode(type: c_void_p) -> TypeCode
```

Parameters
----------

type: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

TypeCode:
    Underlying .Net type is TypeCode

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID"></a>

#### GetTypeFromProgID

```python
@staticmethod
def GetTypeFromProgID(progID: str) -> c_void_p
```

Parameters
----------

progID: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID2"></a>

#### GetTypeFromProgID2

```python
@staticmethod
def GetTypeFromProgID2(progID: str, throwOnError: bool) -> c_void_p
```

Parameters
----------

progID: str
    Underlying .Net type is string

throwOnError: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID3"></a>

#### GetTypeFromProgID3

```python
@staticmethod
def GetTypeFromProgID3(progID: str, server: str) -> c_void_p
```

Parameters
----------

progID: str
    Underlying .Net type is string

server: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetTypeFromProgID4"></a>

#### GetTypeFromProgID4

```python
@staticmethod
def GetTypeFromProgID4(progID: str, server: str,
                       throwOnError: bool) -> c_void_p
```

Parameters
----------

progID: str
    Underlying .Net type is string

server: str
    Underlying .Net type is string

throwOnError: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_BaseType"></a>

#### get\_BaseType

```python
def get_BaseType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetInterface"></a>

#### GetInterface

```python
def GetInterface(name: str) -> c_void_p
```

Parameters
----------

name: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetInterface2"></a>

#### GetInterface2

```python
def GetInterface2(name: str, ignoreCase: bool) -> c_void_p
```

Parameters
----------

name: str
    Underlying .Net type is string

ignoreCase: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetInterfaces"></a>

#### GetInterfaces

```python
def GetInterfaces() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsInstanceOfType"></a>

#### IsInstanceOfType

```python
def IsInstanceOfType(o: c_void_p) -> bool
```

Parameters
----------

o: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsEquivalentTo"></a>

#### IsEquivalentTo

```python
def IsEquivalentTo(other: c_void_p) -> bool
```

Parameters
----------

other: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumUnderlyingType"></a>

#### GetEnumUnderlyingType

```python
def GetEnumUnderlyingType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumValues"></a>

#### GetEnumValues

```python
def GetEnumValues() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Array

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumValuesAsUnderlyingType"></a>

#### GetEnumValuesAsUnderlyingType

```python
def GetEnumValuesAsUnderlyingType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Array

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeArrayType"></a>

#### MakeArrayType

```python
def MakeArrayType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeArrayType2"></a>

#### MakeArrayType2

```python
def MakeArrayType2(rank: int) -> c_void_p
```

Parameters
----------

rank: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeByRefType"></a>

#### MakeByRefType

```python
def MakeByRefType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeGenericType"></a>

#### MakeGenericType

```python
def MakeGenericType(typeArguments: c_void_p) -> c_void_p
```

Parameters
----------

typeArguments: c_void_p
GC Handle Pointer to .Net type Type[]

Returns
-------

c_void_p:
GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakePointerType"></a>

#### MakePointerType

```python
def MakePointerType() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeGenericSignatureType"></a>

#### MakeGenericSignatureType

```python
@staticmethod
def MakeGenericSignatureType(genericTypeDefinition: c_void_p,
                             typeArguments: c_void_p) -> c_void_p
```

Parameters
----------

genericTypeDefinition: c_void_p
GC Handle Pointer to .Net type Type

typeArguments: c_void_p
GC Handle Pointer to .Net type Type[]

Returns
-------

c_void_p:
GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.MakeGenericMethodParameter"></a>

#### MakeGenericMethodParameter

```python
@staticmethod
def MakeGenericMethodParameter(position: int) -> c_void_p
```

Parameters
----------

position: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.Equals"></a>

#### Equals

```python
def Equals(o: c_void_p) -> bool
```

Parameters
----------

o: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.Equals2"></a>

#### Equals2

```python
def Equals2(o: c_void_p) -> bool
```

Parameters
----------

o: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.op_Equality"></a>

#### op\_Equality

```python
@staticmethod
def op_Equality(left: c_void_p, right: c_void_p) -> bool
```

Parameters
----------

left: c_void_p
    GC Handle Pointer to .Net type Type

right: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.op_Inequality"></a>

#### op\_Inequality

```python
@staticmethod
def op_Inequality(left: c_void_p, right: c_void_p) -> bool
```

Parameters
----------

left: c_void_p
    GC Handle Pointer to .Net type Type

right: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.ReflectionOnlyGetType"></a>

#### ReflectionOnlyGetType

```python
@staticmethod
def ReflectionOnlyGetType(typeName: str, throwIfNotFound: bool,
                          ignoreCase: bool) -> c_void_p
```

Parameters
----------

typeName: str
    Underlying .Net type is string

throwIfNotFound: bool
    Underlying .Net type is Boolean

ignoreCase: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsEnumDefined"></a>

#### IsEnumDefined

```python
def IsEnumDefined(value: c_void_p) -> bool
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumName"></a>

#### GetEnumName

```python
def GetEnumName(value: c_void_p) -> str
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetEnumNames"></a>

#### GetEnumNames

```python
def GetEnumNames() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type string[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsSerializable"></a>

#### get\_IsSerializable

```python
def get_IsSerializable() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_ContainsGenericParameters"></a>

#### get\_ContainsGenericParameters

```python
def get_ContainsGenericParameters() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsVisible"></a>

#### get\_IsVisible

```python
def get_IsVisible() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsSubclassOf"></a>

#### IsSubclassOf

```python
def IsSubclassOf(c: c_void_p) -> bool
```

Parameters
----------

c: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsAssignableFrom"></a>

#### IsAssignableFrom

```python
def IsAssignableFrom(c: c_void_p) -> bool
```

Parameters
----------

c: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.IsDefined"></a>

#### IsDefined

```python
def IsDefined(attributeType: c_void_p, inherit: bool) -> bool
```

Parameters
----------

attributeType: c_void_p
    GC Handle Pointer to .Net type Type

inherit: bool
    Underlying .Net type is Boolean

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetCustomAttributes"></a>

#### GetCustomAttributes

```python
def GetCustomAttributes(inherit: bool) -> c_void_p
```

Parameters
----------

inherit: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.GetCustomAttributes2"></a>

#### GetCustomAttributes2

```python
def GetCustomAttributes2(attributeType: c_void_p, inherit: bool) -> c_void_p
```

Parameters
----------

attributeType: c_void_p
    GC Handle Pointer to .Net type Type

inherit: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_IsCollectible"></a>

#### get\_IsCollectible

```python
def get_IsCollectible() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_MetadataToken"></a>

#### get\_MetadataToken

```python
def get_MetadataToken() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_EmptyTypes"></a>

#### get\_EmptyTypes

```python
@staticmethod
def get_EmptyTypes() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Type.Type.get_Missing"></a>

#### get\_Missing

```python
@staticmethod
def get_Missing() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan"></a>

## TimeSpan Objects

```python
class TimeSpan(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    Pointer to .Net type TimeSpan in memory as bytes

Returns
----------

TimeSpan:
    Instance wrapping the .net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(ticks: int) -> c_void_p
```

Parameters
----------

ticks: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(hours: int, minutes: int, seconds: int) -> c_void_p
```

Parameters
----------

hours: int
    Underlying .Net type is int

minutes: int
    Underlying .Net type is int

seconds: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor3"></a>

#### Constructor3

```python
@staticmethod
def Constructor3(days: int, hours: int, minutes: int,
                 seconds: int) -> c_void_p
```

Parameters
----------

days: int
    Underlying .Net type is int

hours: int
    Underlying .Net type is int

minutes: int
    Underlying .Net type is int

seconds: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor4"></a>

#### Constructor4

```python
@staticmethod
def Constructor4(days: int, hours: int, minutes: int, seconds: int,
                 milliseconds: int) -> c_void_p
```

Parameters
----------

days: int
    Underlying .Net type is int

hours: int
    Underlying .Net type is int

minutes: int
    Underlying .Net type is int

seconds: int
    Underlying .Net type is int

milliseconds: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Constructor5"></a>

#### Constructor5

```python
@staticmethod
def Constructor5(days: int, hours: int, minutes: int, seconds: int,
                 milliseconds: int, microseconds: int) -> c_void_p
```

Parameters
----------

days: int
    Underlying .Net type is int

hours: int
    Underlying .Net type is int

minutes: int
    Underlying .Net type is int

seconds: int
    Underlying .Net type is int

milliseconds: int
    Underlying .Net type is int

microseconds: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Ticks"></a>

#### get\_Ticks

```python
def get_Ticks() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Days"></a>

#### get\_Days

```python
def get_Days() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Hours"></a>

#### get\_Hours

```python
def get_Hours() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Milliseconds"></a>

#### get\_Milliseconds

```python
def get_Milliseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Microseconds"></a>

#### get\_Microseconds

```python
def get_Microseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Nanoseconds"></a>

#### get\_Nanoseconds

```python
def get_Nanoseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Minutes"></a>

#### get\_Minutes

```python
def get_Minutes() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Seconds"></a>

#### get\_Seconds

```python
def get_Seconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalDays"></a>

#### get\_TotalDays

```python
def get_TotalDays() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalHours"></a>

#### get\_TotalHours

```python
def get_TotalHours() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalMilliseconds"></a>

#### get\_TotalMilliseconds

```python
def get_TotalMilliseconds() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalMicroseconds"></a>

#### get\_TotalMicroseconds

```python
def get_TotalMicroseconds() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalNanoseconds"></a>

#### get\_TotalNanoseconds

```python
def get_TotalNanoseconds() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalMinutes"></a>

#### get\_TotalMinutes

```python
def get_TotalMinutes() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TotalSeconds"></a>

#### get\_TotalSeconds

```python
def get_TotalSeconds() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Add"></a>

#### Add

```python
def Add(ts: c_void_p) -> c_void_p
```

Parameters
----------

ts: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Compare"></a>

#### Compare

```python
@staticmethod
def Compare(t1: c_void_p, t2: c_void_p) -> int
```

Parameters
----------

t1: c_void_p
    GC Handle Pointer to .Net type TimeSpan

t2: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.CompareTo"></a>

#### CompareTo

```python
def CompareTo(value: c_void_p) -> int
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.CompareTo2"></a>

#### CompareTo2

```python
def CompareTo2(value: c_void_p) -> int
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromDays"></a>

#### FromDays

```python
@staticmethod
def FromDays(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Duration"></a>

#### Duration

```python
def Duration() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Equals"></a>

#### Equals

```python
def Equals(value: c_void_p) -> bool
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Equals2"></a>

#### Equals2

```python
def Equals2(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Equals3"></a>

#### Equals3

```python
@staticmethod
def Equals3(t1: c_void_p, t2: c_void_p) -> bool
```

Parameters
----------

t1: c_void_p
    GC Handle Pointer to .Net type TimeSpan

t2: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromHours"></a>

#### FromHours

```python
@staticmethod
def FromHours(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromMilliseconds"></a>

#### FromMilliseconds

```python
@staticmethod
def FromMilliseconds(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromMicroseconds"></a>

#### FromMicroseconds

```python
@staticmethod
def FromMicroseconds(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromMinutes"></a>

#### FromMinutes

```python
@staticmethod
def FromMinutes(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Negate"></a>

#### Negate

```python
def Negate() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromSeconds"></a>

#### FromSeconds

```python
@staticmethod
def FromSeconds(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Subtract"></a>

#### Subtract

```python
def Subtract(ts: c_void_p) -> c_void_p
```

Parameters
----------

ts: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Multiply"></a>

#### Multiply

```python
def Multiply(factor: float) -> c_void_p
```

Parameters
----------

factor: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Divide"></a>

#### Divide

```python
def Divide(divisor: float) -> c_void_p
```

Parameters
----------

divisor: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Divide2"></a>

#### Divide2

```python
def Divide2(ts: c_void_p) -> float
```

Parameters
----------

ts: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.FromTicks"></a>

#### FromTicks

```python
@staticmethod
def FromTicks(value: int) -> c_void_p
```

Parameters
----------

value: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Parse"></a>

#### Parse

```python
@staticmethod
def Parse(s: str) -> c_void_p
```

Parameters
----------

s: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.Parse2"></a>

#### Parse2

```python
@staticmethod
def Parse2(input: str, formatProvider: c_void_p) -> c_void_p
```

Parameters
----------

input: str
    Underlying .Net type is string

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ParseExact"></a>

#### ParseExact

```python
@staticmethod
def ParseExact(input: str, format: str, formatProvider: c_void_p) -> c_void_p
```

Parameters
----------

input: str
    Underlying .Net type is string

format: str
    Underlying .Net type is string

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ParseExact2"></a>

#### ParseExact2

```python
@staticmethod
def ParseExact2(input: str, formats: c_void_p,
                formatProvider: c_void_p) -> c_void_p
```

Parameters
----------

input: str
    Underlying .Net type is string

formats: c_void_p
    GC Handle Pointer to .Net type string[]

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParse"></a>

#### TryParse

```python
@staticmethod
def TryParse(s: str, result: c_void_p) -> bool
```

Parameters
----------

s: str
    Underlying .Net type is string

result: c_void_p
    GC Handle Pointer to .Net type TimeSpan&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParse3"></a>

#### TryParse3

```python
@staticmethod
def TryParse3(input: str, formatProvider: c_void_p, result: c_void_p) -> bool
```

Parameters
----------

input: str
    Underlying .Net type is string

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

result: c_void_p
    GC Handle Pointer to .Net type TimeSpan&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParseExact"></a>

#### TryParseExact

```python
@staticmethod
def TryParseExact(input: str, format: str, formatProvider: c_void_p,
                  result: c_void_p) -> bool
```

Parameters
----------

input: str
    Underlying .Net type is string

format: str
    Underlying .Net type is string

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

result: c_void_p
    GC Handle Pointer to .Net type TimeSpan&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.TryParseExact3"></a>

#### TryParseExact3

```python
@staticmethod
def TryParseExact3(input: str, formats: c_void_p, formatProvider: c_void_p,
                   result: c_void_p) -> bool
```

Parameters
----------

input: str
    Underlying .Net type is string

formats: c_void_p
    GC Handle Pointer to .Net type string[]

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

result: c_void_p
    GC Handle Pointer to .Net type TimeSpan&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ToString2"></a>

#### ToString2

```python
def ToString2(format: str) -> str
```

Parameters
----------

format: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.ToString3"></a>

#### ToString3

```python
def ToString3(format: str, formatProvider: c_void_p) -> str
```

Parameters
----------

format: str
    Underlying .Net type is string

formatProvider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.op_Equality"></a>

#### op\_Equality

```python
@staticmethod
def op_Equality(t1: c_void_p, t2: c_void_p) -> bool
```

Parameters
----------

t1: c_void_p
    GC Handle Pointer to .Net type TimeSpan

t2: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.op_Inequality"></a>

#### op\_Inequality

```python
@staticmethod
def op_Inequality(t1: c_void_p, t2: c_void_p) -> bool
```

Parameters
----------

t1: c_void_p
    GC Handle Pointer to .Net type TimeSpan

t2: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_Zero"></a>

#### get\_Zero

```python
@staticmethod
def get_Zero() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_MaxValue"></a>

#### get\_MaxValue

```python
@staticmethod
def get_MaxValue() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_MinValue"></a>

#### get\_MinValue

```python
@staticmethod
def get_MinValue() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_NanosecondsPerTick"></a>

#### get\_NanosecondsPerTick

```python
@staticmethod
def get_NanosecondsPerTick() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerMicrosecond"></a>

#### get\_TicksPerMicrosecond

```python
@staticmethod
def get_TicksPerMicrosecond() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerMillisecond"></a>

#### get\_TicksPerMillisecond

```python
@staticmethod
def get_TicksPerMillisecond() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerSecond"></a>

#### get\_TicksPerSecond

```python
@staticmethod
def get_TicksPerSecond() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerMinute"></a>

#### get\_TicksPerMinute

```python
@staticmethod
def get_TicksPerMinute() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerHour"></a>

#### get\_TicksPerHour

```python
@staticmethod
def get_TicksPerHour() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan.TimeSpan.get_TicksPerDay"></a>

#### get\_TicksPerDay

```python
@staticmethod
def get_TicksPerDay() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider"></a>

## IFormatProvider Objects

```python
class IFormatProvider(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
----------

IFormatProvider:
    Instance wrapping the .net type IFormatProvider

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
----------

IFormatProvider:
    Instance wrapping the .net type IFormatProvider

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.IFormatProvider.IFormatProvider.GetFormat"></a>

#### GetFormat

```python
def GetFormat(formatType: c_void_p) -> c_void_p
```

Parameters
----------

formatType: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.TypeCode"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.TypeCode

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.Enum

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum"></a>

## Enum Objects

```python
class Enum(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type Enum

Returns
----------

Enum:
    Instance wrapping the .net type Enum

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type Enum

Returns
----------

Enum:
    Instance wrapping the .net type Enum

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetName2"></a>

#### GetName2

```python
@staticmethod
def GetName2(enumType: c_void_p, value: c_void_p) -> str
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetNames"></a>

#### GetNames

```python
@staticmethod
def GetNames(enumType: c_void_p) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type string[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetUnderlyingType"></a>

#### GetUnderlyingType

```python
@staticmethod
def GetUnderlyingType(enumType: c_void_p) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Type

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetValues"></a>

#### GetValues

```python
@staticmethod
def GetValues(enumType: c_void_p) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Array

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetValuesAsUnderlyingType"></a>

#### GetValuesAsUnderlyingType

```python
@staticmethod
def GetValuesAsUnderlyingType(enumType: c_void_p) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Array

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.HasFlag"></a>

#### HasFlag

```python
def HasFlag(flag: c_void_p) -> bool
```

Parameters
----------

flag: c_void_p
    GC Handle Pointer to .Net type Enum

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.IsDefined2"></a>

#### IsDefined2

```python
@staticmethod
def IsDefined2(enumType: c_void_p, value: c_void_p) -> bool
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Parse"></a>

#### Parse

```python
@staticmethod
def Parse(enumType: c_void_p, value: str) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Parse3"></a>

#### Parse3

```python
@staticmethod
def Parse3(enumType: c_void_p, value: str, ignoreCase: bool) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: str
    Underlying .Net type is string

ignoreCase: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.TryParse"></a>

#### TryParse

```python
@staticmethod
def TryParse(enumType: c_void_p, value: str, result: c_void_p) -> bool
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: str
    Underlying .Net type is string

result: c_void_p
    GC Handle Pointer to .Net type Object&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.TryParse3"></a>

#### TryParse3

```python
@staticmethod
def TryParse3(enumType: c_void_p, value: str, ignoreCase: bool,
              result: c_void_p) -> bool
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: str
    Underlying .Net type is string

ignoreCase: bool
    Underlying .Net type is Boolean

result: c_void_p
    GC Handle Pointer to .Net type Object&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.CompareTo"></a>

#### CompareTo

```python
def CompareTo(target: c_void_p) -> int
```

Parameters
----------

target: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString2"></a>

#### ToString2

```python
def ToString2(format: str) -> str
```

Parameters
----------

format: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString3"></a>

#### ToString3

```python
def ToString3(provider: c_void_p) -> str
```

Parameters
----------

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToString4"></a>

#### ToString4

```python
def ToString4(format: str, provider: c_void_p) -> str
```

Parameters
----------

format: str
    Underlying .Net type is string

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.Format"></a>

#### Format

```python
@staticmethod
def Format(enumType: c_void_p, value: c_void_p, format: str) -> str
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: c_void_p
    GC Handle Pointer to .Net type Object

format: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.GetTypeCode"></a>

#### GetTypeCode

```python
def GetTypeCode() -> TypeCode
```

Parameters
----------

Returns
-------

TypeCode:
    Underlying .Net type is TypeCode

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject"></a>

#### ToObject

```python
@staticmethod
def ToObject(enumType: c_void_p, value: c_void_p) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject4"></a>

#### ToObject4

```python
@staticmethod
def ToObject4(enumType: c_void_p, value: int) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject5"></a>

#### ToObject5

```python
@staticmethod
def ToObject5(enumType: c_void_p, value: int) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: int
    Underlying .Net type is byte

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Enum.Enum.ToObject8"></a>

#### ToObject8

```python
@staticmethod
def ToObject8(enumType: c_void_p, value: int) -> c_void_p
```

Parameters
----------

enumType: c_void_p
    GC Handle Pointer to .Net type Type

value: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime"></a>

## DateTime Objects

```python
class DateTime(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
----------

DateTime:
    Instance wrapping the .net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
----------

DateTime:
    Instance wrapping the .net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(ticks: int) -> c_void_p
```

Parameters
----------

ticks: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor5"></a>

#### Constructor5

```python
@staticmethod
def Constructor5(year: int, month: int, day: int) -> c_void_p
```

Parameters
----------

year: int
    Underlying .Net type is int

month: int
    Underlying .Net type is int

day: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor8"></a>

#### Constructor8

```python
@staticmethod
def Constructor8(year: int, month: int, day: int, hour: int, minute: int,
                 second: int) -> c_void_p
```

Parameters
----------

year: int
    Underlying .Net type is int

month: int
    Underlying .Net type is int

day: int
    Underlying .Net type is int

hour: int
    Underlying .Net type is int

minute: int
    Underlying .Net type is int

second: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor11"></a>

#### Constructor11

```python
@staticmethod
def Constructor11(year: int, month: int, day: int, hour: int, minute: int,
                  second: int, millisecond: int) -> c_void_p
```

Parameters
----------

year: int
    Underlying .Net type is int

month: int
    Underlying .Net type is int

day: int
    Underlying .Net type is int

hour: int
    Underlying .Net type is int

minute: int
    Underlying .Net type is int

second: int
    Underlying .Net type is int

millisecond: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Constructor14"></a>

#### Constructor14

```python
@staticmethod
def Constructor14(year: int, month: int, day: int, hour: int, minute: int,
                  second: int, millisecond: int, microsecond: int) -> c_void_p
```

Parameters
----------

year: int
    Underlying .Net type is int

month: int
    Underlying .Net type is int

day: int
    Underlying .Net type is int

hour: int
    Underlying .Net type is int

minute: int
    Underlying .Net type is int

second: int
    Underlying .Net type is int

millisecond: int
    Underlying .Net type is int

microsecond: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Add"></a>

#### Add

```python
def Add(value: c_void_p) -> c_void_p
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddDays"></a>

#### AddDays

```python
def AddDays(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddHours"></a>

#### AddHours

```python
def AddHours(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMilliseconds"></a>

#### AddMilliseconds

```python
def AddMilliseconds(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMicroseconds"></a>

#### AddMicroseconds

```python
def AddMicroseconds(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMinutes"></a>

#### AddMinutes

```python
def AddMinutes(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddMonths"></a>

#### AddMonths

```python
def AddMonths(months: int) -> c_void_p
```

Parameters
----------

months: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddSeconds"></a>

#### AddSeconds

```python
def AddSeconds(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddTicks"></a>

#### AddTicks

```python
def AddTicks(value: int) -> c_void_p
```

Parameters
----------

value: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.AddYears"></a>

#### AddYears

```python
def AddYears(value: int) -> c_void_p
```

Parameters
----------

value: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Compare"></a>

#### Compare

```python
@staticmethod
def Compare(t1: c_void_p, t2: c_void_p) -> int
```

Parameters
----------

t1: c_void_p
    GC Handle Pointer to .Net type DateTime

t2: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.CompareTo"></a>

#### CompareTo

```python
def CompareTo(value: c_void_p) -> int
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.CompareTo2"></a>

#### CompareTo2

```python
def CompareTo2(value: c_void_p) -> int
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.DaysInMonth"></a>

#### DaysInMonth

```python
@staticmethod
def DaysInMonth(year: int, month: int) -> int
```

Parameters
----------

year: int
    Underlying .Net type is int

month: int
    Underlying .Net type is int

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Equals"></a>

#### Equals

```python
def Equals(value: c_void_p) -> bool
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Equals2"></a>

#### Equals2

```python
def Equals2(value: c_void_p) -> bool
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Equals3"></a>

#### Equals3

```python
@staticmethod
def Equals3(t1: c_void_p, t2: c_void_p) -> bool
```

Parameters
----------

t1: c_void_p
    GC Handle Pointer to .Net type DateTime

t2: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromBinary"></a>

#### FromBinary

```python
@staticmethod
def FromBinary(dateData: int) -> c_void_p
```

Parameters
----------

dateData: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromFileTime"></a>

#### FromFileTime

```python
@staticmethod
def FromFileTime(fileTime: int) -> c_void_p
```

Parameters
----------

fileTime: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromFileTimeUtc"></a>

#### FromFileTimeUtc

```python
@staticmethod
def FromFileTimeUtc(fileTime: int) -> c_void_p
```

Parameters
----------

fileTime: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.FromOADate"></a>

#### FromOADate

```python
@staticmethod
def FromOADate(d: float) -> c_void_p
```

Parameters
----------

d: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.IsDaylightSavingTime"></a>

#### IsDaylightSavingTime

```python
def IsDaylightSavingTime() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToBinary"></a>

#### ToBinary

```python
def ToBinary() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Date"></a>

#### get\_Date

```python
def get_Date() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Day"></a>

#### get\_Day

```python
def get_Day() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_DayOfYear"></a>

#### get\_DayOfYear

```python
def get_DayOfYear() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Hour"></a>

#### get\_Hour

```python
def get_Hour() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Millisecond"></a>

#### get\_Millisecond

```python
def get_Millisecond() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Microsecond"></a>

#### get\_Microsecond

```python
def get_Microsecond() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Nanosecond"></a>

#### get\_Nanosecond

```python
def get_Nanosecond() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Minute"></a>

#### get\_Minute

```python
def get_Minute() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Month"></a>

#### get\_Month

```python
def get_Month() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Now"></a>

#### get\_Now

```python
@staticmethod
def get_Now() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Second"></a>

#### get\_Second

```python
def get_Second() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Ticks"></a>

#### get\_Ticks

```python
def get_Ticks() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_TimeOfDay"></a>

#### get\_TimeOfDay

```python
def get_TimeOfDay() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Today"></a>

#### get\_Today

```python
@staticmethod
def get_Today() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_Year"></a>

#### get\_Year

```python
def get_Year() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.IsLeapYear"></a>

#### IsLeapYear

```python
@staticmethod
def IsLeapYear(year: int) -> bool
```

Parameters
----------

year: int
    Underlying .Net type is int

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Parse"></a>

#### Parse

```python
@staticmethod
def Parse(s: str) -> c_void_p
```

Parameters
----------

s: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Parse2"></a>

#### Parse2

```python
@staticmethod
def Parse2(s: str, provider: c_void_p) -> c_void_p
```

Parameters
----------

s: str
    Underlying .Net type is string

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ParseExact"></a>

#### ParseExact

```python
@staticmethod
def ParseExact(s: str, format: str, provider: c_void_p) -> c_void_p
```

Parameters
----------

s: str
    Underlying .Net type is string

format: str
    Underlying .Net type is string

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Subtract"></a>

#### Subtract

```python
def Subtract(value: c_void_p) -> c_void_p
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Subtract2"></a>

#### Subtract2

```python
def Subtract2(value: c_void_p) -> c_void_p
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToOADate"></a>

#### ToOADate

```python
def ToOADate() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToFileTime"></a>

#### ToFileTime

```python
def ToFileTime() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToFileTimeUtc"></a>

#### ToFileTimeUtc

```python
def ToFileTimeUtc() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToLocalTime"></a>

#### ToLocalTime

```python
def ToLocalTime() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToLongDateString"></a>

#### ToLongDateString

```python
def ToLongDateString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToLongTimeString"></a>

#### ToLongTimeString

```python
def ToLongTimeString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToShortDateString"></a>

#### ToShortDateString

```python
def ToShortDateString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToShortTimeString"></a>

#### ToShortTimeString

```python
def ToShortTimeString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString2"></a>

#### ToString2

```python
def ToString2(format: str) -> str
```

Parameters
----------

format: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString3"></a>

#### ToString3

```python
def ToString3(provider: c_void_p) -> str
```

Parameters
----------

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToString4"></a>

#### ToString4

```python
def ToString4(format: str, provider: c_void_p) -> str
```

Parameters
----------

format: str
    Underlying .Net type is string

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.ToUniversalTime"></a>

#### ToUniversalTime

```python
def ToUniversalTime() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.TryParse"></a>

#### TryParse

```python
@staticmethod
def TryParse(s: str, result: c_void_p) -> bool
```

Parameters
----------

s: str
    Underlying .Net type is string

result: c_void_p
    GC Handle Pointer to .Net type DateTime&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.op_Equality"></a>

#### op\_Equality

```python
@staticmethod
def op_Equality(d1: c_void_p, d2: c_void_p) -> bool
```

Parameters
----------

d1: c_void_p
    GC Handle Pointer to .Net type DateTime

d2: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.op_Inequality"></a>

#### op\_Inequality

```python
@staticmethod
def op_Inequality(d1: c_void_p, d2: c_void_p) -> bool
```

Parameters
----------

d1: c_void_p
    GC Handle Pointer to .Net type DateTime

d2: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.Deconstruct2"></a>

#### Deconstruct2

```python
def Deconstruct2(year: c_void_p, month: c_void_p, day: c_void_p) -> None
```

Parameters
----------

year: c_void_p
    GC Handle Pointer to .Net type Int32&

month: c_void_p
    GC Handle Pointer to .Net type Int32&

day: c_void_p
    GC Handle Pointer to .Net type Int32&

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetDateTimeFormats"></a>

#### GetDateTimeFormats

```python
def GetDateTimeFormats() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type string[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetDateTimeFormats2"></a>

#### GetDateTimeFormats2

```python
def GetDateTimeFormats2(provider: c_void_p) -> c_void_p
```

Parameters
----------

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type string[]

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.GetTypeCode"></a>

#### GetTypeCode

```python
def GetTypeCode() -> TypeCode
```

Parameters
----------

Returns
-------

TypeCode:
    Underlying .Net type is TypeCode

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.TryParse5"></a>

#### TryParse5

```python
@staticmethod
def TryParse5(s: str, provider: c_void_p, result: c_void_p) -> bool
```

Parameters
----------

s: str
    Underlying .Net type is string

provider: c_void_p
    GC Handle Pointer to .Net type IFormatProvider

result: c_void_p
    GC Handle Pointer to .Net type DateTime&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_UtcNow"></a>

#### get\_UtcNow

```python
@staticmethod
def get_UtcNow() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_MinValue"></a>

#### get\_MinValue

```python
@staticmethod
def get_MinValue() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_MaxValue"></a>

#### get\_MaxValue

```python
@staticmethod
def get_MaxValue() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime.DateTime.get_UnixEpoch"></a>

#### get\_UnixEpoch

```python
@staticmethod
def get_UnixEpoch() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken"></a>

## CancellationToken Objects

```python
class CancellationToken(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
----------

CancellationToken:
    Instance wrapping the .net type CancellationToken

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
----------

CancellationToken:
    Instance wrapping the .net type CancellationToken

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(canceled: bool) -> c_void_p
```

Parameters
----------

canceled: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationToken

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.get_None"></a>

#### get\_None

```python
@staticmethod
def get_None() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationToken

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.get_IsCancellationRequested"></a>

#### get\_IsCancellationRequested

```python
def get_IsCancellationRequested() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.get_CanBeCanceled"></a>

#### get\_CanBeCanceled

```python
def get_CanBeCanceled() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.Equals"></a>

#### Equals

```python
def Equals(other: c_void_p) -> bool
```

Parameters
----------

other: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.Equals2"></a>

#### Equals2

```python
def Equals2(other: c_void_p) -> bool
```

Parameters
----------

other: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.op_Equality"></a>

#### op\_Equality

```python
@staticmethod
def op_Equality(left: c_void_p, right: c_void_p) -> bool
```

Parameters
----------

left: c_void_p
    GC Handle Pointer to .Net type CancellationToken

right: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.op_Inequality"></a>

#### op\_Inequality

```python
@staticmethod
def op_Inequality(left: c_void_p, right: c_void_p) -> bool
```

Parameters
----------

left: c_void_p
    GC Handle Pointer to .Net type CancellationToken

right: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.ThrowIfCancellationRequested"></a>

#### ThrowIfCancellationRequested

```python
def ThrowIfCancellationRequested() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken.CancellationToken.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource"></a>

# quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource"></a>

## CancellationTokenSource Objects

```python
class CancellationTokenSource(object)
```

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type CancellationTokenSource

Returns
----------

CancellationTokenSource:
    Instance wrapping the .net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type CancellationTokenSource

Returns
----------

CancellationTokenSource:
    Instance wrapping the .net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(delay: c_void_p) -> c_void_p
```

Parameters
----------

delay: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Constructor3"></a>

#### Constructor3

```python
@staticmethod
def Constructor3(millisecondsDelay: int) -> c_void_p
```

Parameters
----------

millisecondsDelay: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.get_IsCancellationRequested"></a>

#### get\_IsCancellationRequested

```python
def get_IsCancellationRequested() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.get_Token"></a>

#### get\_Token

```python
def get_Token() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationToken

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Cancel"></a>

#### Cancel

```python
def Cancel() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Cancel2"></a>

#### Cancel2

```python
def Cancel2(throwOnFirstException: bool) -> None
```

Parameters
----------

throwOnFirstException: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CancelAfter"></a>

#### CancelAfter

```python
def CancelAfter(delay: c_void_p) -> None
```

Parameters
----------

delay: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CancelAfter2"></a>

#### CancelAfter2

```python
def CancelAfter2(millisecondsDelay: int) -> None
```

Parameters
----------

millisecondsDelay: int
    Underlying .Net type is int

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.TryReset"></a>

#### TryReset

```python
def TryReset() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CreateLinkedTokenSource"></a>

#### CreateLinkedTokenSource

```python
@staticmethod
def CreateLinkedTokenSource(token1: c_void_p, token2: c_void_p) -> c_void_p
```

Parameters
----------

token1: c_void_p
    GC Handle Pointer to .Net type CancellationToken

token2: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CreateLinkedTokenSource2"></a>

#### CreateLinkedTokenSource2

```python
@staticmethod
def CreateLinkedTokenSource2(token: c_void_p) -> c_void_p
```

Parameters
----------

token: c_void_p
    GC Handle Pointer to .Net type CancellationToken

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationTokenSource

<a id="quixstreams.native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource.CancellationTokenSource.CreateLinkedTokenSource3"></a>

#### CreateLinkedTokenSource3

```python
@staticmethod
def CreateLinkedTokenSource3(tokens: c_void_p) -> c_void_p
```

Parameters
----------

tokens: c_void_p
    GC Handle Pointer to .Net type CancellationToken[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CancellationTokenSource

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions"></a>

## SecurityOptions Objects

```python
class SecurityOptions(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type SecurityOptions

Returns
----------

SecurityOptions:
    Instance wrapping the .net type SecurityOptions

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type SecurityOptions

Returns
----------

SecurityOptions:
    Instance wrapping the .net type SecurityOptions

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type SecurityOptions

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(
        sslCertificates: str,
        username: str,
        password: str,
        saslMechanism: SaslMechanism = SaslMechanism.ScramSha256) -> c_void_p
```

Parameters
----------

sslCertificates: str
    Underlying .Net type is string

username: str
    Underlying .Net type is string

password: str
    Underlying .Net type is string

saslMechanism: SaslMechanism
    (Optional) Underlying .Net type is SaslMechanism. Defaults to ScramSha256

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type SecurityOptions

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_SaslMechanism"></a>

#### get\_SaslMechanism

```python
def get_SaslMechanism() -> Optional[SaslMechanism]
```

Parameters
----------

Returns
-------

Optional[SaslMechanism]:
    Underlying .Net type is SaslMechanism?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_SaslMechanism"></a>

#### set\_SaslMechanism

```python
def set_SaslMechanism(value: Optional[SaslMechanism]) -> None
```

Parameters
----------

value: Optional[SaslMechanism]
    Underlying .Net type is SaslMechanism?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_Username"></a>

#### get\_Username

```python
def get_Username() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_Username"></a>

#### set\_Username

```python
def set_Username(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_Password"></a>

#### get\_Password

```python
def get_Password() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_Password"></a>

#### set\_Password

```python
def set_Password(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_SslCertificates"></a>

#### get\_SslCertificates

```python
def get_SslCertificates() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_SslCertificates"></a>

#### set\_SslCertificates

```python
def set_SslCertificates(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_UseSsl"></a>

#### get\_UseSsl

```python
def get_UseSsl() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_UseSsl"></a>

#### set\_UseSsl

```python
def set_UseSsl(value: bool) -> None
```

Parameters
----------

value: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.get_UseSasl"></a>

#### get\_UseSasl

```python
def get_UseSasl() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SecurityOptions.SecurityOptions.set_UseSasl"></a>

#### set\_UseSasl

```python
def set_UseSasl(value: bool) -> None
```

Parameters
----------

value: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Configuration.SaslMechanism"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Configuration.SaslMechanism

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions"></a>

# quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions.QuixStreamingClientExtensions"></a>

## QuixStreamingClientExtensions Objects

```python
class QuixStreamingClientExtensions(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClientExtensions.QuixStreamingClientExtensions.GetTopicConsumer"></a>

#### GetTopicConsumer

```python
@staticmethod
def GetTopicConsumer(
        client: c_void_p,
        topicId: str,
        consumerGroup: str = None,
        commitMode: CommitMode = CommitMode.Automatic,
        autoOffset: AutoOffsetReset = AutoOffsetReset.Latest) -> c_void_p
```

Parameters
----------

client: c_void_p
    GC Handle Pointer to .Net type QuixStreamingClient

topicId: str
    Underlying .Net type is string

consumerGroup: str
    (Optional) Underlying .Net type is string. Defaults to None

commitMode: CommitMode
    (Optional) Underlying .Net type is CommitMode. Defaults to Automatic

autoOffset: AutoOffsetReset
    (Optional) Underlying .Net type is AutoOffsetReset. Defaults to Latest

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs"></a>

## PackageReceivedEventArgs Objects

```python
class PackageReceivedEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type PackageReceivedEventArgs

Returns
----------

PackageReceivedEventArgs:
Instance wrapping the .net type PackageReceivedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type PackageReceivedEventArgs

Returns
----------

PackageReceivedEventArgs:
Instance wrapping the .net type PackageReceivedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topicConsumer: c_void_p, consumer: c_void_p,
                package: c_void_p) -> c_void_p
```

Parameters
----------

topicConsumer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

consumer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

package: c_void_p
    GC Handle Pointer to .Net type StreamPackage

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type PackageReceivedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.get_TopicConsumer"></a>

#### get\_TopicConsumer

```python
def get_TopicConsumer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.PackageReceivedEventArgs.PackageReceivedEventArgs.get_Package"></a>

#### get\_Package

```python
def get_Package() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamPackage

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.TopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer"></a>

## TopicProducer Objects

```python
class TopicProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TopicProducer

Returns
----------

TopicProducer:
    Instance wrapping the .net type TopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TopicProducer

Returns
----------

TopicProducer:
    Instance wrapping the .net type TopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(createKafkaProducer: Callable[[str], c_void_p]) -> c_void_p
```

Parameters
----------

createKafkaProducer: Callable[[str], c_void_p]
    Underlying .Net type is Func<string, TelemetryKafkaProducer>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(config: c_void_p, topic: str) -> c_void_p
```

Parameters
----------

config: c_void_p
    GC Handle Pointer to .Net type KafkaProducerConfiguration

topic: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.CreateStream"></a>

#### CreateStream

```python
def CreateStream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.CreateStream2"></a>

#### CreateStream2

```python
def CreateStream2(streamId: str) -> c_void_p
```

Parameters
----------

streamId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.GetStream"></a>

#### GetStream

```python
def GetStream(streamId: str) -> c_void_p
```

Parameters
----------

streamId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.GetOrCreateStream"></a>

#### GetOrCreateStream

```python
def GetOrCreateStream(
        streamId: str,
        onStreamCreated: Callable[[c_void_p], None] = None) -> c_void_p
```

Parameters
----------

streamId: str
    Underlying .Net type is string

onStreamCreated: Callable[[c_void_p], None]
    (Optional) Underlying .Net type is Action<IStreamProducer>. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.RemoveStream"></a>

#### RemoveStream

```python
def RemoveStream(streamId: str) -> None
```

Parameters
----------

streamId: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicProducer.TopicProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer"></a>

## IStreamConsumer Objects

```python
class IStreamConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

Returns
----------

IStreamConsumer:
    Instance wrapping the .net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

Returns
----------

IStreamConsumer:
    Instance wrapping the .net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_StreamId"></a>

#### get\_StreamId

```python
def get_StreamId() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_Properties"></a>

#### get\_Properties

```python
def get_Properties() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamPropertiesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_Timeseries"></a>

#### get\_Timeseries

```python
def get_Timeseries() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamTimeseriesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.get_Events"></a>

#### get\_Events

```python
def get_Events() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamEventsConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.add_OnPackageReceived"></a>

#### add\_OnPackageReceived

```python
def add_OnPackageReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<PackageReceivedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.remove_OnPackageReceived"></a>

#### remove\_OnPackageReceived

```python
def remove_OnPackageReceived(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<PackageReceivedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.add_OnStreamClosed"></a>

#### add\_OnStreamClosed

```python
def add_OnStreamClosed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<StreamClosedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamConsumer.IStreamConsumer.remove_OnStreamClosed"></a>

#### remove\_OnStreamClosed

```python
def remove_OnStreamClosed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<StreamClosedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs"></a>

## StreamClosedEventArgs Objects

```python
class StreamClosedEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type StreamClosedEventArgs

Returns
----------

StreamClosedEventArgs:
Instance wrapping the .net type StreamClosedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type StreamClosedEventArgs

Returns
----------

StreamClosedEventArgs:
Instance wrapping the .net type StreamClosedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topicConsumer: c_void_p, consumer: c_void_p,
                endType: StreamEndType) -> c_void_p
```

Parameters
----------

topicConsumer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

consumer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

endType: StreamEndType
    Underlying .Net type is StreamEndType

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamClosedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.get_TopicConsumer"></a>

#### get\_TopicConsumer

```python
def get_TopicConsumer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.StreamClosedEventArgs.StreamClosedEventArgs.get_EndType"></a>

#### get\_EndType

```python
def get_EndType() -> StreamEndType
```

Parameters
----------

Returns
-------

StreamEndType:
    Underlying .Net type is StreamEndType

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags"></a>

## TimeseriesDataTimestampTags Objects

```python
class TimeseriesDataTimestampTags(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestampTags

Returns
----------

TimeseriesDataTimestampTags:
    Instance wrapping the .net type TimeseriesDataTimestampTags

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestampTags

Returns
----------

TimeseriesDataTimestampTags:
    Instance wrapping the .net type TimeseriesDataTimestampTags

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.GetEnumerator"></a>

#### GetEnumerator

```python
def GetEnumerator() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerator<KeyValuePair<string, string>>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Item"></a>

#### get\_Item

```python
def get_Item(key: str) -> str
```

Parameters
----------

key: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Keys"></a>

#### get\_Keys

```python
def get_Keys() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerable<string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Values"></a>

#### get\_Values

```python
def get_Values() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerable<string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.get_Count"></a>

#### get\_Count

```python
def get_Count() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.ContainsKey"></a>

#### ContainsKey

```python
def ContainsKey(key: str) -> bool
```

Parameters
----------

key: str
    Underlying .Net type is string

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.TryGetValue"></a>

#### TryGetValue

```python
def TryGetValue(key: str, value: c_void_p) -> bool
```

Parameters
----------

key: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type String&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestampTags.TimeseriesDataTimestampTags.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils.QuixUtils"></a>

## QuixUtils Objects

```python
class QuixUtils(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.QuixUtils.QuixUtils.TryGetWorkspaceIdPrefix"></a>

#### TryGetWorkspaceIdPrefix

```python
@staticmethod
def TryGetWorkspaceIdPrefix(topicId: str, workspaceId: c_void_p) -> bool
```

Parameters
----------

topicId: str
    Underlying .Net type is string

workspaceId: c_void_p
    GC Handle Pointer to .Net type String&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps"></a>

## TimeseriesDataTimestamps Objects

```python
class TimeseriesDataTimestamps(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestamps

Returns
----------

TimeseriesDataTimestamps:
    Instance wrapping the .net type TimeseriesDataTimestamps

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestamps

Returns
----------

TimeseriesDataTimestamps:
    Instance wrapping the .net type TimeseriesDataTimestamps

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.GetEnumerator"></a>

#### GetEnumerator

```python
def GetEnumerator() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerator<TimeseriesDataTimestamp>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.get_Item"></a>

#### get\_Item

```python
def get_Item(index: int) -> c_void_p
```

Parameters
----------

index: int
    Underlying .Net type is int

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.get_Count"></a>

#### get\_Count

```python
def get_Count() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Utils.TimeseriesDataTimestamps.TimeseriesDataTimestamps.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder"></a>

## EventDefinitionBuilder Objects

```python
class EventDefinitionBuilder(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDefinitionBuilder

Returns
----------

EventDefinitionBuilder:
    Instance wrapping the .net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDefinitionBuilder

Returns
----------

EventDefinitionBuilder:
    Instance wrapping the .net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(streamEventsProducer: c_void_p,
                location: str,
                properties: c_void_p = None) -> c_void_p
```

Parameters
----------

streamEventsProducer: c_void_p
    GC Handle Pointer to .Net type StreamEventsProducer

location: str
    Underlying .Net type is string

properties: c_void_p
    (Optional) GC Handle Pointer to .Net type EventDefinition. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.SetLevel"></a>

#### SetLevel

```python
def SetLevel(level: EventLevel) -> c_void_p
```

Parameters
----------

level: EventLevel
    Underlying .Net type is EventLevel

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.SetCustomProperties"></a>

#### SetCustomProperties

```python
def SetCustomProperties(customProperties: str) -> c_void_p
```

Parameters
----------

customProperties: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder.AddDefinition"></a>

#### AddDefinition

```python
def AddDefinition(eventId: str,
                  name: str = None,
                  description: str = None) -> c_void_p
```

Parameters
----------

eventId: str
    Underlying .Net type is string

name: str
    (Optional) Underlying .Net type is string. Defaults to None

description: str
    (Optional) Underlying .Net type is string. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer"></a>

## StreamEventsProducer Objects

```python
class StreamEventsProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamEventsProducer

Returns
----------

StreamEventsProducer:
    Instance wrapping the .net type StreamEventsProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamEventsProducer

Returns
----------

StreamEventsProducer:
    Instance wrapping the .net type StreamEventsProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.get_DefaultTags"></a>

#### get\_DefaultTags

```python
def get_DefaultTags() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.set_DefaultTags"></a>

#### set\_DefaultTags

```python
def set_DefaultTags(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.get_DefaultLocation"></a>

#### get\_DefaultLocation

```python
def get_DefaultLocation() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.set_DefaultLocation"></a>

#### set\_DefaultLocation

```python
def set_DefaultLocation(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.get_Epoch"></a>

#### get\_Epoch

```python
def get_Epoch() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.set_Epoch"></a>

#### set\_Epoch

```python
def set_Epoch(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestamp"></a>

#### AddTimestamp

```python
def AddTimestamp(dateTime: c_void_p) -> c_void_p
```

Parameters
----------

dateTime: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestamp2"></a>

#### AddTimestamp2

```python
def AddTimestamp2(timeSpan: c_void_p) -> c_void_p
```

Parameters
----------

timeSpan: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestampMilliseconds"></a>

#### AddTimestampMilliseconds

```python
def AddTimestampMilliseconds(timeMilliseconds: int) -> c_void_p
```

Parameters
----------

timeMilliseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddTimestampNanoseconds"></a>

#### AddTimestampNanoseconds

```python
def AddTimestampNanoseconds(timeNanoseconds: int) -> c_void_p
```

Parameters
----------

timeNanoseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddDefinitions"></a>

#### AddDefinitions

```python
def AddDefinitions(definitions: c_void_p) -> None
```

Parameters
----------

definitions: c_void_p
    GC Handle Pointer to .Net type List<EventDefinition>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddDefinition"></a>

#### AddDefinition

```python
def AddDefinition(eventId: str,
                  name: str = None,
                  description: str = None) -> c_void_p
```

Parameters
----------

eventId: str
    Underlying .Net type is string

name: str
    (Optional) Underlying .Net type is string. Defaults to None

description: str
    (Optional) Underlying .Net type is string. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.AddLocation"></a>

#### AddLocation

```python
def AddLocation(location: str) -> c_void_p
```

Parameters
----------

location: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Flush"></a>

#### Flush

```python
def Flush() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Publish"></a>

#### Publish

```python
def Publish(data: c_void_p) -> None
```

Parameters
----------

data: c_void_p
    GC Handle Pointer to .Net type EventData

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Publish2"></a>

#### Publish2

```python
def Publish2(events: c_void_p) -> None
```

Parameters
----------

events: c_void_p
    GC Handle Pointer to .Net type ICollection<EventData>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamEventsProducer.StreamEventsProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder"></a>

## TimeseriesDataBuilder Objects

```python
class TimeseriesDataBuilder(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataBuilder

Returns
----------

TimeseriesDataBuilder:
    Instance wrapping the .net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataBuilder

Returns
----------

TimeseriesDataBuilder:
    Instance wrapping the .net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(buffer: c_void_p, data: c_void_p,
                timestamp: c_void_p) -> c_void_p
```

Parameters
----------

buffer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferProducer

data: c_void_p
    GC Handle Pointer to .Net type TimeseriesData

timestamp: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddValue"></a>

#### AddValue

```python
def AddValue(parameterId: str, value: float) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddValue2"></a>

#### AddValue2

```python
def AddValue2(parameterId: str, value: str) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddValue3"></a>

#### AddValue3

```python
def AddValue3(parameterId: str, value: c_void_p) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddTag"></a>

#### AddTag

```python
def AddTag(tagId: str, value: str) -> c_void_p
```

Parameters
----------

tagId: str
    Underlying .Net type is string

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.AddTags"></a>

#### AddTags

```python
def AddTags(tags: c_void_p) -> c_void_p
```

Parameters
----------

tags: c_void_p
    GC Handle Pointer to .Net type IEnumerable<KeyValuePair<string, string>>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder.TimeseriesDataBuilder.Publish"></a>

#### Publish

```python
def Publish() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer"></a>

## StreamTimeseriesProducer Objects

```python
class StreamTimeseriesProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamTimeseriesProducer

Returns
----------

StreamTimeseriesProducer:
    Instance wrapping the .net type StreamTimeseriesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamTimeseriesProducer

Returns
----------

StreamTimeseriesProducer:
    Instance wrapping the .net type StreamTimeseriesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.get_Buffer"></a>

#### get\_Buffer

```python
def get_Buffer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesBufferProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Publish"></a>

#### Publish

```python
def Publish(data: c_void_p) -> None
```

Parameters
----------

data: c_void_p
    GC Handle Pointer to .Net type TimeseriesData

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Publish2"></a>

#### Publish2

```python
def Publish2(data: c_void_p) -> None
```

Parameters
----------

data: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataRaw

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.get_DefaultLocation"></a>

#### get\_DefaultLocation

```python
def get_DefaultLocation() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.set_DefaultLocation"></a>

#### set\_DefaultLocation

```python
def set_DefaultLocation(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.AddDefinitions"></a>

#### AddDefinitions

```python
def AddDefinitions(definitions: c_void_p) -> None
```

Parameters
----------

definitions: c_void_p
    GC Handle Pointer to .Net type List<ParameterDefinition>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.AddDefinition"></a>

#### AddDefinition

```python
def AddDefinition(parameterId: str,
                  name: str = None,
                  description: str = None) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

name: str
    (Optional) Underlying .Net type is string. Defaults to None

description: str
    (Optional) Underlying .Net type is string. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.AddLocation"></a>

#### AddLocation

```python
def AddLocation(location: str) -> c_void_p
```

Parameters
----------

location: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Flush"></a>

#### Flush

```python
def Flush() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamTimeseriesProducer.StreamTimeseriesProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer"></a>

## StreamPropertiesProducer Objects

```python
class StreamPropertiesProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamPropertiesProducer

Returns
----------

StreamPropertiesProducer:
    Instance wrapping the .net type StreamPropertiesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamPropertiesProducer

Returns
----------

StreamPropertiesProducer:
    Instance wrapping the .net type StreamPropertiesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_FlushInterval"></a>

#### get\_FlushInterval

```python
def get_FlushInterval() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_FlushInterval"></a>

#### set\_FlushInterval

```python
def set_FlushInterval(value: int) -> None
```

Parameters
----------

value: int
    Underlying .Net type is int

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_Name"></a>

#### set\_Name

```python
def set_Name(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Location"></a>

#### get\_Location

```python
def get_Location() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_Location"></a>

#### set\_Location

```python
def set_Location(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_TimeOfRecording"></a>

#### get\_TimeOfRecording

```python
def get_TimeOfRecording() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.set_TimeOfRecording"></a>

#### set\_TimeOfRecording

```python
def set_TimeOfRecording(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Metadata"></a>

#### get\_Metadata

```python
def get_Metadata() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ObservableDictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.get_Parents"></a>

#### get\_Parents

```python
def get_Parents() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ObservableCollection<string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.AddParent"></a>

#### AddParent

```python
def AddParent(parentStreamId: str) -> None
```

Parameters
----------

parentStreamId: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.RemoveParent"></a>

#### RemoveParent

```python
def RemoveParent(parentStreamId: str) -> None
```

Parameters
----------

parentStreamId: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.Flush"></a>

#### Flush

```python
def Flush() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.StreamPropertiesProducer.StreamPropertiesProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder"></a>

## EventDataBuilder Objects

```python
class EventDataBuilder(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDataBuilder

Returns
----------

EventDataBuilder:
    Instance wrapping the .net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDataBuilder

Returns
----------

EventDataBuilder:
    Instance wrapping the .net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(streamEventsProducer: c_void_p,
                timestampNanoseconds: int) -> c_void_p
```

Parameters
----------

streamEventsProducer: c_void_p
    GC Handle Pointer to .Net type StreamEventsProducer

timestampNanoseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.AddValue"></a>

#### AddValue

```python
def AddValue(eventId: str, value: str) -> c_void_p
```

Parameters
----------

eventId: str
    Underlying .Net type is string

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.AddTag"></a>

#### AddTag

```python
def AddTag(tagId: str, value: str) -> c_void_p
```

Parameters
----------

tagId: str
    Underlying .Net type is string

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.AddTags"></a>

#### AddTags

```python
def AddTags(tagsValues: c_void_p) -> c_void_p
```

Parameters
----------

tagsValues: c_void_p
    GC Handle Pointer to .Net type IEnumerable<KeyValuePair<string, string>>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder.Publish"></a>

#### Publish

```python
def Publish() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder"></a>

## ParameterDefinitionBuilder Objects

```python
class ParameterDefinitionBuilder(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

Returns
----------

ParameterDefinitionBuilder:
    Instance wrapping the .net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

Returns
----------

ParameterDefinitionBuilder:
    Instance wrapping the .net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(streamTimeseriesProducer: c_void_p,
                location: str,
                definition: c_void_p = None) -> c_void_p
```

Parameters
----------

streamTimeseriesProducer: c_void_p
    GC Handle Pointer to .Net type StreamTimeseriesProducer

location: str
    Underlying .Net type is string

definition: c_void_p
    (Optional) GC Handle Pointer to .Net type ParameterDefinition. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetRange"></a>

#### SetRange

```python
def SetRange(minimumValue: float, maximumValue: float) -> c_void_p
```

Parameters
----------

minimumValue: float
    Underlying .Net type is double

maximumValue: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetUnit"></a>

#### SetUnit

```python
def SetUnit(unit: str) -> c_void_p
```

Parameters
----------

unit: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetFormat"></a>

#### SetFormat

```python
def SetFormat(format: str) -> c_void_p
```

Parameters
----------

format: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.SetCustomProperties"></a>

#### SetCustomProperties

```python
def SetCustomProperties(customProperties: str) -> c_void_p
```

Parameters
----------

customProperties: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.ParameterDefinitionBuilder.ParameterDefinitionBuilder.AddDefinition"></a>

#### AddDefinition

```python
def AddDefinition(parameterId: str,
                  name: str = None,
                  description: str = None) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

name: str
    (Optional) Underlying .Net type is string. Defaults to None

description: str
    (Optional) Underlying .Net type is string. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer"></a>

## TimeseriesBufferProducer Objects

```python
class TimeseriesBufferProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferProducer

Returns
----------

TimeseriesBufferProducer:
    Instance wrapping the .net type TimeseriesBufferProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferProducer

Returns
----------

TimeseriesBufferProducer:
    Instance wrapping the .net type TimeseriesBufferProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.get_Epoch"></a>

#### get\_Epoch

```python
def get_Epoch() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.set_Epoch"></a>

#### set\_Epoch

```python
def set_Epoch(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestamp"></a>

#### AddTimestamp

```python
def AddTimestamp(dateTime: c_void_p) -> c_void_p
```

Parameters
----------

dateTime: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestamp2"></a>

#### AddTimestamp2

```python
def AddTimestamp2(timeSpan: c_void_p) -> c_void_p
```

Parameters
----------

timeSpan: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestampMilliseconds"></a>

#### AddTimestampMilliseconds

```python
def AddTimestampMilliseconds(timeMilliseconds: int) -> c_void_p
```

Parameters
----------

timeMilliseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.AddTimestampNanoseconds"></a>

#### AddTimestampNanoseconds

```python
def AddTimestampNanoseconds(timeNanoseconds: int) -> c_void_p
```

Parameters
----------

timeNanoseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataBuilder

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.Publish"></a>

#### Publish

```python
def Publish(data: c_void_p) -> None
```

Parameters
----------

data: c_void_p
    GC Handle Pointer to .Net type TimeseriesData

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.get_DefaultTags"></a>

#### get\_DefaultTags

```python
def get_DefaultTags() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.set_DefaultTags"></a>

#### set\_DefaultTags

```python
def set_DefaultTags(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Dictionary<string, string>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.Flush"></a>

#### Flush

```python
def Flush() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesBufferProducer.TimeseriesBufferProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData"></a>

## TimeseriesData Objects

```python
class TimeseriesData(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesData

Returns
----------

TimeseriesData:
    Instance wrapping the .net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesData

Returns
----------

TimeseriesData:
    Instance wrapping the .net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(capacity: int = None) -> c_void_p
```

Parameters
----------

capacity: int
    (Optional) Underlying .Net type is int. Defaults to 10

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(rawData: c_void_p,
                 parametersFilter: c_void_p = None,
                 merge: bool = True,
                 clean: bool = True) -> c_void_p
```

Parameters
----------

rawData: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataRaw

parametersFilter: c_void_p
    (Optional) GC Handle Pointer to .Net type string[]. Defaults to None

merge: bool
    (Optional) Underlying .Net type is Boolean. Defaults to True

clean: bool
    (Optional) Underlying .Net type is Boolean. Defaults to True

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Constructor3"></a>

#### Constructor3

```python
@staticmethod
def Constructor3(timestamps: c_void_p,
                 merge: bool = True,
                 clean: bool = True) -> c_void_p
```

Parameters
----------

timestamps: c_void_p
    GC Handle Pointer to .Net type List<TimeseriesDataTimestamp>

merge: bool
    (Optional) Underlying .Net type is Boolean. Defaults to True

clean: bool
    (Optional) Underlying .Net type is Boolean. Defaults to True

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Clone"></a>

#### Clone

```python
def Clone(parametersFilter: c_void_p) -> c_void_p
```

Parameters
----------

parametersFilter: c_void_p
    GC Handle Pointer to .Net type string[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.get_Timestamps"></a>

#### get\_Timestamps

```python
def get_Timestamps() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamps

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestamp"></a>

#### AddTimestamp

```python
def AddTimestamp(dateTime: c_void_p) -> c_void_p
```

Parameters
----------

dateTime: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestamp2"></a>

#### AddTimestamp2

```python
def AddTimestamp2(timeSpan: c_void_p) -> c_void_p
```

Parameters
----------

timeSpan: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestampMilliseconds"></a>

#### AddTimestampMilliseconds

```python
def AddTimestampMilliseconds(timeMilliseconds: int) -> c_void_p
```

Parameters
----------

timeMilliseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.AddTimestampNanoseconds"></a>

#### AddTimestampNanoseconds

```python
def AddTimestampNanoseconds(timeNanoseconds: int) -> c_void_p
```

Parameters
----------

timeNanoseconds: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesData.TimeseriesData.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue"></a>

## ParameterValue Objects

```python
class ParameterValue(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterValue

Returns
----------

ParameterValue:
    Instance wrapping the .net type ParameterValue

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterValue

Returns
----------

ParameterValue:
    Instance wrapping the .net type ParameterValue

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_ParameterId"></a>

#### get\_ParameterId

```python
def get_ParameterId() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_NumericValue"></a>

#### get\_NumericValue

```python
def get_NumericValue() -> Optional[float]
```

Parameters
----------

Returns
-------

Optional[float]:
    Underlying .Net type is double?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.set_NumericValue"></a>

#### set\_NumericValue

```python
def set_NumericValue(value: Optional[float]) -> None
```

Parameters
----------

value: Optional[float]
    Underlying .Net type is double?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_StringValue"></a>

#### get\_StringValue

```python
def get_StringValue() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.set_StringValue"></a>

#### set\_StringValue

```python
def set_StringValue(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_BinaryValue"></a>

#### get\_BinaryValue

```python
def get_BinaryValue() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type byte[]

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.set_BinaryValue"></a>

#### set\_BinaryValue

```python
def set_BinaryValue(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_Value"></a>

#### get\_Value

```python
def get_Value() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.op_Equality"></a>

#### op\_Equality

```python
@staticmethod
def op_Equality(lhs: c_void_p, rhs: c_void_p) -> bool
```

Parameters
----------

lhs: c_void_p
    GC Handle Pointer to .Net type ParameterValue

rhs: c_void_p
    GC Handle Pointer to .Net type ParameterValue

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.op_Inequality"></a>

#### op\_Inequality

```python
@staticmethod
def op_Inequality(lhs: c_void_p, rhs: c_void_p) -> bool
```

Parameters
----------

lhs: c_void_p
    GC Handle Pointer to .Net type ParameterValue

rhs: c_void_p
    GC Handle Pointer to .Net type ParameterValue

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValue.ParameterValue.get_Type"></a>

#### get\_Type

```python
def get_Type() -> ParameterValueType
```

Parameters
----------

Returns
-------

ParameterValueType:
    Underlying .Net type is ParameterValueType

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData"></a>

## EventData Objects

```python
class EventData(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventData

Returns
----------

EventData:
    Instance wrapping the .net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventData

Returns
----------

EventData:
    Instance wrapping the .net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(eventId: str, timestampNanoseconds: int,
                eventValue: str) -> c_void_p
```

Parameters
----------

eventId: str
    Underlying .Net type is string

timestampNanoseconds: int
    Underlying .Net type is long

eventValue: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(eventId: str, timestamp: c_void_p,
                 eventValue: str) -> c_void_p
```

Parameters
----------

eventId: str
    Underlying .Net type is string

timestamp: c_void_p
    GC Handle Pointer to .Net type DateTime

eventValue: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Constructor3"></a>

#### Constructor3

```python
@staticmethod
def Constructor3(eventId: str, timestamp: c_void_p,
                 eventValue: str) -> c_void_p
```

Parameters
----------

eventId: str
    Underlying .Net type is string

timestamp: c_void_p
    GC Handle Pointer to .Net type TimeSpan

eventValue: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.Clone"></a>

#### Clone

```python
def Clone() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Id"></a>

#### get\_Id

```python
def get_Id() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.set_Id"></a>

#### set\_Id

```python
def set_Id(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Value"></a>

#### get\_Value

```python
def get_Value() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.set_Value"></a>

#### set\_Value

```python
def set_Value(value: str) -> None
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Tags"></a>

#### get\_Tags

```python
def get_Tags() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IDictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.AddTag"></a>

#### AddTag

```python
def AddTag(tagId: str, tagValue: str) -> c_void_p
```

Parameters
----------

tagId: str
    Underlying .Net type is string

tagValue: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.AddTags"></a>

#### AddTags

```python
def AddTags(tags: c_void_p) -> c_void_p
```

Parameters
----------

tags: c_void_p
    GC Handle Pointer to .Net type IEnumerable<KeyValuePair<string, string>>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.RemoveTag"></a>

#### RemoveTag

```python
def RemoveTag(tagId: str) -> c_void_p
```

Parameters
----------

tagId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_TimestampNanoseconds"></a>

#### get\_TimestampNanoseconds

```python
def get_TimestampNanoseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_TimestampMilliseconds"></a>

#### get\_TimestampMilliseconds

```python
def get_TimestampMilliseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_Timestamp"></a>

#### get\_Timestamp

```python
def get_Timestamp() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventData.EventData.get_TimestampAsTimeSpan"></a>

#### get\_TimestampAsTimeSpan

```python
def get_TimestampAsTimeSpan() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValueType"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterValueType

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.CommitMode"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.CommitMode

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration"></a>

## TimeseriesBufferConfiguration Objects

```python
class TimeseriesBufferConfiguration(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferConfiguration

Returns
----------

TimeseriesBufferConfiguration:
    Instance wrapping the .net type TimeseriesBufferConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferConfiguration

Returns
----------

TimeseriesBufferConfiguration:
    Instance wrapping the .net type TimeseriesBufferConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesBufferConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_PacketSize"></a>

#### get\_PacketSize

```python
def get_PacketSize() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is int?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_PacketSize"></a>

#### set\_PacketSize

```python
def set_PacketSize(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is int?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_TimeSpanInNanoseconds"></a>

#### get\_TimeSpanInNanoseconds

```python
def get_TimeSpanInNanoseconds() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is long?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_TimeSpanInNanoseconds"></a>

#### set\_TimeSpanInNanoseconds

```python
def set_TimeSpanInNanoseconds(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is long?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_TimeSpanInMilliseconds"></a>

#### get\_TimeSpanInMilliseconds

```python
def get_TimeSpanInMilliseconds() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is long?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_TimeSpanInMilliseconds"></a>

#### set\_TimeSpanInMilliseconds

```python
def set_TimeSpanInMilliseconds(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is long?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_CustomTriggerBeforeEnqueue"></a>

#### get\_CustomTriggerBeforeEnqueue

```python
def get_CustomTriggerBeforeEnqueue() -> Callable[[c_void_p], bool]
```

Parameters
----------

Returns
-------

Callable[[c_void_p], bool]:
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_CustomTriggerBeforeEnqueue"></a>

#### set\_CustomTriggerBeforeEnqueue

```python
def set_CustomTriggerBeforeEnqueue(value: Callable[[c_void_p], bool]) -> None
```

Parameters
----------

value: Callable[[c_void_p], bool]
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_CustomTrigger"></a>

#### get\_CustomTrigger

```python
def get_CustomTrigger() -> Callable[[c_void_p], bool]
```

Parameters
----------

Returns
-------

Callable[[c_void_p], bool]:
    Underlying .Net type is Func<TimeseriesData, Boolean>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_CustomTrigger"></a>

#### set\_CustomTrigger

```python
def set_CustomTrigger(value: Callable[[c_void_p], bool]) -> None
```

Parameters
----------

value: Callable[[c_void_p], bool]
    Underlying .Net type is Func<TimeseriesData, Boolean>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_Filter"></a>

#### get\_Filter

```python
def get_Filter() -> Callable[[c_void_p], bool]
```

Parameters
----------

Returns
-------

Callable[[c_void_p], bool]:
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_Filter"></a>

#### set\_Filter

```python
def set_Filter(value: Callable[[c_void_p], bool]) -> None
```

Parameters
----------

value: Callable[[c_void_p], bool]
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.get_BufferTimeout"></a>

#### get\_BufferTimeout

```python
def get_BufferTimeout() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is int?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBufferConfiguration.TimeseriesBufferConfiguration.set_BufferTimeout"></a>

#### set\_BufferTimeout

```python
def set_BufferTimeout(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is int?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition"></a>

## EventDefinition Objects

```python
class EventDefinition(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDefinition

Returns
----------

EventDefinition:
    Instance wrapping the .net type EventDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type EventDefinition

Returns
----------

EventDefinition:
    Instance wrapping the .net type EventDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Id"></a>

#### get\_Id

```python
def get_Id() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Description"></a>

#### get\_Description

```python
def get_Description() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Location"></a>

#### get\_Location

```python
def get_Location() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_CustomProperties"></a>

#### get\_CustomProperties

```python
def get_CustomProperties() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.EventDefinition.EventDefinition.get_Level"></a>

#### get\_Level

```python
def get_Level() -> EventLevel
```

Parameters
----------

Returns
-------

EventLevel:
    Underlying .Net type is EventLevel

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer"></a>

## TimeseriesBufferConsumer Objects

```python
class TimeseriesBufferConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferConsumer

Returns
----------

TimeseriesBufferConsumer:
    Instance wrapping the .net type TimeseriesBufferConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBufferConsumer

Returns
----------

TimeseriesBufferConsumer:
    Instance wrapping the .net type TimeseriesBufferConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer.TimeseriesBufferConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer"></a>

## StreamPropertiesConsumer Objects

```python
class StreamPropertiesConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamPropertiesConsumer

Returns
----------

StreamPropertiesConsumer:
    Instance wrapping the .net type StreamPropertiesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamPropertiesConsumer

Returns
----------

StreamPropertiesConsumer:
    Instance wrapping the .net type StreamPropertiesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.add_OnChanged"></a>

#### add\_OnChanged

```python
def add_OnChanged(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<StreamPropertiesChangedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.remove_OnChanged"></a>

#### remove\_OnChanged

```python
def remove_OnChanged(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<StreamPropertiesChangedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Location"></a>

#### get\_Location

```python
def get_Location() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_TimeOfRecording"></a>

#### get\_TimeOfRecording

```python
def get_TimeOfRecording() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Metadata"></a>

#### get\_Metadata

```python
def get_Metadata() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Dictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.get_Parents"></a>

#### get\_Parents

```python
def get_Parents() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type List<string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer.StreamPropertiesConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer"></a>

## StreamTimeseriesConsumer Objects

```python
class StreamTimeseriesConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamTimeseriesConsumer

Returns
----------

StreamTimeseriesConsumer:
    Instance wrapping the .net type StreamTimeseriesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamTimeseriesConsumer

Returns
----------

StreamTimeseriesConsumer:
    Instance wrapping the .net type StreamTimeseriesConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.CreateBuffer"></a>

#### CreateBuffer

```python
def CreateBuffer(parametersFilter: c_void_p,
                 bufferConfiguration: c_void_p = None) -> c_void_p
```

Parameters
----------

parametersFilter: c_void_p
    GC Handle Pointer to .Net type string[]

bufferConfiguration: c_void_p
    (Optional) GC Handle Pointer to .Net type TimeseriesBufferConfiguration. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesBufferConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.CreateBuffer2"></a>

#### CreateBuffer2

```python
def CreateBuffer2(parametersFilter: c_void_p) -> c_void_p
```

Parameters
----------

parametersFilter: c_void_p
    GC Handle Pointer to .Net type string[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesBufferConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.add_OnDefinitionsChanged"></a>

#### add\_OnDefinitionsChanged

```python
def add_OnDefinitionsChanged(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<ParameterDefinitionsChangedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.remove_OnDefinitionsChanged"></a>

#### remove\_OnDefinitionsChanged

```python
def remove_OnDefinitionsChanged(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<ParameterDefinitionsChangedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.add_OnDataReceived"></a>

#### add\_OnDataReceived

```python
def add_OnDataReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.remove_OnDataReceived"></a>

#### remove\_OnDataReceived

```python
def remove_OnDataReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.add_OnRawReceived"></a>

#### add\_OnRawReceived

```python
def add_OnRawReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataRawReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.remove_OnRawReceived"></a>

#### remove\_OnRawReceived

```python
def remove_OnRawReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataRawReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.get_Definitions"></a>

#### get\_Definitions

```python
def get_Definitions() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type List<ParameterDefinition>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamTimeseriesConsumer.StreamTimeseriesConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs"></a>

## EventDataReadEventArgs Objects

```python
class EventDataReadEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type EventDataReadEventArgs

Returns
----------

EventDataReadEventArgs:
Instance wrapping the .net type EventDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type EventDataReadEventArgs

Returns
----------

EventDataReadEventArgs:
Instance wrapping the .net type EventDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topicConsumer: c_void_p, consumer: c_void_p,
                data: c_void_p) -> c_void_p
```

Parameters
----------

topicConsumer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

consumer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

data: c_void_p
    GC Handle Pointer to .Net type EventData

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.get_TopicConsumer"></a>

#### get\_TopicConsumer

```python
def get_TopicConsumer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDataReadEventArgs.EventDataReadEventArgs.get_Data"></a>

#### get\_Data

```python
def get_Data() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs"></a>

## EventDefinitionsChangedEventArgs Objects

```python
class EventDefinitionsChangedEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type EventDefinitionsChangedEventArgs

Returns
----------

EventDefinitionsChangedEventArgs:
Instance wrapping the .net type EventDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type EventDefinitionsChangedEventArgs

Returns
----------

EventDefinitionsChangedEventArgs:
Instance wrapping the .net type EventDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topicConsumer: c_void_p, consumer: c_void_p) -> c_void_p
```

Parameters
----------

topicConsumer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

consumer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type EventDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.get_TopicConsumer"></a>

#### get\_TopicConsumer

```python
def get_TopicConsumer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs.EventDefinitionsChangedEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs"></a>

## ParameterDefinitionsChangedEventArgs Objects

```python
class ParameterDefinitionsChangedEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type ParameterDefinitionsChangedEventArgs

Returns
----------

ParameterDefinitionsChangedEventArgs:
Instance wrapping the .net type ParameterDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type ParameterDefinitionsChangedEventArgs

Returns
----------

ParameterDefinitionsChangedEventArgs:
Instance wrapping the .net type ParameterDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topicConsumer: c_void_p, consumer: c_void_p) -> c_void_p
```

Parameters
----------

topicConsumer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

consumer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinitionsChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.get_TopicConsumer"></a>

#### get\_TopicConsumer

```python
def get_TopicConsumer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs.ParameterDefinitionsChangedEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer"></a>

## StreamEventsConsumer Objects

```python
class StreamEventsConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamEventsConsumer

Returns
----------

StreamEventsConsumer:
    Instance wrapping the .net type StreamEventsConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StreamEventsConsumer

Returns
----------

StreamEventsConsumer:
    Instance wrapping the .net type StreamEventsConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.add_OnDataReceived"></a>

#### add\_OnDataReceived

```python
def add_OnDataReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<EventDataReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.remove_OnDataReceived"></a>

#### remove\_OnDataReceived

```python
def remove_OnDataReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<EventDataReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.add_OnDefinitionsChanged"></a>

#### add\_OnDefinitionsChanged

```python
def add_OnDefinitionsChanged(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<EventDefinitionsChangedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.remove_OnDefinitionsChanged"></a>

#### remove\_OnDefinitionsChanged

```python
def remove_OnDefinitionsChanged(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<EventDefinitionsChangedEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.get_Definitions"></a>

#### get\_Definitions

```python
def get_Definitions() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IList<EventDefinition>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamEventsConsumer.StreamEventsConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs"></a>

## StreamPropertiesChangedEventArgs Objects

```python
class StreamPropertiesChangedEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type StreamPropertiesChangedEventArgs

Returns
----------

StreamPropertiesChangedEventArgs:
Instance wrapping the .net type StreamPropertiesChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type StreamPropertiesChangedEventArgs

Returns
----------

StreamPropertiesChangedEventArgs:
Instance wrapping the .net type StreamPropertiesChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topicConsumer: c_void_p, consumer: c_void_p) -> c_void_p
```

Parameters
----------

topicConsumer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

consumer: c_void_p
    GC Handle Pointer to .Net type IStreamConsumer

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamPropertiesChangedEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.get_TopicConsumer"></a>

#### get\_TopicConsumer

```python
def get_TopicConsumer() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs.StreamPropertiesChangedEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs"></a>

## TimeseriesDataReadEventArgs Objects

```python
class TimeseriesDataReadEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type TimeseriesDataReadEventArgs

Returns
----------

TimeseriesDataReadEventArgs:
Instance wrapping the .net type TimeseriesDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type TimeseriesDataReadEventArgs

Returns
----------

TimeseriesDataReadEventArgs:
Instance wrapping the .net type TimeseriesDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topic: c_void_p, stream: c_void_p, data: c_void_p) -> c_void_p
```

Parameters
----------

topic: c_void_p
    GC Handle Pointer to .Net type Object

stream: c_void_p
    GC Handle Pointer to .Net type Object

data: c_void_p
    GC Handle Pointer to .Net type TimeseriesData

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.get_Topic"></a>

#### get\_Topic

```python
def get_Topic() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs.TimeseriesDataReadEventArgs.get_Data"></a>

#### get\_Data

```python
def get_Data() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesData

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs"></a>

## TimeseriesDataRawReadEventArgs Objects

```python
class TimeseriesDataRawReadEventArgs(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type TimeseriesDataRawReadEventArgs

Returns
----------

TimeseriesDataRawReadEventArgs:
Instance wrapping the .net type TimeseriesDataRawReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type TimeseriesDataRawReadEventArgs

Returns
----------

TimeseriesDataRawReadEventArgs:
Instance wrapping the .net type TimeseriesDataRawReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(topic: c_void_p, stream: c_void_p, data: c_void_p) -> c_void_p
```

Parameters
----------

topic: c_void_p
    GC Handle Pointer to .Net type Object

stream: c_void_p
    GC Handle Pointer to .Net type Object

data: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataRaw

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataRawReadEventArgs

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.get_Topic"></a>

#### get\_Topic

```python
def get_Topic() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.get_Stream"></a>

#### get\_Stream

```python
def get_Stream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Object

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs.TimeseriesDataRawReadEventArgs.get_Data"></a>

#### get\_Data

```python
def get_Data() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataRaw

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer"></a>

## TimeseriesBuffer Objects

```python
class TimeseriesBuffer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBuffer

Returns
----------

TimeseriesBuffer:
    Instance wrapping the .net type TimeseriesBuffer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesBuffer

Returns
----------

TimeseriesBuffer:
    Instance wrapping the .net type TimeseriesBuffer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.add_OnDataReleased"></a>

#### add\_OnDataReleased

```python
def add_OnDataReleased(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.remove_OnDataReleased"></a>

#### remove\_OnDataReleased

```python
def remove_OnDataReleased(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.add_OnRawReleased"></a>

#### add\_OnRawReleased

```python
def add_OnRawReleased(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataRawReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.remove_OnRawReleased"></a>

#### remove\_OnRawReleased

```python
def remove_OnRawReleased(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<TimeseriesDataRawReadEventArgs>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_PacketSize"></a>

#### get\_PacketSize

```python
def get_PacketSize() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is int?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_PacketSize"></a>

#### set\_PacketSize

```python
def set_PacketSize(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is int?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_BufferTimeout"></a>

#### get\_BufferTimeout

```python
def get_BufferTimeout() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is int?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_BufferTimeout"></a>

#### set\_BufferTimeout

```python
def set_BufferTimeout(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is int?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_TimeSpanInNanoseconds"></a>

#### get\_TimeSpanInNanoseconds

```python
def get_TimeSpanInNanoseconds() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is long?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_TimeSpanInNanoseconds"></a>

#### set\_TimeSpanInNanoseconds

```python
def set_TimeSpanInNanoseconds(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is long?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_TimeSpanInMilliseconds"></a>

#### get\_TimeSpanInMilliseconds

```python
def get_TimeSpanInMilliseconds() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is long?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_TimeSpanInMilliseconds"></a>

#### set\_TimeSpanInMilliseconds

```python
def set_TimeSpanInMilliseconds(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is long?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_Filter"></a>

#### get\_Filter

```python
def get_Filter() -> Callable[[c_void_p], bool]
```

Parameters
----------

Returns
-------

Callable[[c_void_p], bool]:
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_Filter"></a>

#### set\_Filter

```python
def set_Filter(value: Callable[[c_void_p], bool]) -> None
```

Parameters
----------

value: Callable[[c_void_p], bool]
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_CustomTriggerBeforeEnqueue"></a>

#### get\_CustomTriggerBeforeEnqueue

```python
def get_CustomTriggerBeforeEnqueue() -> Callable[[c_void_p], bool]
```

Parameters
----------

Returns
-------

Callable[[c_void_p], bool]:
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_CustomTriggerBeforeEnqueue"></a>

#### set\_CustomTriggerBeforeEnqueue

```python
def set_CustomTriggerBeforeEnqueue(value: Callable[[c_void_p], bool]) -> None
```

Parameters
----------

value: Callable[[c_void_p], bool]
    Underlying .Net type is Func<TimeseriesDataTimestamp, Boolean>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.get_CustomTrigger"></a>

#### get\_CustomTrigger

```python
def get_CustomTrigger() -> Callable[[c_void_p], bool]
```

Parameters
----------

Returns
-------

Callable[[c_void_p], bool]:
    Underlying .Net type is Func<TimeseriesData, Boolean>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.set_CustomTrigger"></a>

#### set\_CustomTrigger

```python
def set_CustomTrigger(value: Callable[[c_void_p], bool]) -> None
```

Parameters
----------

value: Callable[[c_void_p], bool]
    Underlying .Net type is Func<TimeseriesData, Boolean>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesBuffer.TimeseriesBuffer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition"></a>

## ParameterDefinition Objects

```python
class ParameterDefinition(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterDefinition

Returns
----------

ParameterDefinition:
    Instance wrapping the .net type ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ParameterDefinition

Returns
----------

ParameterDefinition:
    Instance wrapping the .net type ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterDefinition

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Id"></a>

#### get\_Id

```python
def get_Id() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Name"></a>

#### get\_Name

```python
def get_Name() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Description"></a>

#### get\_Description

```python
def get_Description() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Location"></a>

#### get\_Location

```python
def get_Location() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_MinimumValue"></a>

#### get\_MinimumValue

```python
def get_MinimumValue() -> Optional[float]
```

Parameters
----------

Returns
-------

Optional[float]:
    Underlying .Net type is double?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_MaximumValue"></a>

#### get\_MaximumValue

```python
def get_MaximumValue() -> Optional[float]
```

Parameters
----------

Returns
-------

Optional[float]:
    Underlying .Net type is double?

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Unit"></a>

#### get\_Unit

```python
def get_Unit() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_Format"></a>

#### get\_Format

```python
def get_Format() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.ParameterDefinition.ParameterDefinition.get_CustomProperties"></a>

#### get\_CustomProperties

```python
def get_CustomProperties() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp"></a>

## TimeseriesDataTimestamp Objects

```python
class TimeseriesDataTimestamp(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

Returns
----------

TimeseriesDataTimestamp:
    Instance wrapping the .net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

Returns
----------

TimeseriesDataTimestamp:
    Instance wrapping the .net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_Parameters"></a>

#### get\_Parameters

```python
def get_Parameters() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestampValues

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_Tags"></a>

#### get\_Tags

```python
def get_Tags() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestampTags

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_TimestampNanoseconds"></a>

#### get\_TimestampNanoseconds

```python
def get_TimestampNanoseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.set_TimestampNanoseconds"></a>

#### set\_TimestampNanoseconds

```python
def set_TimestampNanoseconds(value: int) -> None
```

Parameters
----------

value: int
    Underlying .Net type is long

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_TimestampMilliseconds"></a>

#### get\_TimestampMilliseconds

```python
def get_TimestampMilliseconds() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_Timestamp"></a>

#### get\_Timestamp

```python
def get_Timestamp() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.get_TimestampAsTimeSpan"></a>

#### get\_TimestampAsTimeSpan

```python
def get_TimestampAsTimeSpan() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue"></a>

#### AddValue

```python
def AddValue(parameterId: str, value: float) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue2"></a>

#### AddValue2

```python
def AddValue2(parameterId: str, value: str) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue3"></a>

#### AddValue3

```python
def AddValue3(parameterId: str, value: c_void_p) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddValue4"></a>

#### AddValue4

```python
def AddValue4(parameterId: str, value: c_void_p) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type ParameterValue

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.RemoveValue"></a>

#### RemoveValue

```python
def RemoveValue(parameterId: str) -> c_void_p
```

Parameters
----------

parameterId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddTag"></a>

#### AddTag

```python
def AddTag(tagId: str, tagValue: str) -> c_void_p
```

Parameters
----------

tagId: str
    Underlying .Net type is string

tagValue: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.AddTags"></a>

#### AddTags

```python
def AddTags(tags: c_void_p) -> c_void_p
```

Parameters
----------

tags: c_void_p
    GC Handle Pointer to .Net type IEnumerable<KeyValuePair<string, string>>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.RemoveTag"></a>

#### RemoveTag

```python
def RemoveTag(tagId: str) -> c_void_p
```

Parameters
----------

tagId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeseriesDataTimestamp

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp.TimeseriesDataTimestamp.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer"></a>

## ITopicProducer Objects

```python
class ITopicProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ITopicProducer

Returns
----------

ITopicProducer:
    Instance wrapping the .net type ITopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ITopicProducer

Returns
----------

ITopicProducer:
    Instance wrapping the .net type ITopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.CreateStream"></a>

#### CreateStream

```python
def CreateStream() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.CreateStream2"></a>

#### CreateStream2

```python
def CreateStream2(streamId: str) -> c_void_p
```

Parameters
----------

streamId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.GetStream"></a>

#### GetStream

```python
def GetStream(streamId: str) -> c_void_p
```

Parameters
----------

streamId: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.GetOrCreateStream"></a>

#### GetOrCreateStream

```python
def GetOrCreateStream(
        streamId: str,
        onStreamCreated: Callable[[c_void_p], None] = None) -> c_void_p
```

Parameters
----------

streamId: str
    Underlying .Net type is string

onStreamCreated: Callable[[c_void_p], None]
    (Optional) Underlying .Net type is Action<IStreamProducer>. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicProducer.ITopicProducer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer"></a>

## IStreamProducer Objects

```python
class IStreamProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStreamProducer

Returns
----------

IStreamProducer:
    Instance wrapping the .net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStreamProducer

Returns
----------

IStreamProducer:
    Instance wrapping the .net type IStreamProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_StreamId"></a>

#### get\_StreamId

```python
def get_StreamId() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Epoch"></a>

#### get\_Epoch

```python
def get_Epoch() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type DateTime

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.set_Epoch"></a>

#### set\_Epoch

```python
def set_Epoch(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type DateTime

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Properties"></a>

#### get\_Properties

```python
def get_Properties() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamPropertiesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Timeseries"></a>

#### get\_Timeseries

```python
def get_Timeseries() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamTimeseriesProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.get_Events"></a>

#### get\_Events

```python
def get_Events() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StreamEventsProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.Close"></a>

#### Close

```python
def Close(streamState: StreamEndType = StreamEndType.Closed) -> None
```

Parameters
----------

streamState: StreamEndType
    (Optional) Underlying .Net type is StreamEndType. Defaults to Closed

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.add_OnWriteException"></a>

#### add\_OnWriteException

```python
def add_OnWriteException(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.IStreamProducer.IStreamProducer.remove_OnWriteException"></a>

#### remove\_OnWriteException

```python
def remove_OnWriteException(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer"></a>

## ITopicConsumer Objects

```python
class ITopicConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

Returns
----------

ITopicConsumer:
    Instance wrapping the .net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type ITopicConsumer

Returns
----------

ITopicConsumer:
    Instance wrapping the .net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.Subscribe"></a>

#### Subscribe

```python
def Subscribe() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnStreamReceived"></a>

#### add\_OnStreamReceived

```python
def add_OnStreamReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnStreamReceived"></a>

#### remove\_OnStreamReceived

```python
def remove_OnStreamReceived(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnRevoking"></a>

#### add\_OnRevoking

```python
def add_OnRevoking(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnRevoking"></a>

#### remove\_OnRevoking

```python
def remove_OnRevoking(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnStreamsRevoked"></a>

#### add\_OnStreamsRevoked

```python
def add_OnStreamsRevoked(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnStreamsRevoked"></a>

#### remove\_OnStreamsRevoked

```python
def remove_OnStreamsRevoked(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnCommitted"></a>

#### add\_OnCommitted

```python
def add_OnCommitted(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnCommitted"></a>

#### remove\_OnCommitted

```python
def remove_OnCommitted(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnCommitting"></a>

#### add\_OnCommitting

```python
def add_OnCommitting(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnCommitting"></a>

#### remove\_OnCommitting

```python
def remove_OnCommitting(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.Commit"></a>

#### Commit

```python
def Commit() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.ITopicConsumer.ITopicConsumer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer"></a>

## TopicConsumer Objects

```python
class TopicConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TopicConsumer

Returns
----------

TopicConsumer:
    Instance wrapping the .net type TopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TopicConsumer

Returns
----------

TopicConsumer:
    Instance wrapping the .net type TopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(telemetryKafkaConsumer: c_void_p) -> c_void_p
```

Parameters
----------

telemetryKafkaConsumer: c_void_p
    GC Handle Pointer to .Net type TelemetryKafkaConsumer

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnStreamReceived"></a>

#### add\_OnStreamReceived

```python
def add_OnStreamReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnStreamReceived"></a>

#### remove\_OnStreamReceived

```python
def remove_OnStreamReceived(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnRevoking"></a>

#### add\_OnRevoking

```python
def add_OnRevoking(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnRevoking"></a>

#### remove\_OnRevoking

```python
def remove_OnRevoking(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnStreamsRevoked"></a>

#### add\_OnStreamsRevoked

```python
def add_OnStreamsRevoked(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnStreamsRevoked"></a>

#### remove\_OnStreamsRevoked

```python
def remove_OnStreamsRevoked(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<IStreamConsumer[]>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnCommitted"></a>

#### add\_OnCommitted

```python
def add_OnCommitted(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnCommitted"></a>

#### remove\_OnCommitted

```python
def remove_OnCommitted(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnCommitting"></a>

#### add\_OnCommitting

```python
def add_OnCommitting(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnCommitting"></a>

#### remove\_OnCommitting

```python
def remove_OnCommitting(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Commit"></a>

#### Commit

```python
def Commit() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Subscribe"></a>

#### Subscribe

```python
def Subscribe() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TopicConsumer.TopicConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.App"></a>

# quixstreams.native.Python.QuixStreamsStreaming.App

<a id="quixstreams.native.Python.QuixStreamsStreaming.App.App"></a>

## App Objects

```python
class App(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.App.App.Run"></a>

#### Run

```python
@staticmethod
def Run(cancellationToken: c_void_p = None,
        beforeShutdown: Callable[[], None] = None) -> None
```

Parameters
----------

cancellationToken: c_void_p
    (Optional) GC Handle Pointer to .Net type CancellationToken. Defaults to None

beforeShutdown: Callable[[], None]
    (Optional) Underlying .Net type is Action. Defaults to None

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues"></a>

# quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues"></a>

## TimeseriesDataTimestampValues Objects

```python
class TimeseriesDataTimestampValues(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestampValues

Returns
----------

TimeseriesDataTimestampValues:
    Instance wrapping the .net type TimeseriesDataTimestampValues

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TimeseriesDataTimestampValues

Returns
----------

TimeseriesDataTimestampValues:
    Instance wrapping the .net type TimeseriesDataTimestampValues

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Values"></a>

#### get\_Values

```python
def get_Values() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerable<ParameterValue>

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.ContainsKey"></a>

#### ContainsKey

```python
def ContainsKey(key: str) -> bool
```

Parameters
----------

key: str
    Underlying .Net type is string

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.TryGetValue"></a>

#### TryGetValue

```python
def TryGetValue(key: str, value: c_void_p) -> bool
```

Parameters
----------

key: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type ParameterValue&

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Count"></a>

#### get\_Count

```python
def get_Count() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Keys"></a>

#### get\_Keys

```python
def get_Keys() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerable<string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.get_Item"></a>

#### get\_Item

```python
def get_Item(key: str) -> c_void_p
```

Parameters
----------

key: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ParameterValue

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.Equals"></a>

#### Equals

```python
def Equals(obj: c_void_p) -> bool
```

Parameters
----------

obj: c_void_p
    GC Handle Pointer to .Net type Object

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.GetHashCode"></a>

#### GetHashCode

```python
def GetHashCode() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.QuixStreamsStreaming.TimeseriesDataTimestampValues.TimeseriesDataTimestampValues.ToString"></a>

#### ToString

```python
def ToString() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient"></a>

# quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient"></a>

## QuixStreamingClient Objects

```python
class QuixStreamingClient(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type QuixStreamingClient

Returns
----------

QuixStreamingClient:
    Instance wrapping the .net type QuixStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type QuixStreamingClient

Returns
----------

QuixStreamingClient:
    Instance wrapping the .net type QuixStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(token: str = None,
                autoCreateTopics: bool = True,
                properties: c_void_p = None,
                debug: bool = False,
                httpClient: c_void_p = None) -> c_void_p
```

Parameters
----------

token: str
    (Optional) Underlying .Net type is string. Defaults to None

autoCreateTopics: bool
    (Optional) Underlying .Net type is Boolean. Defaults to True

properties: c_void_p
    (Optional) GC Handle Pointer to .Net type IDictionary<string, string>. Defaults to None

debug: bool
    (Optional) Underlying .Net type is Boolean. Defaults to False

httpClient: c_void_p
    (Optional) GC Handle Pointer to .Net type HttpClient. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type QuixStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.get_TokenValidationConfig"></a>

#### get\_TokenValidationConfig

```python
def get_TokenValidationConfig() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type QuixStreamingClient.TokenValidationConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.set_TokenValidationConfig"></a>

#### set\_TokenValidationConfig

```python
def set_TokenValidationConfig(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type QuixStreamingClient.TokenValidationConfiguration

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetTopicConsumer"></a>

#### GetTopicConsumer

```python
def GetTopicConsumer(
        topicIdOrName: str,
        consumerGroup: str = None,
        options: c_void_p = None,
        autoOffset: AutoOffsetReset = AutoOffsetReset.Latest) -> c_void_p
```

Parameters
----------

topicIdOrName: str
    Underlying .Net type is string

consumerGroup: str
    (Optional) Underlying .Net type is string. Defaults to None

options: c_void_p
    (Optional) GC Handle Pointer to .Net type CommitOptions. Defaults to None

autoOffset: AutoOffsetReset
    (Optional) Underlying .Net type is AutoOffsetReset. Defaults to Latest

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetRawTopicConsumer"></a>

#### GetRawTopicConsumer

```python
def GetRawTopicConsumer(
        topicIdOrName: str,
        consumerGroup: str = None,
        autoOffset: Optional[AutoOffsetReset] = None) -> c_void_p
```

Parameters
----------

topicIdOrName: str
    Underlying .Net type is string

consumerGroup: str
    (Optional) Underlying .Net type is string. Defaults to None

autoOffset: Optional[AutoOffsetReset]
    (Optional) Underlying .Net type is AutoOffsetReset?. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IRawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetRawTopicProducer"></a>

#### GetRawTopicProducer

```python
def GetRawTopicProducer(topicIdOrName: str) -> c_void_p
```

Parameters
----------

topicIdOrName: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IRawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.GetTopicProducer"></a>

#### GetTopicProducer

```python
def GetTopicProducer(topicIdOrName: str) -> c_void_p
```

Parameters
----------

topicIdOrName: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.get_ApiUrl"></a>

#### get\_ApiUrl

```python
def get_ApiUrl() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.set_ApiUrl"></a>

#### set\_ApiUrl

```python
def set_ApiUrl(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.get_CachePeriod"></a>

#### get\_CachePeriod

```python
def get_CachePeriod() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient.QuixStreamingClient.set_CachePeriod"></a>

#### set\_CachePeriod

```python
def set_CachePeriod(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration"></a>

# quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient\_TokenValidationConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration"></a>

## TokenValidationConfiguration Objects

```python
class TokenValidationConfiguration(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TokenValidationConfiguration

Returns
----------

TokenValidationConfiguration:
    Instance wrapping the .net type TokenValidationConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TokenValidationConfiguration

Returns
----------

TokenValidationConfiguration:
    Instance wrapping the .net type TokenValidationConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type QuixStreamingClient.TokenValidationConfiguration

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.get_Enabled"></a>

#### get\_Enabled

```python
def get_Enabled() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.set_Enabled"></a>

#### set\_Enabled

```python
def set_Enabled(value: bool) -> None
```

Parameters
----------

value: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.get_WarningBeforeExpiry"></a>

#### get\_WarningBeforeExpiry

```python
def get_WarningBeforeExpiry() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan?

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.set_WarningBeforeExpiry"></a>

#### set\_WarningBeforeExpiry

```python
def set_WarningBeforeExpiry(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TimeSpan?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.get_WarnAboutNonPatToken"></a>

#### get\_WarnAboutNonPatToken

```python
def get_WarnAboutNonPatToken() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsStreaming.QuixStreamingClient_TokenValidationConfiguration.TokenValidationConfiguration.set_WarnAboutNonPatToken"></a>

#### set\_WarnAboutNonPatToken

```python
def set_WarnAboutNonPatToken(value: bool) -> None
```

Parameters
----------

value: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage"></a>

## RawMessage Objects

```python
class RawMessage(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type RawMessage

Returns
----------

RawMessage:
    Instance wrapping the .net type RawMessage

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type RawMessage

Returns
----------

RawMessage:
    Instance wrapping the .net type RawMessage

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(key: c_void_p, value: c_void_p) -> c_void_p
```

Parameters
----------

key: c_void_p
    GC Handle Pointer to .Net type byte[]

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type RawMessage

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(value: c_void_p) -> c_void_p
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type RawMessage

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.get_Metadata"></a>

#### get\_Metadata

```python
def get_Metadata() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ReadOnlyDictionary<string, string>

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.get_Key"></a>

#### get\_Key

```python
def get_Key() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type byte[]

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.set_Key"></a>

#### set\_Key

```python
def set_Key(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.get_Value"></a>

#### get\_Value

```python
def get_Value() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type byte[]

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawMessage.RawMessage.set_Value"></a>

#### set\_Value

```python
def set_Value(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer"></a>

## IRawTopicProducer Objects

```python
class IRawTopicProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IRawTopicProducer

Returns
----------

IRawTopicProducer:
    Instance wrapping the .net type IRawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IRawTopicProducer

Returns
----------

IRawTopicProducer:
    Instance wrapping the .net type IRawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.Publish"></a>

#### Publish

```python
def Publish(data: c_void_p) -> None
```

Parameters
----------

data: c_void_p
    GC Handle Pointer to .Net type RawMessage

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicProducer.IRawTopicProducer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer"></a>

## RawTopicProducer Objects

```python
class RawTopicProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type RawTopicProducer

Returns
----------

RawTopicProducer:
    Instance wrapping the .net type RawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type RawTopicProducer

Returns
----------

RawTopicProducer:
    Instance wrapping the .net type RawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(brokerAddress: str,
                topicName: str,
                brokerProperties: c_void_p = None) -> c_void_p
```

Parameters
----------

brokerAddress: str
    Underlying .Net type is string

topicName: str
    Underlying .Net type is string

brokerProperties: c_void_p
    (Optional) GC Handle Pointer to .Net type Dictionary<string, string>. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type RawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.Publish"></a>

#### Publish

```python
def Publish(message: c_void_p) -> None
```

Parameters
----------

message: c_void_p
    GC Handle Pointer to .Net type RawMessage

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicProducer.RawTopicProducer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer"></a>

## RawTopicConsumer Objects

```python
class RawTopicConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type RawTopicConsumer

Returns
----------

RawTopicConsumer:
    Instance wrapping the .net type RawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type RawTopicConsumer

Returns
----------

RawTopicConsumer:
    Instance wrapping the .net type RawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(brokerAddress: str,
                topicName: str,
                consumerGroup: str,
                brokerProperties: c_void_p = None,
                autoOffset: Optional[AutoOffsetReset] = None) -> c_void_p
```

Parameters
----------

brokerAddress: str
    Underlying .Net type is string

topicName: str
    Underlying .Net type is string

consumerGroup: str
    Underlying .Net type is string

brokerProperties: c_void_p
    (Optional) GC Handle Pointer to .Net type Dictionary<string, string>. Defaults to None

autoOffset: Optional[AutoOffsetReset]
    (Optional) Underlying .Net type is AutoOffsetReset?. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type RawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.add_OnMessageReceived"></a>

#### add\_OnMessageReceived

```python
def add_OnMessageReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<RawMessage>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.remove_OnMessageReceived"></a>

#### remove\_OnMessageReceived

```python
def remove_OnMessageReceived(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<RawMessage>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.add_OnErrorOccurred"></a>

#### add\_OnErrorOccurred

```python
def add_OnErrorOccurred(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.remove_OnErrorOccurred"></a>

#### remove\_OnErrorOccurred

```python
def remove_OnErrorOccurred(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.Subscribe"></a>

#### Subscribe

```python
def Subscribe() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer.RawTopicConsumer.Dispose"></a>

#### Dispose

```python
def Dispose() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer"></a>

# quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer"></a>

## IRawTopicConsumer Objects

```python
class IRawTopicConsumer(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IRawTopicConsumer

Returns
----------

IRawTopicConsumer:
    Instance wrapping the .net type IRawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IRawTopicConsumer

Returns
----------

IRawTopicConsumer:
    Instance wrapping the .net type IRawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.Subscribe"></a>

#### Subscribe

```python
def Subscribe() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.add_OnMessageReceived"></a>

#### add\_OnMessageReceived

```python
def add_OnMessageReceived(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<RawMessage>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.remove_OnMessageReceived"></a>

#### remove\_OnMessageReceived

```python
def remove_OnMessageReceived(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<RawMessage>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.add_OnErrorOccurred"></a>

#### add\_OnErrorOccurred

```python
def add_OnErrorOccurred(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.remove_OnErrorOccurred"></a>

#### remove\_OnErrorOccurred

```python
def remove_OnErrorOccurred(
        value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler<Exception>

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.add_OnDisposed"></a>

#### add\_OnDisposed

```python
def add_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.Raw.IRawTopicConsumer.IRawTopicConsumer.remove_OnDisposed"></a>

#### remove\_OnDisposed

```python
def remove_OnDisposed(value: Callable[[c_void_p, c_void_p], None]) -> None
```

Parameters
----------

value: Callable[[c_void_p, c_void_p], None]
    Underlying .Net type is EventHandler

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient"></a>

# quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient"></a>

## KafkaStreamingClient Objects

```python
class KafkaStreamingClient(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type KafkaStreamingClient

Returns
----------

KafkaStreamingClient:
    Instance wrapping the .net type KafkaStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type KafkaStreamingClient

Returns
----------

KafkaStreamingClient:
    Instance wrapping the .net type KafkaStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(brokerAddress: str,
                securityOptions: c_void_p = None,
                properties: c_void_p = None,
                debug: bool = False) -> c_void_p
```

Parameters
----------

brokerAddress: str
    Underlying .Net type is string

securityOptions: c_void_p
    (Optional) GC Handle Pointer to .Net type SecurityOptions. Defaults to None

properties: c_void_p
    (Optional) GC Handle Pointer to .Net type IDictionary<string, string>. Defaults to None

debug: bool
    (Optional) Underlying .Net type is Boolean. Defaults to False

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type KafkaStreamingClient

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetTopicConsumer"></a>

#### GetTopicConsumer

```python
def GetTopicConsumer(
        topic: str,
        consumerGroup: str = None,
        options: c_void_p = None,
        autoOffset: AutoOffsetReset = AutoOffsetReset.Latest) -> c_void_p
```

Parameters
----------

topic: str
    Underlying .Net type is string

consumerGroup: str
    (Optional) Underlying .Net type is string. Defaults to None

options: c_void_p
    (Optional) GC Handle Pointer to .Net type CommitOptions. Defaults to None

autoOffset: AutoOffsetReset
    (Optional) Underlying .Net type is AutoOffsetReset. Defaults to Latest

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetRawTopicConsumer"></a>

#### GetRawTopicConsumer

```python
def GetRawTopicConsumer(
        topic: str,
        consumerGroup: str = None,
        autoOffset: Optional[AutoOffsetReset] = None) -> c_void_p
```

Parameters
----------

topic: str
    Underlying .Net type is string

consumerGroup: str
    (Optional) Underlying .Net type is string. Defaults to None

autoOffset: Optional[AutoOffsetReset]
    (Optional) Underlying .Net type is AutoOffsetReset?. Defaults to None

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IRawTopicConsumer

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetRawTopicProducer"></a>

#### GetRawTopicProducer

```python
def GetRawTopicProducer(topic: str) -> c_void_p
```

Parameters
----------

topic: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IRawTopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClient.KafkaStreamingClient.GetTopicProducer"></a>

#### GetTopicProducer

```python
def GetTopicProducer(topic: str) -> c_void_p
```

Parameters
----------

topic: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicProducer

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions"></a>

# quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions.KafkaStreamingClientExtensions"></a>

## KafkaStreamingClientExtensions Objects

```python
class KafkaStreamingClientExtensions(object)
```

<a id="quixstreams.native.Python.QuixStreamsStreaming.KafkaStreamingClientExtensions.KafkaStreamingClientExtensions.GetTopicConsumer"></a>

#### GetTopicConsumer

```python
@staticmethod
def GetTopicConsumer(
        client: c_void_p,
        topic: str,
        consumerGroup: str = None,
        commitMode: CommitMode = CommitMode.Automatic,
        autoOffset: AutoOffsetReset = AutoOffsetReset.Latest) -> c_void_p
```

Parameters
----------

client: c_void_p
    GC Handle Pointer to .Net type KafkaStreamingClient

topic: str
    Underlying .Net type is string

consumerGroup: str
    (Optional) Underlying .Net type is string. Defaults to None

commitMode: CommitMode
    (Optional) Underlying .Net type is CommitMode. Defaults to Automatic

autoOffset: AutoOffsetReset
    (Optional) Underlying .Net type is AutoOffsetReset. Defaults to Latest

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type ITopicConsumer

<a id="quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage"></a>

# quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage

<a id="quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage"></a>

## LocalFileStorage Objects

```python
class LocalFileStorage(object)
```

<a id="quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type LocalFileStorage

Returns
----------

LocalFileStorage:
    Instance wrapping the .net type LocalFileStorage

<a id="quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type LocalFileStorage

Returns
----------

LocalFileStorage:
    Instance wrapping the .net type LocalFileStorage

<a id="quixstreams.native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage.LocalFileStorage.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(storageDirectory: str = None,
                autoCreateDir: bool = True) -> c_void_p
```

Parameters
----------

storageDirectory: str
    (Optional) Underlying .Net type is string. Defaults to None

autoCreateDir: bool
    (Optional) Underlying .Net type is Boolean. Defaults to True

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type LocalFileStorage

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions"></a>

# quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions"></a>

## StorageExtensions Objects

```python
class StorageExtensions(object)
```

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set"></a>

#### Set

```python
@staticmethod
def Set(stateStorage: c_void_p, key: str, value: int) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

value: int
    Underlying .Net type is long

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set2"></a>

#### Set2

```python
@staticmethod
def Set2(stateStorage: c_void_p, key: str, value: float) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

value: float
    Underlying .Net type is double

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set3"></a>

#### Set3

```python
@staticmethod
def Set3(stateStorage: c_void_p, key: str, value: str) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

value: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set4"></a>

#### Set4

```python
@staticmethod
def Set4(stateStorage: c_void_p, key: str, value: c_void_p) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set5"></a>

#### Set5

```python
@staticmethod
def Set5(stateStorage: c_void_p, key: str, value: bool) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

value: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Set6"></a>

#### Set6

```python
@staticmethod
def Set6(stateStorage: c_void_p, key: str, value: c_void_p) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

value: c_void_p
    GC Handle Pointer to .Net type StateValue

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Get"></a>

#### Get

```python
@staticmethod
def Get(stateStorage: c_void_p, key: str) -> c_void_p
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetDouble"></a>

#### GetDouble

```python
@staticmethod
def GetDouble(stateStorage: c_void_p, key: str) -> float
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetString"></a>

#### GetString

```python
@staticmethod
def GetString(stateStorage: c_void_p, key: str) -> str
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetBool"></a>

#### GetBool

```python
@staticmethod
def GetBool(stateStorage: c_void_p, key: str) -> bool
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetLong"></a>

#### GetLong

```python
@staticmethod
def GetLong(stateStorage: c_void_p, key: str) -> int
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetBinary"></a>

#### GetBinary

```python
@staticmethod
def GetBinary(stateStorage: c_void_p, key: str) -> c_void_p
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type byte[]

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Remove"></a>

#### Remove

```python
@staticmethod
def Remove(stateStorage: c_void_p, key: str) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.ContainsKey"></a>

#### ContainsKey

```python
@staticmethod
def ContainsKey(stateStorage: c_void_p, key: str) -> bool
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

key: str
    Underlying .Net type is string

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.GetAllKeys"></a>

#### GetAllKeys

```python
@staticmethod
def GetAllKeys(stateStorage: c_void_p) -> c_void_p
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type string[]

<a id="quixstreams.native.Python.QuixStreamsState.Storage.StorageExtensions.StorageExtensions.Clear"></a>

#### Clear

```python
@staticmethod
def Clear(stateStorage: c_void_p) -> None
```

Parameters
----------

stateStorage: c_void_p
    GC Handle Pointer to .Net type IStateStorage

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage"></a>

# quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage

<a id="quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage.IStateStorage"></a>

## IStateStorage Objects

```python
class IStateStorage(object)
```

<a id="quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage.IStateStorage.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStateStorage

Returns
----------

IStateStorage:
    Instance wrapping the .net type IStateStorage

<a id="quixstreams.native.Python.QuixStreamsState.Storage.IStateStorage.IStateStorage.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IStateStorage

Returns
----------

IStateStorage:
    Instance wrapping the .net type IStateStorage

<a id="quixstreams.native.Python.QuixStreamsState.StateValue_StateType"></a>

# quixstreams.native.Python.QuixStreamsState.StateValue\_StateType

<a id="quixstreams.native.Python.QuixStreamsState.StateValue"></a>

# quixstreams.native.Python.QuixStreamsState.StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue"></a>

## StateValue Objects

```python
class StateValue(object)
```

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StateValue

Returns
----------

StateValue:
    Instance wrapping the .net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type StateValue

Returns
----------

StateValue:
    Instance wrapping the .net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(value: bool) -> c_void_p
```

Parameters
----------

value: bool
    Underlying .Net type is Boolean

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2(value: int) -> c_void_p
```

Parameters
----------

value: int
    Underlying .Net type is long

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor3"></a>

#### Constructor3

```python
@staticmethod
def Constructor3(value: c_void_p) -> c_void_p
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor4"></a>

#### Constructor4

```python
@staticmethod
def Constructor4(value: c_void_p, type: StateType) -> c_void_p
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type byte[]

type: StateType
    Underlying .Net type is StateValue.StateType

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor5"></a>

#### Constructor5

```python
@staticmethod
def Constructor5(value: str) -> c_void_p
```

Parameters
----------

value: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Constructor6"></a>

#### Constructor6

```python
@staticmethod
def Constructor6(value: float) -> c_void_p
```

Parameters
----------

value: float
    Underlying .Net type is double

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type StateValue

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_Type"></a>

#### get\_Type

```python
def get_Type() -> StateType
```

Parameters
----------

Returns
-------

StateType:
    Underlying .Net type is StateValue.StateType

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_DoubleValue"></a>

#### get\_DoubleValue

```python
def get_DoubleValue() -> float
```

Parameters
----------

Returns
-------

float:
    Underlying .Net type is double

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_LongValue"></a>

#### get\_LongValue

```python
def get_LongValue() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_StringValue"></a>

#### get\_StringValue

```python
def get_StringValue() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_BoolValue"></a>

#### get\_BoolValue

```python
def get_BoolValue() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.get_BinaryValue"></a>

#### get\_BinaryValue

```python
def get_BinaryValue() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type byte[]

<a id="quixstreams.native.Python.QuixStreamsState.StateValue.StateValue.Equals"></a>

#### Equals

```python
def Equals(other: c_void_p) -> bool
```

Parameters
----------

other: c_void_p
    GC Handle Pointer to .Net type StateValue

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.SystemNetPrimitives.System.Net.HttpStatusCode"></a>

# quixstreams.native.Python.SystemNetPrimitives.System.Net.HttpStatusCode

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter"></a>

# quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter"></a>

## IByteSplitter Objects

```python
class IByteSplitter(object)
```

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IByteSplitter

Returns
----------

IByteSplitter:
    Instance wrapping the .net type IByteSplitter

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IByteSplitter

Returns
----------

IByteSplitter:
    Instance wrapping the .net type IByteSplitter

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.IByteSplitter.IByteSplitter.Split"></a>

#### Split

```python
def Split(msgBytes: c_void_p) -> c_void_p
```

Parameters
----------

msgBytes: c_void_p
    GC Handle Pointer to .Net type byte[]

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type IEnumerable<byte[]>

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions"></a>

# quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions"></a>

## CommitOptions Objects

```python
class CommitOptions(object)
```

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type CommitOptions

Returns
----------

CommitOptions:
    Instance wrapping the .net type CommitOptions

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type CommitOptions

Returns
----------

CommitOptions:
    Instance wrapping the .net type CommitOptions

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type CommitOptions

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.get_AutoCommitEnabled"></a>

#### get\_AutoCommitEnabled

```python
def get_AutoCommitEnabled() -> bool
```

Parameters
----------

Returns
-------

bool:
    Underlying .Net type is Boolean

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.set_AutoCommitEnabled"></a>

#### set\_AutoCommitEnabled

```python
def set_AutoCommitEnabled(value: bool) -> None
```

Parameters
----------

value: bool
    Underlying .Net type is Boolean

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.get_CommitInterval"></a>

#### get\_CommitInterval

```python
def get_CommitInterval() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is int?

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.set_CommitInterval"></a>

#### set\_CommitInterval

```python
def set_CommitInterval(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is int?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.get_CommitEvery"></a>

#### get\_CommitEvery

```python
def get_CommitEvery() -> Optional[int]
```

Parameters
----------

Returns
-------

Optional[int]:
    Underlying .Net type is int?

<a id="quixstreams.native.Python.QuixStreamsTransport.Fw.CommitOptions.CommitOptions.set_CommitEvery"></a>

#### set\_CommitEvery

```python
def set_CommitEvery(value: Optional[int]) -> None
```

Parameters
----------

value: Optional[int]
    Underlying .Net type is int?

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.IProducer"></a>

# quixstreams.native.Python.QuixStreamsTransport.IO.IProducer

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.IProducer.IProducer"></a>

## IProducer Objects

```python
class IProducer(object)
```

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.IProducer.IProducer.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IProducer

Returns
----------

IProducer:
    Instance wrapping the .net type IProducer

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.IProducer.IProducer.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type IProducer

Returns
----------

IProducer:
    Instance wrapping the .net type IProducer

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext"></a>

# quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext"></a>

## TransportContext Objects

```python
class TransportContext(object)
```

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TransportContext

Returns
----------

TransportContext:
    Instance wrapping the .net type TransportContext

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type TransportContext

Returns
----------

TransportContext:
    Instance wrapping the .net type TransportContext

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(dictionary: c_void_p) -> c_void_p
```

Parameters
----------

dictionary: c_void_p
    GC Handle Pointer to .Net type IDictionary<string, Object>

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TransportContext

<a id="quixstreams.native.Python.QuixStreamsTransport.IO.TransportContext.TransportContext.Constructor2"></a>

#### Constructor2

```python
@staticmethod
def Constructor2() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TransportContext

<a id="quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging"></a>

# quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging

<a id="quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging.Logging"></a>

## Logging Objects

```python
class Logging(object)
```

<a id="quixstreams.native.Python.QuixStreamsTransport.QuixStreams.Logging.Logging.UpdateFactory"></a>

#### UpdateFactory

```python
@staticmethod
def UpdateFactory(logLevel: LogLevel) -> None
```

Parameters
----------

logLevel: LogLevel
    Underlying .Net type is LogLevel

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.MicrosoftExtensionsLoggingAbstractions.Microsoft.Extensions.Logging.LogLevel"></a>

# quixstreams.native.Python.MicrosoftExtensionsLoggingAbstractions.Microsoft.Extensions.Logging.LogLevel

<a id="quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs"></a>

# quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs"></a>

## PropertyChangedEventArgs Objects

```python
class PropertyChangedEventArgs(object)
```

<a id="quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type PropertyChangedEventArgs

Returns
----------

PropertyChangedEventArgs:
Instance wrapping the .net type PropertyChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type PropertyChangedEventArgs

Returns
----------

PropertyChangedEventArgs:
Instance wrapping the .net type PropertyChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor(propertyName: str) -> c_void_p
```

Parameters
----------

propertyName: str
    Underlying .Net type is string

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type PropertyChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.ComponentModel.PropertyChangedEventArgs.PropertyChangedEventArgs.get_PropertyName"></a>

#### get\_PropertyName

```python
def get_PropertyName() -> str
```

Parameters
----------

Returns
-------

str:
    Underlying .Net type is string

<a id="quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs"></a>

# quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs"></a>

## NotifyCollectionChangedEventArgs Objects

```python
class NotifyCollectionChangedEventArgs(object)
```

<a id="quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type NotifyCollectionChangedEventArgs

Returns
----------

NotifyCollectionChangedEventArgs:
Instance wrapping the .net type NotifyCollectionChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
GC Handle Pointer to .Net type NotifyCollectionChangedEventArgs

Returns
----------

NotifyCollectionChangedEventArgs:
Instance wrapping the .net type NotifyCollectionChangedEventArgs

<a id="quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.get_NewStartingIndex"></a>

#### get\_NewStartingIndex

```python
def get_NewStartingIndex() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemObjectModel.System.Collections.Specialized.NotifyCollectionChangedEventArgs.NotifyCollectionChangedEventArgs.get_OldStartingIndex"></a>

#### get\_OldStartingIndex

```python
def get_OldStartingIndex() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is int

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient"></a>

# quixstreams.native.Python.SystemNetHttp.HttpClient

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient"></a>

## HttpClient Objects

```python
class HttpClient(object)
```

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.__new__"></a>

#### \_\_new\_\_

```python
def __new__(cls, net_pointer: c_void_p)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type HttpClient

Returns
----------

HttpClient:
    Instance wrapping the .net type HttpClient

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.__init__"></a>

#### \_\_init\_\_

```python
def __init__(net_pointer: c_void_p, finalize: bool = True)
```

Parameters
----------

net_pointer: c_void_p
    GC Handle Pointer to .Net type HttpClient

Returns
----------

HttpClient:
    Instance wrapping the .net type HttpClient

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.Constructor"></a>

#### Constructor

```python
@staticmethod
def Constructor() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type HttpClient

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.get_BaseAddress"></a>

#### get\_BaseAddress

```python
def get_BaseAddress() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type Uri

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.set_BaseAddress"></a>

#### set\_BaseAddress

```python
def set_BaseAddress(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type Uri

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.get_Timeout"></a>

#### get\_Timeout

```python
def get_Timeout() -> c_void_p
```

Parameters
----------

Returns
-------

c_void_p:
    GC Handle Pointer to .Net type TimeSpan

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.set_Timeout"></a>

#### set\_Timeout

```python
def set_Timeout(value: c_void_p) -> None
```

Parameters
----------

value: c_void_p
    GC Handle Pointer to .Net type TimeSpan

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.get_MaxResponseContentBufferSize"></a>

#### get\_MaxResponseContentBufferSize

```python
def get_MaxResponseContentBufferSize() -> int
```

Parameters
----------

Returns
-------

int:
    Underlying .Net type is long

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.set_MaxResponseContentBufferSize"></a>

#### set\_MaxResponseContentBufferSize

```python
def set_MaxResponseContentBufferSize(value: int) -> None
```

Parameters
----------

value: int
    Underlying .Net type is long

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.SystemNetHttp.HttpClient.HttpClient.CancelPendingRequests"></a>

#### CancelPendingRequests

```python
def CancelPendingRequests() -> None
```

Parameters
----------

Returns
-------
None:
    Underlying .Net type is void

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable"></a>

# quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable.Enumerable"></a>

## Enumerable Objects

```python
class Enumerable()
```

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable.Enumerable.ReadAny"></a>

#### ReadAny

```python
@staticmethod
def ReadAny(enumerable_hptr: c_void_p) -> c_void_p
```

Read any object that implements IEnumerable. Useful for cases when other enumerable methods don't fulfill the

the role in a more efficient manner

**Arguments**:

- `enumerable_hptr`: Handler pointer to an object

**Returns**:

Handle pointer to an array of values which depend on the underlying value types

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.List"></a>

# quixstreams.native.Python.InteropHelpers.ExternalTypes.System.List

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Collection"></a>

# quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Collection

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary"></a>

# quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary"></a>

## Dictionary Objects

```python
class Dictionary()
```

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadAnyHPtrToUPtr"></a>

#### ReadAnyHPtrToUPtr

```python
@staticmethod
def ReadAnyHPtrToUPtr(dictionary_hptr: c_void_p) -> c_void_p
```

Read any dictionary that implements IEnumerable<KeyValuePair<,>>. Useful for dictionaries that do not implement

IDictionary, such as ReadOnlyDictionary

**Arguments**:

- `dictionary_hptr`: Handler pointer to a dictionary

**Returns**:

Pointer to array with elements [c_void_p, c_void_p] where first is the key array, 2nd is the value array

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteBlittables"></a>

#### WriteBlittables

```python
@staticmethod
def WriteBlittables(dictionary: Dict[any, any], key_converter,
                    value_converter) -> c_void_p
```

Writes dictionary into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadBlittables"></a>

#### ReadBlittables

```python
@staticmethod
def ReadBlittables(dict_uptr: c_void_p, key_converter,
                   value_converter) -> c_void_p
```

Read a pointer into a managed dictionary. The pointer must be to a structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringPointers"></a>

#### WriteStringPointers

```python
@staticmethod
def WriteStringPointers(dictionary: Dict[str, c_void_p]) -> c_void_p
```

Writes dictionary of [str, c_void_p] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringPointers"></a>

#### ReadStringPointers

```python
@staticmethod
def ReadStringPointers(dictionary_uptr: c_void_p,
                       valuemapper=None) -> Dict[str, c_void_p]
```

Writes dictionary of [str, c_void_p] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringDoublesArray"></a>

#### WriteStringDoublesArray

```python
@staticmethod
def WriteStringDoublesArray(dictionary: Dict[str, List[float]]) -> c_void_p
```

Writes dictionary of [str, [float]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringDoublesArray"></a>

#### ReadStringDoublesArray

```python
@staticmethod
def ReadStringDoublesArray(dictionary: Dict[str, List[int]]) -> c_void_p
```

Reads unmanaged memory at address, converting it to managed [str, float[]] dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringLongsArray"></a>

#### WriteStringLongsArray

```python
@staticmethod
def WriteStringLongsArray(dictionary: Dict[str, List[int]]) -> c_void_p
```

Writes dictionary of [str, [int64]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringLongsArray"></a>

#### ReadStringLongsArray

```python
@staticmethod
def ReadStringLongsArray(dictionary: Dict[str, List[int]]) -> c_void_p
```

Reads unmanaged memory at address, converting it to managed [str, int64p[]] dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringStrings"></a>

#### WriteStringStrings

```python
@staticmethod
def WriteStringStrings(dictionary: Dict[str, str]) -> c_void_p
```

Writes dictionary of [str, str] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringStrings"></a>

#### ReadStringStrings

```python
@staticmethod
def ReadStringStrings(dictionary: Dict[str, str]) -> c_void_p
```

Reads unmanaged memory at address, converting it to managed [str, str] dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringStringsArray"></a>

#### WriteStringStringsArray

```python
@staticmethod
def WriteStringStringsArray(dictionary: Dict[str, List[str]]) -> c_void_p
```

Writes dictionary of [str, [str]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringStringsArray"></a>

#### ReadStringStringsArray

```python
@staticmethod
def ReadStringStringsArray(dictionary: Dict[str, List[str]]) -> c_void_p
```

Reads unmanaged memory at address, converting it to managed [str, str[]] dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringBytesArray"></a>

#### WriteStringBytesArray

```python
@staticmethod
def WriteStringBytesArray(dictionary: Dict[str, List[bytes]]) -> c_void_p
```

Writes dictionary of [str, [bytes]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringBytesArray"></a>

#### ReadStringBytesArray

```python
@staticmethod
def ReadStringBytesArray(dictionary: Dict[str, List[bytes]]) -> c_void_p
```

Reads unmanaged memory at address, converting it to managed [str, [bytes]] dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.WriteStringNullableDoublesArray"></a>

#### WriteStringNullableDoublesArray

```python
@staticmethod
def WriteStringNullableDoublesArray(
        dictionary: Dict[str, List[Optional[float]]]) -> c_void_p
```

Writes dictionary of [str, [Optional[float]]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary.Dictionary.ReadStringNullableDoublesArray"></a>

#### ReadStringNullableDoublesArray

```python
@staticmethod
def ReadStringNullableDoublesArray(
        dictionary: Dict[str, List[Optional[Optional[float]]]]) -> c_void_p
```

Reads unmanaged memory at address, converting it to managed [str, float[]] dictionary

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array"></a>

# quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array"></a>

## Array Objects

```python
class Array()
```

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadBlittables"></a>

#### ReadBlittables

```python
@staticmethod
def ReadBlittables(array_uptr: ctypes.c_void_p,
                   valuetype,
                   valuemapper=None) -> []
```

Reads blittable values starting from the array pointer using the specified type and mapper then frees the pointer

**Arguments**:

- `array_uptr`: Unmanaged memory pointer to the first element of the array.
- `valuetype`: Type of the value
- `valuemapper`: Conversion function for the value

**Returns**:

The converted array to the given type using the mapper

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteBlittables"></a>

#### WriteBlittables

```python
@staticmethod
def WriteBlittables(blittables: [any],
                    valuetype,
                    valuemapper=None) -> c_void_p
```

Writes a list of blittables (like int) into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadArray"></a>

#### ReadArray

```python
@staticmethod
def ReadArray(array_uptr: ctypes.c_void_p,
              valuemapper: Callable[[c_void_p], Any]) -> [[any]]
```

Reads from unmanaged memory, returning list of any sets (any[][])

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteArray"></a>

#### WriteArray

```python
@staticmethod
def WriteArray(values: [[any]], valuemapper: Callable[[Any],
                                                      c_void_p]) -> c_void_p
```

Writes an array of any sets (any[][]) into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadNullables"></a>

#### ReadNullables

```python
@staticmethod
def ReadNullables(array_uptr: ctypes.c_void_p, nullable_type) -> [Any]
```

Parameters
----------

array_ptr: c_void_p
    Pointer to .Net nullable array.

nullable_type:
    nullable type created by InteropUtils.create_nullable

Returns
-------
[]:
   array of underlying type with possible None values

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadLongs"></a>

#### ReadLongs

```python
@staticmethod
def ReadLongs(array_uptr: ctypes.c_void_p) -> [int]
```

Reads from unmanaged memory, returning list of int64

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadLongsArray"></a>

#### ReadLongsArray

```python
@staticmethod
def ReadLongsArray(array_uptr: ctypes.c_void_p) -> [[int]]
```

Reads from unmanaged memory, returning list of int64 lists

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteLongs"></a>

#### WriteLongs

```python
@staticmethod
def WriteLongs(longs: [int]) -> c_void_p
```

Writes list of int64 into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteLongsArray"></a>

#### WriteLongsArray

```python
@staticmethod
def WriteLongsArray(longs_array: [[int]]) -> c_void_p
```

Writes list of int64 lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadStrings"></a>

#### ReadStrings

```python
@staticmethod
def ReadStrings(array_uptr: ctypes.c_void_p) -> [str]
```

Reads from unmanaged memory, returning list of str

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadStringsArray"></a>

#### ReadStringsArray

```python
@staticmethod
def ReadStringsArray(array_uptr: ctypes.c_void_p) -> [[str]]
```

Reads from unmanaged memory, returning list of str lists

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteStrings"></a>

#### WriteStrings

```python
@staticmethod
def WriteStrings(strings: [str]) -> c_void_p
```

Writes list of str into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteStringsArray"></a>

#### WriteStringsArray

```python
@staticmethod
def WriteStringsArray(strings_array: [[str]]) -> c_void_p
```

Writes list of str lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadDoubles"></a>

#### ReadDoubles

```python
@staticmethod
def ReadDoubles(array_uptr: ctypes.c_void_p) -> [float]
```

Reads from unmanaged memory, returning list of double (float)

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadDoublesArray"></a>

#### ReadDoublesArray

```python
@staticmethod
def ReadDoublesArray(array_uptr: ctypes.c_void_p) -> [[float]]
```

Reads from unmanaged memory, returning list of double (float) lists

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteDoubles"></a>

#### WriteDoubles

```python
@staticmethod
def WriteDoubles(doubles: [float]) -> c_void_p
```

Writes list of double (float) into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteDoublesArray"></a>

#### WriteDoublesArray

```python
@staticmethod
def WriteDoublesArray(doubles_array: [[float]]) -> c_void_p
```

Writes list of double (float) lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadPointers"></a>

#### ReadPointers

```python
@staticmethod
def ReadPointers(pointers: [c_void_p]) -> c_void_p
```

Reads from unmanaged memory, returning list of pointers

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadPointersArray"></a>

#### ReadPointersArray

```python
@staticmethod
def ReadPointersArray(array_uptr: ctypes.c_void_p) -> [[c_void_p]]
```

Reads from unmanaged memory, returning list of pointer lists

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WritePointers"></a>

#### WritePointers

```python
@staticmethod
def WritePointers(pointers: [c_void_p]) -> c_void_p
```

Writes list of pointer into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WritePointersArray"></a>

#### WritePointersArray

```python
@staticmethod
def WritePointersArray(pointers_array: [[c_void_p]]) -> c_void_p
```

Writes list of pointer lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadBytes"></a>

#### ReadBytes

```python
@staticmethod
def ReadBytes(array_uptr: ctypes.c_void_p) -> bytes
```

Reads from unmanaged memory, returning bytes

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadBytesArray"></a>

#### ReadBytesArray

```python
@staticmethod
def ReadBytesArray(array_uptr: ctypes.c_void_p) -> [bytes]
```

Reads from unmanaged memory, returning list of bytes

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteBytes"></a>

#### WriteBytes

```python
@staticmethod
def WriteBytes(bytes_value: Union[bytes, bytearray]) -> c_void_p
```

Writes list of bytes into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteBytesArray"></a>

#### WriteBytesArray

```python
@staticmethod
def WriteBytesArray(
        bytes_array: Union[List[bytes], List[bytearray]]) -> c_void_p
```

Writes list of bytes into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadNullableDoubles"></a>

#### ReadNullableDoubles

```python
@staticmethod
def ReadNullableDoubles(array_uptr: ctypes.c_void_p) -> [Optional[float]]
```

Reads from unmanaged memory, returning list of Optional[float]

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.ReadNullableDoublesArray"></a>

#### ReadNullableDoublesArray

```python
@staticmethod
def ReadNullableDoublesArray(
        array_uptr: ctypes.c_void_p) -> [[Optional[float]]]
```

Reads from unmanaged memory, returning list of Optional[float] lists

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteNullableDoubles"></a>

#### WriteNullableDoubles

```python
@staticmethod
def WriteNullableDoubles(nullable_doubles: [Optional[float]]) -> c_void_p
```

Writes list of Optional[float] into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array.Array.WriteNullableDoublesArray"></a>

#### WriteNullableDoublesArray

```python
@staticmethod
def WriteNullableDoublesArray(
        nullable_doubles_array: [[Optional[float]]]) -> c_void_p
```

Writes list of int64 lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils"></a>

# quixstreams.native.Python.InteropHelpers.InteropUtils

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils"></a>

## InteropUtils Objects

```python
class InteropUtils(object)
```

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.set_exception_callback"></a>

#### set\_exception\_callback

```python
@staticmethod
def set_exception_callback(callback: Callable[[InteropException], None])
```

Sets the exception handler for interop exceptions

callback: Callable[[InteropException], None]
    The callback which takes InteropException and returns nothing

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.log_debug"></a>

#### log\_debug

```python
@staticmethod
def log_debug(message: str)
```

Logs debug message if debugging is enabled

message: str
    The message to log

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.enable_debug"></a>

#### enable\_debug

```python
@staticmethod
def enable_debug()
```

Enables Debugging logs

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.disable_debug"></a>

#### disable\_debug

```python
@staticmethod
def disable_debug()
```

Enables Debugging logs

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.pin_hptr_target"></a>

#### pin\_hptr\_target

```python
@staticmethod
def pin_hptr_target(hptr: c_void_p) -> c_void_p
```

Creates a GC Handle with pinned type.
Use get_pin_address to acquire the pinned resources's address
Must be freed as soon as possible with free_hptr

Parameters
----------

hptr: c_void_p
    Pointer to .Net GC Handle

Returns
-------
c_void_p:
    .Net pointer to the value

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.get_pin_address"></a>

#### get\_pin\_address

```python
@staticmethod
def get_pin_address(hptr: c_void_p) -> c_void_p
```

Retrieves the address of the pinned resource from the provided GC Handle ptr

Parameters
----------

hptr: c_void_p
    Pointer to .Net pinned GC Handle

Returns
-------
c_void_p:
    memory address of the underlying resource

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.free_hptr"></a>

#### free\_hptr

```python
@staticmethod
def free_hptr(hptr: c_void_p) -> None
```

Frees the provided GC Handle

Parameters
----------

hptr: c_void_p
    Pointer to .Net GC Handle

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.free_uptr"></a>

#### free\_uptr

```python
@staticmethod
def free_uptr(uptr: c_void_p) -> None
```

Frees the provided unmanaged pointer

Parameters
----------

uptr: c_void_p
    Unmanaged pointer

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.allocate_uptr"></a>

#### allocate\_uptr

```python
@staticmethod
def allocate_uptr(size: int) -> c_void_p
```

Allocated unmanaged memory pointer of the desired size

Parameters
----------

size: c_void_p
    The desired size of the memory

Returns
-------
c_void_p:
    Unmanaged memory pointer

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.invoke_and_free"></a>

#### invoke\_and\_free

```python
@staticmethod
def invoke_and_free(hptr: ctypes.c_void_p, func, *args, **kwargs)
```

Invokes a function where first positional argument is a c# GC Handle pointer (hptr), then disposes it

Parameters
----------

hptr: ctypes.c_void_p
    c# GC Handle pointer (hptr)

func:
    callable function where first arg is the hptr

Returns
-------
Any:
    the function result

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.dict_nullables"></a>

#### dict\_nullables

TODO Should I lock on this?

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.create_nullable"></a>

#### create\_nullable

```python
@staticmethod
def create_nullable(underlying_ctype)
```

Create a nullable type that is equivalent for serialization purposes to a nullable c# type in places where it is 
serialized as a struct. In this case byte prefix is added equivalent to minimum addressable memory size according
to cpu architecture. For example a nullable double on 64 bit would be 8 bytes for boolean and 8 bytes for double.

Parameters
----------

underlying_ctype:
    type defined by ctypes such as ctypes.c_double

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.dict_mem_nullables"></a>

#### dict\_mem\_nullables

TODO Should I lock on this?

<a id="quixstreams.native.Python.InteropHelpers.InteropUtils.InteropUtils.create_mem_nullable"></a>

#### create\_mem\_nullable

```python
@staticmethod
def create_mem_nullable(underlying_ctype)
```

Create a nullable type that is equivalent for serialization purposes to a nullable c# type in places where it is not 
serialized as a struct, but rather as a continuous memory segment to pointed at by either a pointer or array.
In this case 1 byte prefix is added kept for the boolean flag rather than the min addressable memory size according
to cpu architecture. For example a nullable double on 64 bit would be 1 byte for boolean and 8 bytes for double.

Parameters
----------

underlying_ctype:
    type defined by ctypes such as ctypes.c_double

<a id="quixstreams.builders"></a>

# quixstreams.builders

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
    parameter_id: str, value: Union[str, float, int, bytes,
                                    bytearray]) -> 'TimeseriesDataBuilder'
```

Adds new parameter value at the time the builder is created for.

**Arguments**:

- `parameter_id` - The id of the parameter to set the value for.
- `value` - The value of type string, float, int, bytes, or bytearray.
  

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

<a id="quixstreams.models.streamproducer"></a>

# quixstreams.models.streamproducer

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
        packet: Union[TimeseriesData, pd.DataFrame,
                      TimeseriesDataRaw]) -> None
```

Publish the given packet to the stream without any buffering.

**Arguments**:

- `packet` - The packet containing TimeseriesData, TimeseriesDataRaw, or pandas DataFrame.
  

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
def publish(packet: Union[TimeseriesData, pd.DataFrame]) -> None
```

Publish the provided timeseries packet to the buffer.

**Arguments**:

- `packet` - The packet containing TimeseriesData or panda DataFrame
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

<a id="quixstreams.models.streamendtype"></a>

# quixstreams.models.streamendtype

<a id="quixstreams.models.commitmode"></a>

# quixstreams.models.commitmode

<a id="quixstreams.models"></a>

# quixstreams.models

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

<a id="quixstreams.models.streamconsumer"></a>

# quixstreams.models.streamconsumer

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
    parameter_id: str, value: Union[float, str, int, bytearray,
                                    bytes]) -> 'TimeseriesDataTimestamp'
```

Adds a new value for the specified parameter.

**Arguments**:

- `parameter_id` - The parameter id to add the value for.
- `value` - The value to add. Can be float, string, int, bytearray, or bytes.
  

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

<a id="quixstreams.state.inmemorystorage"></a>

# quixstreams.state.inmemorystorage

<a id="quixstreams.state.inmemorystorage.InMemoryStorage"></a>

## InMemoryStorage Objects

```python
class InMemoryStorage()
```

In memory storage with an optional backing store

<a id="quixstreams.state"></a>

# quixstreams.state

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

<a id="quixstreams.state.localfilestorage"></a>

# quixstreams.state.localfilestorage

<a id="quixstreams.state.localfilestorage.LocalFileStorage"></a>

## LocalFileStorage Objects

```python
class LocalFileStorage(object)
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

<a id="quixstreams.state.localfilestorage.LocalFileStorage.get"></a>

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

<a id="quixstreams.state.localfilestorage.LocalFileStorage.set"></a>

#### set

```python
def set(key: str, value: Any)
```

Sets the value at the specified key.

**Arguments**:

- `key` - The key to set the value for.
- `value` - The value to be set, which can be one of the following types:
  StateValue, str, int, float, bool, bytes, bytearray, or object (via pickle).

<a id="quixstreams.state.localfilestorage.LocalFileStorage.contains_key"></a>

#### contains\_key

```python
def contains_key(key: str) -> bool
```

Checks if the storage contains the specified key.

**Arguments**:

- `key` - The key to check for.
  

**Returns**:

- `bool` - True if the storage contains the key, False otherwise.

<a id="quixstreams.state.localfilestorage.LocalFileStorage.get_all_keys"></a>

#### get\_all\_keys

```python
def get_all_keys()
```

Retrieves a set containing all the keys in the storage.

**Returns**:

- `set[str]` - A set of all keys in the storage.

<a id="quixstreams.state.localfilestorage.LocalFileStorage.remove"></a>

#### remove

```python
def remove(key) -> None
```

Removes the specified key from the storage.

**Arguments**:

- `key` - The key to be removed.

<a id="quixstreams.state.localfilestorage.LocalFileStorage.clear"></a>

#### clear

```python
def clear()
```

Clears the storage by removing all keys and their associated values.

<a id="quixstreams.exceptions.quixapiexception"></a>

# quixstreams.exceptions.quixapiexception

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

<a id="quixstreams.topicconsumer.TopicConsumer.get_net_pointer"></a>

#### get\_net\_pointer

```python
def get_net_pointer() -> ctypes.c_void_p
```

Retrieves the .net pointer to TopicConsumer instance.

**Returns**:

- `ctypes.c_void_p` - The .net pointer to TopicConsumer instance.

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

<a id="quixstreams.streamproducer.StreamProducer.close"></a>

#### close

```python
def close(end_type: StreamEndType = StreamEndType.Closed)
```

Closes the stream and flushes the pending data to stream.

**Arguments**:

- `end_type` - The type of stream end. Defaults to StreamEndType.Closed.

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

<a id="quixstreams.helpers.nativedecorator"></a>

# quixstreams.helpers.nativedecorator

<a id="quixstreams.helpers"></a>

# quixstreams.helpers

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

