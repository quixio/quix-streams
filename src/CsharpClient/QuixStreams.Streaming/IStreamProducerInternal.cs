using System;
using System.Collections.Generic;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Stands for a new stream that we want to send to the platform.
    /// It provides you helper properties to stream data the platform like parameter values, events, definitions and all the information you can persist to the platform.
    /// </summary>
    internal interface IStreamProducerInternal: IStreamProducer
    {
        /// <summary>
        /// Default Epoch used for Parameters and Events
        /// </summary>
        DateTime Epoch { get; set; }
        
        /// <summary>
        /// Event raised before the message is being sent
        /// </summary>
        event Action<Type> OnBeforeSend;

        /// <summary>
        /// Publish a stream properties to the stream
        /// </summary>
        void Publish(QuixStreams.Telemetry.Models.StreamProperties properties);

        /// <summary>
        /// Publish a single Timeseries data package to the stream
        /// </summary>
        void Publish(QuixStreams.Telemetry.Models.TimeseriesDataRaw rawData);

        /// <summary>
        /// Publish a set of Timeseries data packages to the stream
        /// </summary>
        void Publish(List<QuixStreams.Telemetry.Models.TimeseriesDataRaw> data);

        /// <summary>
        /// Publish the optional Parameter definition properties describing the hierarchical grouping of parameters
        /// Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.
        /// </summary>
        void Publish(QuixStreams.Telemetry.Models.ParameterDefinitions definitions);

        /// <summary>
        /// Publish a single event to the stream
        /// </summary>
        /// <param name="eventDataRaw">Event to send</param>
        void Publish(QuixStreams.Telemetry.Models.EventDataRaw eventDataRaw);

        /// <summary>
        /// Publish a set of events to the stream 
        /// </summary>
        /// <param name="events">Events to send</param>
        void Publish(ICollection<QuixStreams.Telemetry.Models.EventDataRaw> events);

        /// <summary>
        /// Publish the optional Event definition properties describing the hierarchical grouping of events
        /// Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.
        /// </summary>
        void Publish(QuixStreams.Telemetry.Models.EventDefinitions definitions);
    }
}