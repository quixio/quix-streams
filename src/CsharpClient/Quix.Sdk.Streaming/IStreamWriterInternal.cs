using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamWriter;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Stands for a new stream that we want to send to the platform.
    /// It provides you helper properties to stream data the platform like parameter values, events, definitions and all the information you can persist to the platform.
    /// </summary>
    internal interface IStreamWriterInternal
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
        /// Write a stream properties to the stream
        /// </summary>
        void Write(Process.Models.StreamProperties properties);

        /// <summary>
        /// Write a single Parameter data package to the stream
        /// </summary>
        void Write(Process.Models.ParameterDataRaw rawData);

        /// <summary>
        /// Write a set of Parameter data packages to the stream
        /// </summary>
        void Write(List<Process.Models.ParameterDataRaw> data);

        /// <summary>
        /// Write the optional Parameter definition properties describing the hierarchical grouping of parameters
        /// Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.
        /// </summary>
        void Write(Process.Models.ParameterDefinitions definitions);

        /// <summary>
        /// Write a single event to the stream
        /// </summary>
        /// <param name="eventDataRaw">Event to send</param>
        void Write(Process.Models.EventDataRaw eventDataRaw);

        /// <summary>
        /// Write a set of events to the stream 
        /// </summary>
        /// <param name="events">Events to send</param>
        void Write(ICollection<Process.Models.EventDataRaw> events);

        /// <summary>
        /// Write the optional Event definition properties describing the hierarchical grouping of events
        /// Please note, new calls will not result in merged set with previous calls. New calls supersede previously sent values.
        /// </summary>
        void Write(Process.Models.EventDefinitions definitions);
    }
}