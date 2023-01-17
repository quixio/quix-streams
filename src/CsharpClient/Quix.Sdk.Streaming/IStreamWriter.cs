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
    public interface IStreamWriter : IDisposable
    {
        /// <summary>
        /// Stream Id of the new stream created by the writer
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// Default Epoch used for Parameters and Events
        /// </summary>
        DateTime Epoch { get; set; }

        /// <summary>
        /// Properties of the stream. The changes will automatically be sent after a slight delay
        /// </summary>
        StreamPropertiesWriter Properties { get; }

        /// <summary>
        /// Helper for doing anything related to parameters of the stream. Use to send parameter definitions, groups or values. 
        /// </summary>
        StreamParametersWriter Parameters { get; }

        /// <summary>
        /// Helper for doing anything related to events of the stream. Use to send event definitions, groups or values. 
        /// </summary>
        StreamEventsWriter Events { get; }

        /// <summary>
        /// Close the stream and flush the pending data to stream.
        /// </summary>
        /// <param name="streamState">Stream closing state</param>
        void Close(Process.Models.StreamEndType streamState = Process.Models.StreamEndType.Closed);
        
        /// <summary>
        /// Event raised when an exception occurred during the writing processes
        /// </summary>
        event EventHandler<Exception> OnWriteException;
    }
}