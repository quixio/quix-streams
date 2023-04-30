using System;
using QuixStreams.Streaming.Models.StreamProducer;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Stands for a new stream that we want to send to the platform.
    /// It provides you helper properties to stream data the platform like parameter values, events, definitions and all the information you can persist to the platform.
    /// </summary>
    public interface IStreamProducer : IDisposable
    {
        /// <summary>
        /// Stream Id of the new stream created by the producer
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// Default Epoch used for Parameters and Events
        /// </summary>
        DateTime Epoch { get; set; }

        /// <summary>
        /// Properties of the stream. The changes will automatically be sent after a slight delay
        /// </summary>
        StreamPropertiesProducer Properties { get; }

        /// <summary>
        /// Gets the producer for publishing timeseries related information of the stream such as parameter definitions and values 
        /// </summary>
        StreamTimeseriesProducer Timeseries { get; }

        /// <summary>
        /// Gets the producer for publishing event related information of the stream such as event definitions and values  
        /// </summary>
        StreamEventsProducer Events { get; }

        /// <summary>
        /// Flush the pending data to stream.  
        /// </summary>
        void Flush();

        /// <summary>
        /// Close the stream and flush the pending data to stream.
        /// </summary>
        /// <param name="streamState">Stream closing state</param>
        void Close(QuixStreams.Telemetry.Models.StreamEndType streamState = QuixStreams.Telemetry.Models.StreamEndType.Closed);
        
        /// <summary>
        /// Event raised when an exception occurred during the publishing processes
        /// </summary>
        event EventHandler<Exception> OnWriteException;
    }
}