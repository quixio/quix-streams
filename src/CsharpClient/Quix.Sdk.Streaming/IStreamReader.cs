using System;
using Quix.Sdk.Streaming.Models.StreamReader;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Stream reader interface. Stands for a new stream read from the platform.
    /// Allows to read the stream data received from a topic.
    /// </summary>
    public interface IStreamReader
    {
        /// <summary>
        /// Gets the stream Id of the stream.
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// Gets the reader for accessing the properties and metadata of the stream
        /// </summary>
        StreamPropertiesReader Properties { get; }

        /// <summary>
        /// Gets the reader for accessing parameter related information of the stream such as definitions and parameter values 
        /// </summary>
        StreamParametersReader Parameters { get; }

        /// <summary>
        /// Gets the reader for accessing event related information of the stream such as definitions and event values 
        /// </summary>
        StreamEventsReader Events { get; }

        /// <summary>
        /// Event raised when a stream package has been received.
        /// </summary>
        event Action<IStreamReader, Process.Models.StreamPackage> OnPackageReceived;

        /// <summary>
        /// Event raised when the stream has closed.
        /// </summary>
        event Action<IStreamReader, Process.Models.StreamEndType> OnStreamClosed;


    }
}