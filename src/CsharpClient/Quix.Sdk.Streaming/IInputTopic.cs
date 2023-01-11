using System;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Interface to read incoming streams
    /// </summary>
    public interface IInputTopic : IDisposable
    {
        /// <summary>
        /// Start reading streams.
        /// Use 'OnStreamReceived' event to read stream after executing this method
        /// </summary>
        void StartReading();

        /// <summary>
        /// Event raised when a new stream has been received for reading.
        /// Use the Stream Reader interface received to read data from the stream.
        /// You must execute 'StartReading' method before starting to receive streams from this event
        /// </summary>
        event EventHandler<IStreamReader> OnStreamReceived;
        
        /// <summary>
        /// Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point
        /// </summary>
        event EventHandler OnRevoking;

        /// <summary>
        /// Raised when the underlying source of data became unavailable for the streams affected by it
        /// </summary>
        event EventHandler<IStreamReader[]> OnStreamsRevoked;
        
        /// <summary>
        /// Raised when underlying source committed data read up to this point
        /// </summary>
        event EventHandler OnCommitted;
        
        /// <summary>
        /// Raised when underlying source is about to commit data read up to this point
        /// </summary>
        event EventHandler OnCommitting;

        /// <summary>
        /// Commit packages read up until now
        /// </summary>
        void Commit();
        
        /// <summary>
        /// Raised when the resource is disposed
        /// </summary>
        public event EventHandler OnDisposed;
    }
}