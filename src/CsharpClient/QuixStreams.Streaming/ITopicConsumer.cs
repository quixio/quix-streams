using System;
using QuixStreams.Streaming.States;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Interface to subscribe to incoming streams
    /// </summary>
    public interface ITopicConsumer : IDisposable
    {
        /// <summary>
        /// Start subscribing to streams.
        /// Use 'OnStreamReceived' event to read stream after executing this method
        /// </summary>
        void Subscribe();
        
        /// <summary>
        /// Stops subscribing to streams.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        /// Event raised when a new stream has been received for reading.
        /// Use the Stream Reader interface received to read data from the stream.
        /// You must execute 'Subscribe' method before starting to receive streams from this event
        /// </summary>
        event EventHandler<IStreamConsumer> OnStreamReceived;
        
        /// <summary>
        /// Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point
        /// </summary>
        event EventHandler OnRevoking;

        /// <summary>
        /// Raised when the underlying source of data became unavailable for the streams affected by it
        /// </summary>
        event EventHandler<IStreamConsumer[]> OnStreamsRevoked;
        
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
        
        /// <summary>
        /// Get stream state manager
        /// </summary>
        StreamStateManager GetStreamStateManager(string streamId);
    }
}