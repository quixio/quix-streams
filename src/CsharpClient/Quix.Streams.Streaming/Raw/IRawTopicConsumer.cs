using System;

namespace Quix.Streams.Streaming.Raw
{
    /// <summary>
    /// Interface to subscribe to incoming raw messages (capable to read non-quixstreams messages)
    /// </summary>
    public interface IRawTopicConsumer : IDisposable
    {
        /// <summary>
        /// Start reading streams.
        /// Use 'OnRead' event to read stream after executing this method
        /// </summary>
        void Subscribe();

        /// <summary>
        /// Event raised when a message is received from the topic
        /// </summary>
        public event EventHandler<RawMessage> OnMessageRead;

        /// <summary>
        /// Event raised when a new error occurs
        /// </summary>
        public event EventHandler<Exception> OnErrorOccurred;
        
        /// <summary>
        /// Raised when the resource is disposed
        /// </summary>
        public event EventHandler OnDisposed;
    }
}