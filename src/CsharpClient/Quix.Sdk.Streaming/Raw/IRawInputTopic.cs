using System;

namespace Quix.Sdk.Streaming.Raw
{
    /// <summary>
    /// Interface to read incoming raw messages (capable to read non-sdk messages)
    /// </summary>
    public interface IRawInputTopic : IDisposable
    {
        /// <summary>
        /// Start reading streams.
        /// Use 'OnRead' event to read stream after executing this method
        /// </summary>
        void StartReading();

        /// <summary>
        /// Event raised when a message is read from the topic
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