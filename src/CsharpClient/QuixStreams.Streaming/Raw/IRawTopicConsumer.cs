using System;
using QuixStreams.Kafka;

namespace QuixStreams.Streaming.Raw
{
    /// <summary>
    /// Interface to subscribe to incoming raw messages (capable to read non-quixstreams messages)
    /// </summary>
    public interface IRawTopicConsumer : IDisposable
    {
        /// <summary>
        /// Start reading data from the topic.
        /// Use 'OnMessageReceived' event to read messages after executing this method
        /// </summary>
        void Subscribe();
        
        /// <summary>
        /// Stops reading data from the topic.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        /// Event raised when a message is received from the topic
        /// </summary>
        public event EventHandler<KafkaMessage> OnMessageReceived;

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