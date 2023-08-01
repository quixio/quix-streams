using System;

namespace QuixStreams.Streaming.Raw
{
    /// <summary>
    /// Interface to publish raw messages into a topic (capable to producing non-quixstreams messages)
    /// </summary>
    public interface IRawTopicProducer : IDisposable
    {
        /// <summary>
        /// Publish data to the topic
        /// </summary>
        public void Publish(RawMessage data);
        
        /// <summary>
        /// Flushes pending messages to the broker
        /// </summary>
        void Flush();
        
        /// <summary>
        /// Raised when the resource is disposed
        /// </summary>
        public event EventHandler OnDisposed;
    }
}