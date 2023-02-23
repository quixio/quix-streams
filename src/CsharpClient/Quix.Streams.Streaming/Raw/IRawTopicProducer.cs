using System;

namespace Quix.Streams.Streaming.Raw
{
    /// <summary>
    /// Interface to publish raw messages into a topic (capable to write non-quixstreams messages)
    /// </summary>
    public interface IRawTopicProducer : IDisposable
    {
        /// <summary>
        /// Publish data to the topic
        /// </summary>
        public void Publish(RawMessage data);
        
        /// <summary>
        /// Raised when the resource is disposed
        /// </summary>
        public event EventHandler OnDisposed;
    }
}