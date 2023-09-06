using System;
using QuixStreams.Kafka;

namespace QuixStreams.Streaming.Raw
{
    /// <summary>
    /// Interface to publish raw messages into a topic (capable to producing non-quixstreams messages)
    /// </summary>
    public interface IRawTopicProducer : IDisposable
    {
        /// <summary>
        /// Publish message to the topic
        /// </summary>
        public void Publish(KafkaMessage message);
        
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