using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QuixStreams.Kafka
{
    /// <summary>
    /// The interface required to implement an <see cref="IProducer{TKey,TValue}"/>, which sends <see cref="Package"/> to Kafka
    /// </summary>
    public interface IKafkaProducer: IDisposable
    {
        /// <summary>
        /// Publishes a message
        /// </summary>
        /// <param name="message">The message to publish</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting the process</param>
        Task Publish(KafkaMessage message, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Publishes a set of messages with guaranteed order
        /// </summary>
        /// <param name="messages">The messages to publish</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting the process</param>
        Task Publish(IEnumerable<KafkaMessage> messages, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Flush the queue to Kafka
        /// </summary>
        /// <param name="cancellationToken">The cancellation token for aborting flushing</param>
        void Flush(CancellationToken cancellationToken);

        /// <summary>
        /// The maximum message size the producer can handle.
        /// Packages above this size will likely throw <see cref="ProduceException{TKey,TValue}"/> with "Message size too large" message
        /// </summary>
        int MaxMessageSizeBytes { get; }
    }
}