using System;
using System.Threading;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Kafka
{
    /// <summary>
    /// The interface required to implement an <see cref="IProducer"/>, which sends <see cref="Package"/> to Kafka
    /// </summary>
    public interface IKafkaProducer : IProducer, IDisposable
    {
        /// <summary>
        /// Flush the queue to Kafka
        /// </summary>
        /// <param name="cancellationToken">The cancellation token for aborting flushing</param>
        void Flush(CancellationToken cancellationToken);

        /// <summary>
        /// Open connection to Kafka
        /// </summary>
        void Open();
        
        /// <summary>
        /// Close connection to Kafka
        /// </summary>
        void Close();
    }
}