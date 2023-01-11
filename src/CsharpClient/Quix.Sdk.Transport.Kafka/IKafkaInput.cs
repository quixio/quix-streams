using System;
using System.Threading;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport.Kafka
{
    /// <summary>
    /// The interface required to implement an <see cref="IInput"/>, which sends <see cref="Package"/> to Kafka
    /// </summary>
    public interface IKafkaInput : IInput, IDisposable
    {
        /// <summary>
        /// Close connection to Kafka
        /// </summary>
        void Close();

        /// <summary>
        /// Flush the queue to Kafka
        /// </summary>
        /// <param name="cancellationToken">The cancellation token for aborting flushing</param>
        void Flush(CancellationToken cancellationToken);

        /// <summary>
        /// Open connection to Kafka
        /// </summary>
        void Open();
    }
}