using System;
using System.Collections.Generic;
using Confluent.Kafka;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Kafka
{
    /// <summary>
    /// The interface required to implement an <see cref="IConsumer"/>, which polls <see cref="Package"/> from Kafka
    /// </summary>
    public interface IKafkaConsumer : IConsumer
    {
        /// <summary>
        /// Raised when <see cref="Exception"/> occurred.
        /// Kafka exceptions are raised as <see cref="KafkaException"/>. See <see cref="KafkaException.Error"/> for exception details.
        /// </summary>
        event EventHandler<Exception> OnErrorOccurred;

        /// <summary>
        /// Commits all offsets for the current topic partition assignments
        /// </summary>
        void CommitOffsets();

        /// <summary>
        /// Commit a list of offsets limited by the topics this consumer had previously subscribed to
        /// </summary>
        /// <param name="offsets">The offsets to commit</param>
        void CommitOffsets(ICollection<TopicPartitionOffset> offsets);

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