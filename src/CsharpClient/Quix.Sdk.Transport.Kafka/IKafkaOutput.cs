using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Transport.Kafka
{
    /// <summary>
    /// The interface required to implement an <see cref="IOutput"/>, which polls <see cref="Package"/> from Kafka
    /// </summary>
    public interface IKafkaOutput : IOutput
    {
        /// <summary>
        /// Raised when <see cref="Exception"/> occurred.
        /// Kafka exceptions are raised as <see cref="KafkaException"/>. See <see cref="KafkaException.Error"/> for exception details.
        /// </summary>
        event EventHandler<Exception> ErrorOccurred;

        /// <summary>
        /// Close connection to Kafka
        /// </summary>
        void Close();

        /// <summary>
        /// Commits all offsets for the current topic partition assignments
        /// </summary>
        void CommitOffsets();

        /// <summary>
        ///     Commit a list of offsets limited by the topics this output had previously subscribed to
        /// </summary>
        /// <param name="offsets">The offsets to commit</param>
        void CommitOffsets(ICollection<TopicPartitionOffset> offsets);

        /// <summary>
        /// Open connection to Kafka
        /// </summary>
        void Open();
    }
}