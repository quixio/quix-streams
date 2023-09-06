using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QuixStreams.Kafka
{
    /// <summary>
    /// The interface required to implement an <see cref="IO.IKafkaConsumer"/>, which polls <see cref="Package"/> from Kafka
    /// </summary>
    public interface IKafkaConsumer : IDisposable
    {
        /// <summary>
        /// The callback that is used when the <see cref="IKafkaConsumer"/> has new package for the listener
        /// </summary>
        Func<KafkaMessage, Task> OnMessageReceived { get; set; }
        
        /// <summary>
        /// Raised when <see cref="Exception"/> occurred.
        /// Kafka exceptions are raised as <see cref="KafkaException"/>. See <see cref="KafkaException.Error"/> for exception details.
        /// </summary>
        event EventHandler<Exception> OnErrorOccurred;

        /// <summary>
        /// Commits the offsets to the consumer.
        /// </summary>
        /// <param name="partitionOffsets">The offsets to commit</param>
        void Commit(ICollection<TopicPartitionOffset> partitionOffsets);

        /// <summary>
        /// Commits all offsets for the current topic partition assignments
        /// </summary>
        void Commit();
        
        /// <summary>
        /// Event is raised when the transport context finished committing
        /// </summary>
        event EventHandler<CommittedEventArgs> OnCommitted;
        
        /// <summary>
        /// Event is raised when the transport context starts committing. It is not guaranteed to be raised if underlying broker initiates commit on its own
        /// </summary>
        event EventHandler<CommittingEventArgs> OnCommitting;
        
        /// <summary>
        /// Raised when losing access to source depending on implementation
        /// Argument is the state which describes what is being revoked, depending on implementation
        /// </summary>
        event EventHandler<RevokingEventArgs> OnRevoking;

        /// <summary>
        /// Raised when lost access to source depending on implementation
        /// Argument is the state which describes what got revoked, depending on implementation
        /// </summary>
        event EventHandler<RevokedEventArgs> OnRevoked;

        /// <summary>
        /// Open connection to Kafka
        /// </summary>
        void Open();
        
        /// <summary>
        /// Close connection to Kafka
        /// </summary>
        void Close();
    }

    public class CommittedEventArgs: EventArgs
    {
        public CommittedEventArgs(CommittedOffsets committedOffsets)
        {
            this.Committed = committedOffsets;
        }
        
        
        public CommittedOffsets Committed { get; }
    }

    public class CommittingEventArgs: EventArgs
    {
        public CommittingEventArgs(ICollection<TopicPartitionOffset> committingOffsets)
        {
            this.Committing = committingOffsets;
        }
        
        
        public ICollection<TopicPartitionOffset> Committing { get; }
    }

    public class RevokingEventArgs: EventArgs
    {
        public RevokingEventArgs(ICollection<TopicPartitionOffset> revokingOffsets)
        {
            this.Revoking = revokingOffsets;
        }
        
        
        public ICollection<TopicPartitionOffset> Revoking { get; }
    }

    public class RevokedEventArgs : EventArgs
    {
        public RevokedEventArgs(ICollection<TopicPartitionOffset> revokedOffsets)
        {
            this.Revoked = revokedOffsets;
        }
        
        
        public ICollection<TopicPartitionOffset> Revoked { get; }
    }
}