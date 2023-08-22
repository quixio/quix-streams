using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using QuixStreams.Kafka.Transport.SerDes;

namespace QuixStreams.Kafka.Transport
{
    /// <summary>
    /// The interface required by the class implementing deserialization into packaged from kafka
    /// </summary>
    public interface IKafkaTransportConsumer : IDisposable
    {
        /// <summary>
        /// The callback that is used when the <see cref="IKafkaTransportConsumer"/> has new transportPackage for the listener
        /// </summary>
        Func<TransportPackage, Task> PackageReceived { get; set; }
        
        /// <summary>
        /// Raised when <see cref="Exception"/> occurred.
        /// Kafka exceptions are raised as <see cref="KafkaException"/>. See <see cref="KafkaException.Error"/> for exception details.
        /// </summary>
        event EventHandler<Exception> ErrorOccurred;

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
        event EventHandler<CommittedEventArgs> Committed;
        
        /// <summary>
        /// Event is raised when the transport context starts committing. It is not guaranteed to be raised if underlying broker initiates commit on its own
        /// </summary>
        event EventHandler<CommittingEventArgs> Committing;
        
        /// <summary>
        /// Raised when losing access to source depending on implementation
        /// Argument is the state which describes what is being revoked, depending on implementation
        /// </summary>
        event EventHandler<RevokingEventArgs> Revoking;

        /// <summary>
        /// Raised when lost access to source depending on implementation
        /// Argument is the state which describes what got revoked, depending on implementation
        /// </summary>
        event EventHandler<RevokedEventArgs> Revoked;

        /// <summary>
        /// Open connection to Kafka
        /// </summary>
        void Open();
        
        /// <summary>
        /// Close connection to Kafka
        /// </summary>
        void Close();
    }
    
    /// <summary>
    /// A transport pipeline which deserializes and merges the packages and raises the messages
    /// </summary>
    public class KafkaTransportConsumer : IKafkaTransportConsumer
    {
        private readonly IKafkaConsumer kafkaConsumer;
        private readonly Action closeAction = () => { };
        private readonly Action<TransportContext[]> commitAction = (context) => { };

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaTransportConsumer"/>, which listens to the specified <see cref="IKafkaConsumer"/>
        /// </summary>
        /// <param name="kafkaConsumer">The consumer to listen to</param>
        public KafkaTransportConsumer(IKafkaConsumer kafkaConsumer) : this(kafkaConsumer, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaTransportConsumer"/>, which listens to the specified <see cref="IKafkaConsumer"/>
        /// </summary>
        /// <param name="kafkaConsumer">The consumer to listen to</param>
        /// <param name="configureOptions"></param>
        public KafkaTransportConsumer(IKafkaConsumer kafkaConsumer, Action<TransportConsumerOptions> configureOptions)
        {
            this.kafkaConsumer = kafkaConsumer ?? throw new ArgumentNullException(nameof(kafkaConsumer));
            var options = new TransportConsumerOptions();
            configureOptions?.Invoke(options);

            // consumer -> merger -> deserializer -> commitModifier -> raise
            
            
            var buffer = new KafkaMessageBuffer();
            var merger = new KafkaMessageMerger(buffer);

            kafkaConsumer.MessageReceived += message => merger.Merge(message);
            
            var deserializer = new PackageDeserializer();

            if (options.CommitOptions?.AutoCommitEnabled ?? false)
            {
                var commitModifier = new AutoCommitter(options.CommitOptions, this.kafkaConsumer.Commit);
                merger.MessageAvailable += message =>
                {
                    var package = deserializer.Deserialize(message);
                    return commitModifier.Publish(package);
                };
                closeAction = () => commitModifier.Close();

                commitModifier.PackageAvailable += package => this.PackageReceived?.Invoke(package);

                kafkaConsumer.OnRevoked += (sender, args) =>
                {
                    try
                    {
                        merger.HandleRevoked(args);
                        commitModifier.HandleRevoked(args);
                    }
                    finally
                    {
                        this.Revoked?.Invoke(sender, args);
                    }
                };
                
                kafkaConsumer.OnRevoking += (sender, args) =>
                {
                    try
                    {
                        commitModifier.HandleRevoking(args);
                    }
                    finally
                    {
                        this.Revoking?.Invoke(sender, args);
                    }
                };
                
                kafkaConsumer.OnCommitted += (sender, args) =>
                {
                    try
                    {
                        commitModifier.HandleCommitted(args);
                    }
                    finally
                    {
                        this.Committed?.Invoke(sender, args);
                    }
                };
                
                kafkaConsumer.OnCommitting += (sender, args) =>
                {
                    try
                    {
                        commitModifier.HandleCommitting(args);
                    }
                    finally
                    {
                        this.Committing?.Invoke(sender, args);
                    }
                };
            }
            else
            {
                merger.MessageAvailable += message =>
                {
                    var package = deserializer.Deserialize(message);
                    return this.PackageReceived?.Invoke(package);
                };
                
                kafkaConsumer.OnCommitted += (sender, args) =>
                {
                    this.Committed?.Invoke(sender, args);
                        
                };
                
                kafkaConsumer.OnCommitting += (sender, args) =>
                {
                    this.Committing?.Invoke(sender, args);
                };
            }

            kafkaConsumer.OnErrorOccurred += (sender, exception) =>
            {
                this.ErrorOccurred?.Invoke(sender, exception);
            };
        }

        /// <inheritdoc/>
        public Func<TransportPackage, Task> PackageReceived { get; set; }
        
        /// <inheritdoc/>
        public event EventHandler<Exception> ErrorOccurred;

        /// <inheritdoc/>
        public void Commit(ICollection<TopicPartitionOffset> partitionOffsets)
        {
            this.kafkaConsumer.Commit(partitionOffsets);
        }

        /// <inheritdoc/>
        public void Commit()
        {
            this.kafkaConsumer.Commit();
        }

        /// <inheritdoc/>
        public event EventHandler<CommittedEventArgs> Committed;
        
        /// <inheritdoc/>
        public event EventHandler<CommittingEventArgs> Committing;

        /// <inheritdoc/>
        public void Open()
        {
            this.kafkaConsumer.Open();
        }

        /// <summary>
        /// Close transport consumer
        /// </summary>
        public void Close()
        {
            this.closeAction();
        }

        /// <inheritdoc/>
        public event EventHandler<RevokingEventArgs> Revoking;
        
        /// <inheritdoc/>
        public event EventHandler<RevokedEventArgs> Revoked;
        
        /// <inheritdoc/>
        public void Dispose()
        {
            this.Close();
        }
    }

    /// <summary>
    /// Transport consumer options
    /// </summary>
    public class TransportConsumerOptions
    {
        /// <summary>
        /// Auto commit options
        /// </summary>
        public CommitOptions CommitOptions { get; set; } = new CommitOptions();
    }
}