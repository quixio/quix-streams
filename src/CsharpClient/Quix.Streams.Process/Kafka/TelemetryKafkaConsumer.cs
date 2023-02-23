using System;
using System.Diagnostics;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Quix.Streams.Transport.Fw;
using Quix.Streams.Transport.Kafka;

namespace Quix.Streams.Process.Kafka
{
    /// <summary>
    /// KafkaReader initializes transport layer classes and sets up a <see cref="StreamProcessFactory"/> to detect new streams in Kafka topic 
    /// and start new <see cref="StreamProcess"/> instances where all the messages of the stream are going to be sent.
    /// </summary>
    public class TelemetryKafkaConsumer: IDisposable
    {
        private readonly ILogger logger = Quix.Streams.Logging.CreateLogger<TelemetryKafkaConsumer>();
        private Transport.TransportConsumer transportConsumer;
        private bool isDisposed = false;

        private StreamProcessFactory streamProcessFactory;
        private Func<string, IStreamProcess> streamProcessFactoryHandler;
        private IKafkaConsumer kafkaConsumer;
        private readonly Action<CommitOptions> configureCommitOptions;

        /// <summary>
        /// Event raised when an exception occurs during the Reading processes
        /// </summary>
        public event EventHandler<Exception> OnReadException;
        
        /// <summary>
        /// Event raised with streams belonging to kafka partition(s) revoked
        /// </summary>
        public event Action<IStreamProcess[]> OnStreamsRevoked;

        /// <summary>
        /// Raised when the kafka topic will became unavailable, but it is still possible at this point
        /// </summary>
        public event EventHandler OnRevoking;
        
        /// <summary>
        /// Raised when commit occurs
        /// </summary>
        public event EventHandler OnCommitted;
        
        /// <summary>
        /// Raised before commit
        /// </summary>
        public event EventHandler OnCommitting;

        /// <summary>
        /// Stream Context cache for all the streams of the topic
        /// </summary>
        protected IStreamContextCache ContextCache;

        /// <summary>
        /// Initializes a new instance of <see cref="TelemetryKafkaConsumer"/>
        /// </summary>
        /// <param name="telemetryKafkaConsumerConfiguration">Kafka broker configuration for <see cref="TelemetryKafkaConsumer"/></param>
        /// <param name="topic">Topic name to read from</param>
        public TelemetryKafkaConsumer(TelemetryKafkaConsumerConfiguration telemetryKafkaConsumerConfiguration, string topic)
        {
            // Kafka Transport layer -> Transport layer
            var subConfig = telemetryKafkaConsumerConfiguration.ToSubscriberConfiguration();
            var commitOptions = telemetryKafkaConsumerConfiguration.CommitOptions ?? new CommitOptions();
            if (commitOptions.AutoCommitEnabled && !subConfig.ConsumerGroupSet)
            {
                logger.LogDebug("Disabled automatic kafka commit as no consumer group is set");
                commitOptions.AutoCommitEnabled = false;
            }

            configureCommitOptions = o =>
            {
                o.CommitEvery = commitOptions.CommitEvery;
                o.CommitInterval = commitOptions.CommitInterval;
                o.AutoCommitEnabled = commitOptions.AutoCommitEnabled;
            };
            
            var topicConfig = new ConsumerTopicConfiguration(topic);
            this.kafkaConsumer = new KafkaConsumer(subConfig, topicConfig);
        }

        /// <summary>
        /// Protected CTOR for testing purposes. We can simulate a Kafka broker instead of using a real broker.
        /// </summary>
        /// <param name="consumer">Simulated message broker ingoing endpoint of transport layer</param>
        protected TelemetryKafkaConsumer(IKafkaConsumer consumer)
        {
            this.kafkaConsumer = consumer;
        }

        private bool InitializeTransport()
        {
            if (transportConsumer != null)
                return false;

            this.kafkaConsumer.OnErrorOccurred += ReadingExceptionHandler;
            this.kafkaConsumer.Open();

            this.transportConsumer = new Transport.TransportConsumer(kafkaConsumer, (o) =>
            { 
                this.configureCommitOptions?.Invoke(o.CommitOptions);
            });
            return true;
        }

        private void ReadingExceptionHandler(object sender, Exception e)
        {
            if (this.OnReadException == null)
            {
                if (e is KafkaException)
                {
                    this.logger.LogError(e, "Exception receiving package from Kafka");                    
                }
                else
                {
                    this.logger.LogError(e, "Exception while processing package");
                }
            }
            else
            {
                this.OnReadException?.Invoke(sender, e);
            }
        }

        /// <summary>
        /// Starts reading from Kafka.
        /// </summary>
        public void Start()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TelemetryKafkaConsumer));
            if (!this.InitializeTransport()) return;

            // Transport layer -> Streaming layer
            ContextCache = new StreamContextCache();
            this.streamProcessFactory = new KafkaStreamProcessFactory(this.transportConsumer, streamProcessFactoryHandler, ContextCache);
            this.streamProcessFactory.OnStreamsRevoked += StreamsRevokedHandler;
            this.transportConsumer.OnRevoking += RevokingHandler;
            this.transportConsumer.OnCommitted += CommittedHandler;
            this.transportConsumer.OnCommitting += CommitingHandler;
            this.streamProcessFactory.Open();
        }

        private void CommittedHandler(object sender, OnCommittedEventArgs e)
        {
            this.OnCommitted?.Invoke(this, EventArgs.Empty);
        }
        
        private void CommitingHandler(object sender, OnCommittingEventArgs e)
        {
            this.OnCommitting?.Invoke(this, EventArgs.Empty);
        }

        private void RevokingHandler(object sender, OnRevokingEventArgs e)
        {
            this.OnRevoking?.Invoke(this, EventArgs.Empty);
        }

        private void StreamsRevokedHandler(IStreamProcess[] obj)
        {
            OnStreamsRevoked?.Invoke(obj);
        }

        /// <summary>
        /// Function to execute for each stream received by the reader.
        /// </summary>
        /// <param name="streamProcessFactoryHandler">Handler factory to execute for each Stream detected in the incoming messages in order 
        /// to create a new <see cref="StreamProcess"/> for each stream. 
        /// The handler function receives a StreamId and has to return a <see cref="StreamProcess"/>.</param>
        public void ForEach(Func<string, IStreamProcess> streamProcessFactoryHandler)
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TelemetryKafkaConsumer));
            this.streamProcessFactoryHandler = streamProcessFactoryHandler;
        }

        /// <summary>
        /// Stops reading from Kafka and closes all the Stream Processes.
        /// </summary>
        public void Stop()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TelemetryKafkaConsumer));
            StopHelper();
        }

        private void StopHelper()
        {
            // Transport layer
            if (transportConsumer != null)
            {
                this.transportConsumer.OnRevoking -= RevokingHandler;
                this.transportConsumer.OnCommitted -= CommittedHandler;
                this.transportConsumer.Close();
            }

            this.kafkaConsumer?.Close();

            // Stream process factory
            if (this.streamProcessFactory != null)
            {
                this.streamProcessFactory.Close();
                this.streamProcessFactory.OnStreamsRevoked -= this.StreamsRevokedHandler;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {           
            if (isDisposed) return;
            isDisposed = true;
            this.StopHelper();

            this.kafkaConsumer = null;
            this.transportConsumer = null;
            this.streamProcessFactory = null;
        }

        /// <summary>
        /// Commit packages read from kafka up to this point
        /// </summary>
        public void Commit()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TelemetryKafkaConsumer));
            if (transportConsumer == null) throw new InvalidOperationException("Not able to commit to inactive reader");
            Debug.Assert(ContextCache != null);
            lock (this.ContextCache.Sync)
            {
                var all = this.ContextCache.GetAll();
                var contexts = all.Select(y => y.Value.LastUncommittedTransportContext).Where(y => y != null).ToArray();
                if (contexts.Length == 0) return; // there is nothing to commit
                this.transportConsumer.Commit(contexts);
            }
        }
    }
}
