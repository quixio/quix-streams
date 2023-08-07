using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using QuixStreams.Kafka.Transport.SerDes;

namespace QuixStreams.Kafka.Transport
{
    /// <summary>
    /// The interface required to implement an <see cref="IProducer{TKey,TValue}"/>, which sends <see cref="Package"/> to Kafka
    /// </summary>
    public interface IKafkaTransportProducer: IDisposable
    {
        /// <summary>
        /// Publishes a package
        /// </summary>
        /// <param name="package">The package to publish</param>
        /// <param name="cancellationToken">The cancellation token to listen to for aborting the process</param>
        Task Publish(TransportPackage package, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Flush the queue to Kafka
        /// </summary>
        /// <param name="cancellationToken">The cancellation token for aborting flushing</param>
        Task Flush(CancellationToken cancellationToken = default);

        /// <summary>
        /// Open connection to Kafka
        /// </summary>
        void Open();
        
        /// <summary>
        /// Close connection to Kafka
        /// </summary>
        Task Close(CancellationToken cancellationToken = default);
    }
    
    /// <summary>
    /// A prebuilt pipeline, which serializes and optionally splits the provided packages then passes into the specified producer.
    /// </summary>
    public class KafkaTransportProducer : IKafkaTransportProducer
    {
        private readonly IPackageSerializer packageSerializer;
        private readonly IKafkaMessageSplitter kafkaMessageSplitter;
        private readonly IKafkaProducer producer;
        private Task lastPublishTask = null;
        private bool closed = false;
        

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaTransportProducer"/> with the specified <see cref="IProducer{TKey,TValue}"/>
        /// </summary>
        /// <param name="producer">The producer to pass the serialized packages into</param>
        /// <param name="packageSerializer">The package serializer to use</param>
        /// <param name="kafkaMessageSplitter">The optional byte splitter to use. When not provided, producer may receive packages bigger than it can handle</param>
        public KafkaTransportProducer(IKafkaProducer producer, IPackageSerializer packageSerializer = null, IKafkaMessageSplitter kafkaMessageSplitter = null)
        {
            this.producer = producer ?? throw new ArgumentNullException(nameof(producer));
            this.packageSerializer = packageSerializer ?? new PackageSerializer();
            this.kafkaMessageSplitter = kafkaMessageSplitter;
        }

        /// <inheritdocs/>
        public Task Publish(TransportPackage transportPackage, CancellationToken cancellationToken = default)
        {
            // this -> serializer -?> byteSplitter -> producer
            if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
            if (this.closed) throw new ProducerClosedException();
            var serialized = this.packageSerializer.Serialize(transportPackage);
            
            if (serialized.MessageSize >= this.producer.MaxMessageSizeBytes && this.kafkaMessageSplitter != null)
            {
                var splitMessages = this.kafkaMessageSplitter.Split(serialized);
                this.lastPublishTask = this.producer.Publish(splitMessages, cancellationToken);
                return this.lastPublishTask;
            }
            
            this.lastPublishTask = this.producer.Publish(serialized, cancellationToken);
            return this.lastPublishTask;
        }

        
        /// <inheritdocs/>
        public Task Flush(CancellationToken cancellationToken = default)
        {
            if (this.closed) throw new ProducerClosedException();
            return this.lastPublishTask ?? Task.CompletedTask;
        }

        /// <inheritdocs/>
        public void Open()
        {
            closed = false;
            this.producer.Open();
        }

        /// <inheritdocs/>
        public async Task Close(CancellationToken cancellationToken = default)
        {
            if (this.closed) throw new ProducerClosedException();
            closed = true;
            await Flush(cancellationToken);
            this.producer.Close();
        }

        /// <inheritdocs/>
        public void Dispose()
        {
            try
            {
                this.Flush().GetAwaiter().GetResult();
            }
            catch (ProducerClosedException)
            {
                // ignore
            }
        }
    }
}