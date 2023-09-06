using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams.Kafka;
using QuixStreams.Kafka.Transport;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>
    /// Kafka producer component implementation.
    /// It produces all the incoming messages to Kafka with a new StreamId.
    /// </summary>
    public class TelemetryKafkaProducer : StreamComponent, IDisposable
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<TelemetryKafkaProducer>();

        private readonly IKafkaTransportProducer kafkaTransportProducer;

        /// <summary>
        /// The globally unique identifier of the stream.
        /// </summary>
        public string StreamId { get; protected set; }

        /// <summary>
        /// Event raised when an exception occurs during the publishing process
        /// </summary>
        public event EventHandler<Exception> OnWriteException;

        /// <summary>
        /// Initializes a new instance of <see cref="TelemetryKafkaProducer"/>
        /// </summary>
        /// <param name="producer">A stream package producer. Share this among multiple instances of this class to prevent re-initialization.</param>
        /// <param name="streamId">Stream Id to use to generate the new Stream on Kafka. If not specified, it generates a new Guid.</param>
        public TelemetryKafkaProducer(IKafkaProducer producer, string streamId = null)
        {
            this.kafkaTransportProducer = new QuixStreams.Kafka.Transport.KafkaTransportProducer(producer);

            this.InitializeStreaming(streamId);
        }

        private void InitializeStreaming(string streamId)
        {
            this.StreamId = streamId ?? Guid.NewGuid().ToString();
            this.Input.Subscribe(OnStreamPackage);
        }

        private Task OnStreamPackage(StreamPackage package)
        {
            return this.SendAsync(package);
        }

        private async Task SendAsync(StreamPackage package)
        {
            try
            {
                var transportPackage = new TransportPackage(package.Type, StreamId, package.Value);

                if (kafkaTransportProducer == null)
                {
                    throw new InvalidOperationException("Producer is already closed.");
                }

                await this.kafkaTransportProducer.Publish(transportPackage, this.CancellationToken);
                await this.Output.Send(package);
            }
            catch (Exception e)
            {
                if (this.OnWriteException == null)
                {
                    this.logger.LogError(e, "Exception sending package to Kafka");
                }
                else
                {
                    this.OnWriteException?.Invoke(this, e);
                }
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.kafkaTransportProducer?.Flush();
        }

    }
}
