using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using QuixStreams.Streaming.States;
using QuixStreams.Telemetry;
using QuixStreams.Telemetry.Kafka;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Implementation of <see cref="ITopicConsumer"/> to consume incoming streams
    /// </summary>
    public class TopicConsumer : ITopicConsumer
    {
        private readonly ILogger logger = Logging.CreateLogger<StreamConsumer>();
        private readonly TelemetryKafkaConsumer telemetryKafkaConsumer;
        private bool isDisposed = false;
        private readonly object stateLock = new object();
        private volatile TopicStateManager stateManager = null;

        /// <inheritdoc />
        public event EventHandler<IStreamConsumer> OnStreamReceived;

        /// <inheritdoc />
        public event EventHandler OnRevoking;

        /// <inheritdoc />
        public event EventHandler<IStreamConsumer[]> OnStreamsRevoked;

        /// <inheritdoc />
        public event EventHandler OnCommitted;
        
        /// <inheritdoc />
        public event EventHandler OnCommitting;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;

        /// <inheritdoc />
        public void Commit()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TopicConsumer));
            this.telemetryKafkaConsumer.Commit();
        }

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaStreamingClient"/>
        /// </summary>
        /// <param name="telemetryKafkaConsumer">Kafka consumer from Telemetry layer</param>
        public TopicConsumer(TelemetryKafkaConsumer telemetryKafkaConsumer)
        {
            telemetryKafkaConsumer.ForEach(streamId =>
            {
                var stream = new StreamConsumer(this, streamId);
                try
                {
                    this.OnStreamReceived?.Invoke(this, stream);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Exception while raising OnStreamReceived.");
                }

                return stream;
            });

            telemetryKafkaConsumer.OnStreamsRevoked += this.StreamsRevokedEventHandler;
            telemetryKafkaConsumer.OnRevoking += this.StreamsRevokingEventHandler;
            telemetryKafkaConsumer.OnCommitted += this.CommittedEventHandler;
            telemetryKafkaConsumer.OnCommitting += this.CommittingEventHandler;

            this.telemetryKafkaConsumer = telemetryKafkaConsumer;
        }

        private void CommittedEventHandler(object sender, EventArgs e)
        {
            this.OnCommitted?.Invoke(this, EventArgs.Empty);
        }
        
        private void CommittingEventHandler(object sender, EventArgs e)
        {
            this.OnCommitting?.Invoke(this, EventArgs.Empty);
        }

        private void StreamsRevokingEventHandler(object sender, EventArgs e)
        {
            this.OnRevoking?.Invoke(this, EventArgs.Empty);
        }

        private void StreamsRevokedEventHandler(IStreamPipeline[] obj)
        {
            if (this.OnStreamsRevoked == null) return;
            if (obj == null || obj.Length == 0) return;
            
            // This is relying on the assumption that the StreamConsumer that we've created in the StreamPipelineFactoryHandler (see kafkareader.foreach)
            // is being returned here.
            var readers = obj.Select(y => y as IStreamConsumer).Where(y => y != null).ToArray();
            if (readers.Length == 0) return;
            this.OnStreamsRevoked?.Invoke(this, readers);
        }

        /// <inheritdoc />
        public void Subscribe()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TopicConsumer));
            telemetryKafkaConsumer.Start();
        }
        
        /// <inheritdoc />
        public TopicState<T> GetState<T>(string nameOfState, TopicStateDefaultValueDelegate<T> defaultValueFactory = null)
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TopicConsumer));

            return GetStateManager().GetState<T>(nameOfState, defaultValueFactory);
        }

        /// <inheritdoc />
        public TopicStateManager GetStateManager()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(TopicConsumer));

            if (this.stateManager != null) return this.stateManager;
            lock (stateLock)
            {
                if (this.stateManager != null) return this.stateManager;
                var topic = this.telemetryKafkaConsumer.Topic;

                this.stateManager = App.GetStateManager().GetTopicStateManager(this, topic);
            }

            return this.stateManager;
        }


        /// <inheritdoc />
        public void Dispose()
        {
            if (isDisposed) return;
            isDisposed = true;
            telemetryKafkaConsumer.OnStreamsRevoked -= this.StreamsRevokedEventHandler;
            telemetryKafkaConsumer.OnRevoking -= this.StreamsRevokingEventHandler;
            telemetryKafkaConsumer.OnCommitted -= this.CommittedEventHandler;
            telemetryKafkaConsumer.OnCommitting -= this.CommittingEventHandler;
            this.telemetryKafkaConsumer.Dispose(); // TODO code smell, disposing external resource
            this.OnDisposed?.Invoke(this, EventArgs.Empty);
        }
    }

}
