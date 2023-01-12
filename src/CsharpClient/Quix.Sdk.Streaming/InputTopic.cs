using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Transport.IO;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Implementation of <see cref="IInputTopic"/> to read incoming streams
    /// </summary>
    public class InputTopic : IInputTopic
    {
        private ILogger logger = Logging.CreateLogger<StreamReader>();
        private readonly KafkaReader kafkaReader;
        private bool isDisposed = false;

        /// <inheritdoc />
        public event EventHandler<IStreamReader> OnStreamReceived;

        /// <inheritdoc />
        public event EventHandler OnRevoking;

        /// <inheritdoc />
        public event EventHandler<IStreamReader[]> OnStreamsRevoked;

        /// <inheritdoc />
        public event EventHandler OnCommitted;
        
        /// <inheritdoc />
        public event EventHandler OnCommitting;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;

        /// <inheritdoc />
        public void Commit()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(InputTopic));
            this.kafkaReader.Commit();
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StreamingClient"/>
        /// </summary>
        /// <param name="kafkaReader">Kafka reader from Process layer</param>
        public InputTopic(KafkaReader kafkaReader)
        {
            kafkaReader.ForEach(streamId =>
            {
                var stream = new StreamReader(streamId);
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

            kafkaReader.OnStreamsRevoked += this.StreamsRevokedEventHandler;
            kafkaReader.OnRevoking += this.StreamsRevokingEventHandler;
            kafkaReader.OnCommitted += this.CommittedEventHandler;
            kafkaReader.OnCommitting += this.CommittingEventHandler;

            this.kafkaReader = kafkaReader;
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

        private void StreamsRevokedEventHandler(IStreamProcess[] obj)
        {
            if (this.OnStreamsRevoked == null) return;
            if (obj == null || obj.Length == 0) return;
            
            // This is relying on the assumption that the StreamReader that we've created in the StreamProcessFactoryHandler (see kafkareader.foreach)
            // is being returned here.
            var readers = obj.Select(y => y as IStreamReader).Where(y => y != null).ToArray();
            if (readers.Length == 0) return;
            this.OnStreamsRevoked?.Invoke(this, readers);
        }

        /// <inheritdoc />
        public void StartReading()
        {
            if (isDisposed) throw new ObjectDisposedException(nameof(InputTopic));
            kafkaReader.Start();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (isDisposed) return;
            isDisposed = true;
            kafkaReader.OnStreamsRevoked -= this.StreamsRevokedEventHandler;
            kafkaReader.OnRevoking -= this.StreamsRevokingEventHandler;
            kafkaReader.OnCommitted -= this.CommittedEventHandler;
            kafkaReader.OnCommitting -= this.CommittingEventHandler;
            this.kafkaReader.Dispose(); // TODO code smell, disposing external resource
            this.OnDisposed?.Invoke(this, EventArgs.Empty);
        }
    }

}
