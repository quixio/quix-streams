using Quix.Sdk.Process.Models;
using Quix.Sdk.Transport.Kafka;
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Transport.Fw;

namespace Quix.Sdk.Process.Kafka
{
    /// <summary>
    /// Kafka writer component implementation.
    /// It writes all the incoming messages to Kafka with a new StreamId.
    /// </summary>
    public class KafkaWriter : StreamComponent, IDisposable
    {
        private readonly ILogger logger = Logging.CreateLogger<KafkaWriter>();

        private Transport.IO.IInput transportInput;

        /// <summary>
        /// The globally unique identifier of the stream.
        /// </summary>
        public string StreamId { get; protected set; }

        /// <summary>
        /// Event raised when an exception occurs during the writing process
        /// </summary>
        public event EventHandler<Exception> OnWriteException;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaWriter"/>
        /// </summary>
        /// <param name="input">The input to write the stream packages into. This is something you should share between multiple instances of this class to avoid re-initializing them.</param>
        /// <param name="streamId">Stream Id to use to generate the new Stream on Kafka. If not specified, it generates a new Guid.</param>
        [Obsolete("Use constructor with bytesplitter")] // kept for DLL backward compatibility
        public KafkaWriter(Transport.IO.IInput input, string streamId = null)
        {
            this.transportInput = new Transport.TransportInput(input);

            this.InitializeStreaming(streamId);
        }

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaWriter"/>
        /// </summary>
        /// <param name="input">The input to write the stream packages into. This is something you should share between multiple instances of this class to avoid re-initializing them.</param>
        /// <param name="byteSplitter">The byte splitter to use. </param>
        /// <param name="streamId">Stream Id to use to generate the new Stream on Kafka. If not specified, it generates a new Guid.</param>
        public KafkaWriter(Transport.IO.IInput input, IByteSplitter byteSplitter, string streamId = null)
        {
            this.transportInput = new Transport.TransportInput(input, byteSplitter);

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
                var transportPackage = new Transport.IO.Package(
                    package.Type,
                    new Lazy<object>(() => package.Value)
                );

                if (transportInput == null)
                {
                    throw new InvalidOperationException("Writer is already closed.");
                }

                transportPackage.SetKey(StreamId);

                await this.transportInput.Send(transportPackage, this.CancellationToken);
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

        private void Stop()
        {
            // Transport layer
            this.transportInput = null;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.Stop();
        }

    }
}
