using System;
using System.Collections.Concurrent;
using System.Threading;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Implementation of <see cref="IOutputTopic"/> to write outgoing streams
    /// </summary>
    public class OutputTopic : IOutputTopic, IOutputTopicInternal
    {
        private readonly Func<string, KafkaWriter> createKafkaWriter;
        private readonly ConcurrentDictionary<string, Lazy<IStreamWriter>> streams = new ConcurrentDictionary<string, Lazy<IStreamWriter>>();
        private readonly IKafkaProducer kafkaProducer;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;
        
        /// <summary>
        /// Initializes a new instance of <see cref="OutputTopic"/>
        /// </summary>
        /// <param name="createKafkaWriter">Function factory to create a Kafka Writer from Process layer.</param>
        public OutputTopic(Func<string, KafkaWriter> createKafkaWriter)
        {
            this.createKafkaWriter = createKafkaWriter;
        }
        
        /// <summary>
        /// Initializes a new instance of <see cref="OutputTopic"/>
        /// </summary>
        public OutputTopic(KafkaWriterConfiguration config, string topic)
        {
            this.kafkaProducer = KafkaHelper.OpenKafkaInput(config, topic, out var byteSplitter);

            createKafkaWriter = (string streamId) => new KafkaWriter(this.kafkaProducer, byteSplitter, streamId);
        }

        /// <inheritdoc />
        public IStreamWriter CreateStream()
        {
            var streamWriter = new StreamWriter(this, createKafkaWriter);

            if (!this.streams.TryAdd(streamWriter.StreamId, new Lazy<IStreamWriter>(() => streamWriter)))
            {
                throw new Exception($"A stream with id '{streamWriter.StreamId}' already exists in the managed list of streams of the Output topic.");
            }

            return streamWriter;
        }

        /// <inheritdoc />
        public IStreamWriter CreateStream(string streamId)
        {
            var stream = this.streams.AddOrUpdate(streamId, 
                (id) => new Lazy<IStreamWriter>(() => new StreamWriter(this, createKafkaWriter, streamId)),
                (id, s) => throw new Exception($"A stream with id '{streamId}' already exists in the managed list of streams of the Output topic."));

            return stream.Value;
        }

        /// <inheritdoc />
        public IStreamWriter GetStream(string streamId)
        {
            if (!this.streams.TryGetValue(streamId, out var stream))
            {
                return null;
            }

            return stream.Value;
        }

        /// <inheritdoc />
        public IStreamWriter GetOrCreateStream(string streamId, Action<IStreamWriter> onStreamCreated = null)
        {
            var stream = this.streams.GetOrAdd(streamId, id =>
            {
                return new Lazy<IStreamWriter>(() =>
                {
                    var createdStream = new StreamWriter(this, createKafkaWriter, streamId);
                    onStreamCreated?.Invoke(createdStream);
                    return createdStream;
                });
            });

            return stream.Value;
        }

        /// <inheritdoc />
        public void RemoveStream(string streamId)
        {
            this.streams.TryRemove(streamId, out var stream);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.kafkaProducer?.Flush(default);
            this.kafkaProducer?.Dispose();
            this.OnDisposed?.Invoke(this, EventArgs.Empty);
        }
    }

}
