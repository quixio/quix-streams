using System;
using System.Collections.Concurrent;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Implementation of <see cref="ITopicProducer"/> to write outgoing streams
    /// </summary>
    public class TopicProducer : ITopicProducerInternal
    {
        private readonly Func<string, TelemetryKafkaProducer> createKafkaWriter;
        private readonly ConcurrentDictionary<string, Lazy<IStreamProducer>> streams = new ConcurrentDictionary<string, Lazy<IStreamProducer>>();
        private readonly IKafkaProducer kafkaProducer;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;
        
        /// <summary>
        /// Initializes a new instance of <see cref="TopicProducer"/>
        /// </summary>
        /// <param name="createKafkaWriter">Function factory to create a Kafka Writer from Process layer.</param>
        public TopicProducer(Func<string, TelemetryKafkaProducer> createKafkaWriter)
        {
            this.createKafkaWriter = createKafkaWriter;
        }
        
        /// <summary>
        /// Initializes a new instance of <see cref="TopicProducer"/>
        /// </summary>
        public TopicProducer(KafkaWriterConfiguration config, string topic)
        {
            this.kafkaProducer = KafkaHelper.OpenKafkaInput(config, topic, out var byteSplitter);

            createKafkaWriter = (string streamId) => new TelemetryKafkaProducer(this.kafkaProducer, byteSplitter, streamId);
        }

        /// <inheritdoc />
        public IStreamProducer CreateStream()
        {
            var streamProducer = new StreamProducer(this, createKafkaWriter);

            if (!this.streams.TryAdd(streamProducer.StreamId, new Lazy<IStreamProducer>(() => streamProducer)))
            {
                throw new Exception($"A stream with id '{streamProducer.StreamId}' already exists in the managed list of streams of the Output topic.");
            }

            return streamProducer;
        }

        /// <inheritdoc />
        public IStreamProducer CreateStream(string streamId)
        {
            var stream = this.streams.AddOrUpdate(streamId, 
                (id) => new Lazy<IStreamProducer>(() => new StreamProducer(this, createKafkaWriter, streamId)),
                (id, s) => throw new Exception($"A stream with id '{streamId}' already exists in the managed list of streams of the Output topic."));

            return stream.Value;
        }

        /// <inheritdoc />
        public IStreamProducer GetStream(string streamId)
        {
            if (!this.streams.TryGetValue(streamId, out var stream))
            {
                return null;
            }

            return stream.Value;
        }

        /// <inheritdoc />
        public IStreamProducer GetOrCreateStream(string streamId, Action<IStreamProducer> onStreamCreated = null)
        {
            var stream = this.streams.GetOrAdd(streamId, id =>
            {
                return new Lazy<IStreamProducer>(() =>
                {
                    var createdStream = new StreamProducer(this, createKafkaWriter, streamId);
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
