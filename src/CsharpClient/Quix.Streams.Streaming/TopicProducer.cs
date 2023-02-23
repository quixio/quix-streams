using System;
using System.Collections.Concurrent;
using Quix.Streams.Process.Kafka;
using Quix.Streams.Transport.Kafka;

namespace Quix.Streams.Streaming
{
    /// <summary>
    /// Implementation of <see cref="ITopicProducer"/> to write outgoing streams
    /// </summary>
    public class TopicProducer : ITopicProducerInternal
    {
        private readonly Func<string, TelemetryKafkaProducer> createKafkaProducer;
        private readonly ConcurrentDictionary<string, Lazy<IStreamProducer>> streams = new ConcurrentDictionary<string, Lazy<IStreamProducer>>();
        private readonly IKafkaProducer kafkaProducer;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;
        
        /// <summary>
        /// Initializes a new instance of <see cref="TopicProducer"/>
        /// </summary>
        /// <param name="createKafkaProducer">Function factory to create a Kafka producer from Process layer.</param>
        public TopicProducer(Func<string, TelemetryKafkaProducer> createKafkaProducer)
        {
            this.createKafkaProducer = createKafkaProducer;
        }
        
        /// <summary>
        /// Initializes a new instance of <see cref="TopicProducer"/>
        /// </summary>
        public TopicProducer(KafkaProducerConfiguration config, string topic)
        {
            this.kafkaProducer = KafkaHelper.OpenKafkaInput(config, topic, out var byteSplitter);

            createKafkaProducer = (string streamId) => new TelemetryKafkaProducer(this.kafkaProducer, byteSplitter, streamId);
        }

        /// <inheritdoc />
        public IStreamProducer CreateStream()
        {
            var streamProducer = new StreamProducer(this, createKafkaProducer);

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
                (id) => new Lazy<IStreamProducer>(() => new StreamProducer(this, createKafkaProducer, streamId)),
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
                    var createdStream = new StreamProducer(this, createKafkaProducer, streamId);
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
