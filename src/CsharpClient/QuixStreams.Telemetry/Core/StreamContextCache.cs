using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace QuixStreams.Telemetry
{
    /// <summary>
    /// Interface for basic stream context caching
    /// </summary>
    public interface IStreamContextCache
    {
        /// <summary>
        /// Tries to retrieve a context by stream Id
        /// </summary>
        /// <param name="streamId">The id of the stream</param>
        /// <param name="context">The context retrieved</param>
        /// <returns>True if was able to get, false otherwise</returns>
        bool TryGet(string streamId, out StreamContext context);
        
        /// <summary>
        /// Tries to add the context to the cache.
        /// </summary>
        /// <param name="context">The context to add</param>
        /// <returns>True if added, otherwise false</returns>
        bool TryAdd(StreamContext context);

        /// <summary>
        /// Remove context by stream id
        /// </summary>
        /// <param name="streamId">The id of the stream</param>
        /// <returns>True if was able to remove, false otherwise</returns>
        bool Remove(string streamId);

        /// <summary>
        /// Retrieves all contexts managed by this cache.
        /// </summary>
        /// <returns></returns>
        IDictionary<string, StreamContext> GetAll();
        
        /// <summary>
        /// The object which can be used for operation lock
        /// </summary>
        object Sync { get; }
    }

    /// <summary>
    /// Basic stream context caching implementation
    /// </summary>
    public class StreamContextCache : IStreamContextCache
    {
        private Dictionary<string, StreamContext> contexts = new Dictionary<string, StreamContext>();

        /// <inheritdoc/>
        public bool TryGet(string streamId, out StreamContext context)
        {
            return contexts.TryGetValue(streamId, out context);
        }

        /// <inheritdoc/>
        public bool TryAdd(StreamContext context)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            lock (Sync)
            {
                if (this.TryGet(context.StreamId, out var _)) return false;
                contexts[context.StreamId] = context;
                return true;
            }
        }

        /// <inheritdoc/>
        public bool Remove(string streamId)
        {
            lock (Sync)
            {
                return this.contexts.Remove(streamId);
            }
        }

        /// <inheritdoc/>
        public IDictionary<string, StreamContext> GetAll()
        {
            return contexts.ToDictionary(y => y.Key, y => y.Value);
        }

        /// <inheritdoc/>
        public object Sync { get; } = new object();
    }

    /// <summary>
    /// Stream context holder
    /// </summary>
    public class StreamContext
    {
        /// <summary>
        /// Initializes a new instance of <see cref="StreamContext"/>
        /// </summary>
        /// <param name="streamId">The id of the stream the context belongs to</param>
        public StreamContext(string streamId)
        {
            this.StreamId = streamId;
        }
        
        /// <summary>
        /// Stream Id
        /// </summary>
        public readonly string StreamId;
        
        /// <summary>
        /// Stream pipeline of the Stream
        /// </summary>
        public IStreamPipeline StreamPipeline;

        /// <summary>
        /// Last uncommitted topic partition offset received
        /// </summary>
        public TopicPartitionOffset LastUncommittedTopicPartitionOffset;

        /// <summary>
        /// Last topic partition offset received
        /// </summary>
        public TopicPartitionOffset LastTopicPartitionOffset;
    }
}