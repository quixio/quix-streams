using System;
using Microsoft.Extensions.Logging;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// The base codec implementation offering a default implementation for <see cref="TryDeserialize"/> and <see cref="TrySerialize"/>
    /// </summary>
    /// <typeparam name="TContent">Type of the content</typeparam>
    public abstract class Codec<TContent> : ICodec<TContent>
    {
        private readonly ILogger logger;

        /// <summary>
        /// Shared constructor for all the codec implementations
        /// </summary>
        protected Codec()
        {
            logger = QuixStreams.Logging.CreateLogger(this.GetType());
        }
        
        /// <inheritdoc />
        public abstract CodecId Id { get; }

        /// <inheritdoc />
        public abstract TContent Deserialize(byte[] contentBytes);
        
        
        /// <inheritdoc />
        public abstract TContent Deserialize(ArraySegment<byte> contentBytes);

        /// <inheritdoc />
        public abstract byte[] Serialize(TContent obj);

        /// <inheritdoc />
        public virtual bool TryDeserialize(byte[] contentBytes, out object content)
        {
            content = null;
            try
            {
                content = this.Deserialize(contentBytes);
                return true;
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Failed to deserialize message with codec {0}", this.Id);
                return false;
            }
        }
        
        /// <inheritdoc />
        public virtual bool TryDeserialize(ArraySegment<byte> contentBytes, out object content)
        {
            content = null;
            try
            {
                content = this.Deserialize(contentBytes);
                return true;
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Failed to deserialize message with codec {0}", this.Id);
                return false;
            }
        }

        /// <inheritdoc />
        public virtual Type Type { get; } = typeof(TContent);

        /// <inheritdoc />
        public virtual bool TrySerialize(object obj, out byte[] serialized)
        {
            serialized = null;
            if (obj is TContent typeValue)
            {
                serialized = this.Serialize(typeValue);
                return true;
            }

            return false;
        }
    }
}