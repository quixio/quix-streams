using System;
using System.Diagnostics;
using Newtonsoft.Json;
using QuixStreams.Kafka.Transport.SerDes.Codecs.JsonConverters;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// The Id uniquely used to identify a codec
    /// </summary>
    [DebuggerDisplay("{id}")]
    [JsonConverter(typeof(AsStringJsonConverter))]
    public struct CodecId : IEquatable<CodecId>
    {
        private readonly string id;

        /// <summary>
        /// Initializes a new instance of <see cref="CodecId"/>
        /// </summary>
        /// <param name="id">The id to wrap</param>
        public CodecId(string id)
        {
            this.id = id;
        }

        /// <summary>
        /// Converts string to a new instance of <see cref="CodecId"/>
        /// </summary>
        /// <param name="id">The id to wrap</param>
        /// <returns><see cref="CodecId"/> representing the provided string id</returns>
        public static implicit operator CodecId(string id)
        {
            return new CodecId(id);
        }

        /// <summary>
        /// Converts <see cref="CodecId"/> to string
        /// </summary>
        /// <param name="codecId">The codec id to convert to string</param>
        /// <returns>String value of the codec</returns>
        public static implicit operator string(CodecId codecId)
        {
            return codecId.id;
        }

        /// <summary>
        /// Ids used to identify when Codecs when serializing / deserializing Quix packages
        /// </summary>
        public static class WellKnownCodecIds
        {
            // The shorter the key is, the better, but it still has to be unique.

            /// <summary>
            /// Default Json codec
            /// </summary>
            public static readonly CodecId DefaultJsonCodec = "J";

            /// <summary>
            /// Default Typed Json codec
            /// </summary>
            public static readonly CodecId DefaultTypedJsonCodec = "JT";

            /// <summary>
            /// Protobuf codec
            /// </summary>
            public static readonly CodecId ProtobufCodec = "PB";

            /// <summary>
            /// String codec
            /// </summary>
            public static readonly CodecId String = "S"; 

            /// <summary>
            /// Byte codec
            /// </summary>
            public static readonly CodecId Byte = "B[]"; 

            /// <summary>
            /// No codec used
            /// </summary>
            public static readonly CodecId None = string.Empty;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return this.id;
        }

        /// <inheritdoc/>
        public bool Equals(CodecId other)
        {
            if (this.id == null)
            {
                return other.id == null;
            }

            return this.id.Equals(other.id);
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            return obj is CodecId other && this.Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return this.id != null ? this.id.GetHashCode() : 0;
        }
    }
}