using System;
using System.Diagnostics;
using Newtonsoft.Json;
using QuixStreams.Kafka.Transport.SerDes.Codecs.JsonConverters;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// The key used to uniquely identify a model
    /// </summary>
    [DebuggerDisplay("{key}")]
    [JsonConverter(typeof(AsStringJsonConverter))]
    public struct ModelKey : IEquatable<ModelKey>
    {
        private readonly string key;

        /// <summary>
        /// Creates a new ModelKey using the provided string key
        /// </summary>
        /// <param name="key">The string key</param>
        public ModelKey(string key)
        {
            this.key = key;
        }

        /// <summary>
        /// Creates a new key using type.
        /// This is a short hand for type.FullName
        /// </summary>
        /// <param name="type">The Type</param>
        public ModelKey(Type type) : this(type.FullName)
        {
        }

        /// <summary>
        /// Creates a new key using type, which is also versioned.
        /// This is a short hand for {type.FullName}.V{version}
        /// </summary>
        /// <param name="type"></param>
        /// <param name="version"></param>
        public ModelKey(Type type, int version) : this($"{type.FullName}.V{version}")
        {
        }

        /// <summary>
        /// Convert type to ModelKey
        /// </summary>
        /// <param name="type">The Type to convert</param>
        public static implicit operator ModelKey(Type type)
        {
            return new ModelKey(type);
        }

        /// <summary>
        /// Convert string to ModelKey
        /// </summary>
        public static implicit operator ModelKey(string key)
        {
            return new ModelKey(key);
        }

        /// <summary>
        /// Convert ModelKey to string
        /// </summary>
        public static implicit operator string(ModelKey modelKey)
        {
            return modelKey.key;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return this.key;
        }
        
        /// <inheritdoc/>
        public bool Equals(ModelKey other)
        {
            if (this.key == null)
            {
                return other.key == null;
            }

            return this.key.Equals(other.key);
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            return obj is ModelKey other && this.Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return this.key != null ? this.key.GetHashCode() : 0;
        }

        /// <summary>
        /// Well known model keys
        /// </summary>
        public static class WellKnownModelKeys
        {
            // The shorter the key is, the better, but it still has to be unique.

            /// <summary>
            /// String model key
            /// </summary>
            public static readonly ModelKey String = "String";
            
            /// <summary>
            /// Byte array model key
            /// </summary>
            public static readonly ModelKey ByteArray = "B[]";

            /// <summary>
            /// Default model key is a key that is not set
            /// </summary>
            public static readonly ModelKey Default = default;
        }
    }
}