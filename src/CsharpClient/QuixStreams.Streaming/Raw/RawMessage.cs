using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace QuixStreams.Streaming.Raw
{
    /// <summary>
    /// The message read from topic without any transformation
    /// </summary>
    public class RawMessage
    {
        private static ReadOnlyDictionary<string, string> GetEmptyMetadata = new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());

        internal RawMessage(
            byte[] key,
            byte[] value,
            ReadOnlyDictionary<string, string> metadata)
        {
            this.Key = key;
            this.Value = value;
            this.getMetadata = metadata ?? GetEmptyMetadata;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="RawMessage"/>
        /// </summary>
        /// <param name="key">Key of the message</param>
        /// <param name="value">Value of the message</param>
        public RawMessage(
            byte[] key,
            byte[] value):this(key, value, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="RawMessage"/> without a Key
        /// </summary>
        /// <param name="value">Value of the message</param>
        public RawMessage(
            byte[] value) : this(null, value, null)
        {
        }

        /// <summary>
        /// The optional key of the message. Depending on broker and message it is not guaranteed
        /// </summary>
        public byte[] Key;

        /// <summary>
        /// The value of the message
        /// </summary>
        public byte[] Value;

        private ReadOnlyDictionary<string, string> getMetadata;

        /// <summary>
        /// The broker specific optional metadata
        /// </summary>
        public ReadOnlyDictionary<string, string> Metadata {
            get {
                return this.getMetadata;
            } 
        }
    }
}