using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Quix.Streams.Streaming.Raw
{
    /// <summary>
    /// The message read from topic without any transformation
    /// </summary>
    public class RawMessage
    {
        private static Lazy<ReadOnlyDictionary<string, string>> GetEmptyMetadata = 
                new Lazy<ReadOnlyDictionary<string, string>>(() => 
                    new ReadOnlyDictionary<string, string>(new Dictionary<string, string>())
                );

        internal RawMessage(
            string key,
            byte[] value,
            Lazy<ReadOnlyDictionary<string,string>> metadata)
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
            string key,
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
        public string Key;

        /// <summary>
        /// The value of the message
        /// </summary>
        public byte[] Value;

        private Lazy<ReadOnlyDictionary<string, string>> getMetadata;

        /// <summary>
        /// The broker specific optional metadata
        /// </summary>
        public ReadOnlyDictionary<string, string> Metadata {
            get {
                return this.getMetadata.Value;
            } 
        }
    }
}