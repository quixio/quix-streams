using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace QuixStreams.Kafka
{
    /// <summary>
    /// Represent a kafka message
    /// </summary>
    public class KafkaMessage
    {
        /// <summary>
        /// The key of the message. Can be null.
        /// </summary>
        public byte[] Key { get; protected set; }
        
        /// <summary>
        /// The value of the message.
        /// </summary>
        public byte[] Value { get; protected set; }
        
        /// <summary>
        /// The headers of the message. Can be null.
        /// </summary>
        public ICollection<KafkaHeader> Headers { get; protected set; }

        /// <summary>
        /// Kafka headers
        /// </summary>
        protected internal Headers KafkaHeaders { get; protected set; }
        
        /// <summary>
        /// The estimated message size including header, key, value
        /// </summary>
        public int MessageSize { get; protected set; }
        
        /// <summary>
        /// The estimated (worst case) size of the message headers
        /// </summary>
        public int HeaderSize { get; protected set; }
        
        /// <summary>
        /// The topic partition offset associated with the message. Can be null.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset { get; protected set; }
        
        /// <summary>
        /// The message time
        /// </summary>
        public Timestamp Timestamp { get; protected set; }

        /// <summary>
        /// Create a new Kafka message with the specified properties.
        /// </summary>
        /// <param name="key">The message key. Specify null for no key.</param>
        /// <param name="value">The value of the message.</param>
        /// <param name="headers">The headers of the message. Specify null for no </param>
        /// <param name="messageTime">The optional message time. Defaults to utc now</param>
        /// <param name="topicPartitionOffset">The topic and partition with the specified offset this message is representing</param>
        public KafkaMessage(byte[] key, byte[] value, ICollection<KafkaHeader> headers = null, Timestamp? messageTime = null, TopicPartitionOffset topicPartitionOffset = null)
        {
            Key = key;
            MessageSize += key?.Length ?? 0;
            Value = value;
            MessageSize += value.Length;
            Headers = headers;
            if (headers != null)
            {
                KafkaHeaders = new Headers();
                foreach (var kvp in headers)
                {
                    KafkaHeaders.Add(kvp.Key, kvp.Value);
                    HeaderSize += kvp.Key.Length * 4; // UTF-8 chars are between 1-4 bytes, so worst case assumed
                    HeaderSize += kvp.Value.Length;
                }

                MessageSize += HeaderSize;
            }

            Timestamp = messageTime ?? Timestamp.Default;

            TopicPartitionOffset = topicPartitionOffset;
        }
        
        internal KafkaMessage(ConsumeResult<byte[], byte[]> consumeResult)
        {
            
            Key = consumeResult.Message.Key;
            MessageSize += Key?.Length ?? 0;
            Value = consumeResult.Message.Value;
            MessageSize += Value.Length;
            KafkaHeaders = consumeResult.Message.Headers;
            if (KafkaHeaders != null)
            {
                Headers = new List<KafkaHeader>();
                foreach (var kvp in KafkaHeaders)
                {
                    var kvpValue = kvp.GetValueBytes();
                    Headers.Add(new KafkaHeader(kvp.Key, kvpValue));
                    HeaderSize += kvp.Key.Length * 4; // UTF-8 chars are between 1-4 bytes, so worst case assumed
                    HeaderSize += kvpValue.Length;
                }

                MessageSize += HeaderSize;
            }
            
            this.TopicPartitionOffset = consumeResult.TopicPartitionOffset;
            this.Timestamp = consumeResult.Message.Timestamp;
        }

        /// <summary>
        /// Estimates the header size assuming UTF-8 encoding
        /// </summary>
        /// <param name="headers">The headers to estimate</param>
        /// <returns></returns>
        public static int EstimateHeaderSize(IDictionary<string, byte[]> headers)
        {
            if (headers == null) return 0;
            var size = 0;
            foreach (var kvp in headers)
            {
                
                size += kvp.Key.Length * 4; // UTF-8 chars are between 1-4 bytes, so worst case assumed
                size += kvp.Value.Length;
            }

            return size;
        }
    }

    public class KafkaHeader
    {
        /// <summary>
        /// Initializes a new instance of Kafka Header
        /// </summary>
        /// <param name="key">The key of the header</param>
        /// <param name="value">The value of the header</param>
        /// <exception cref="ArgumentNullException">When either key or value is null</exception>
        public KafkaHeader(string key, byte[] value)
        {
            this.Key = key ?? throw new ArgumentNullException(nameof(key));
            this.Value = value ?? throw new ArgumentNullException(nameof(value));
        }
        
        /// <summary>
        /// Initializes a new instance of Kafka Header
        /// </summary>
        /// <param name="key">The key of the header</param>
        /// <param name="value">The value of the header as string. Will be UTF-8 converted</param>
        /// <exception cref="ArgumentNullException">When either key or value is null</exception>
        public KafkaHeader(string key, string value)
        {
            this.Key = key ?? throw new ArgumentNullException(nameof(key));
            this.Value = Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value)));
        }
        
        /// <summary>
        /// The key of the header
        /// </summary>
        public string Key { get; }
        
        /// <summary>
        /// The value of the header
        /// </summary>
        public byte[] Value { get; }

        /// <summary>
        /// Returns the header value as string
        /// </summary>
        /// <returns>Header value as string</returns>
        public string GetValueAsString()
        {
            return Encoding.UTF8.GetString(this.Value);
        }
    }
}