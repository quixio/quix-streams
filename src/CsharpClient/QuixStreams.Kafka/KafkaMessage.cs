using System;
using System.Collections.Generic;
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
        public IDictionary<string, byte[]> Headers { get; protected set; }

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
        public KafkaMessageTime MessageTime { get; protected set; }

        /// <summary>
        /// Create a new Kafka message with the specified properties.
        /// </summary>
        /// <param name="key">The message key. Specify null for no key.</param>
        /// <param name="value">The value of the message.</param>
        /// <param name="headers">The headers of the message. Specify null for no </param>
        /// <param name="messageTime">The optional message time. Defaults to utc now</param>
        /// <param name="topicPartitionOffset">The topic and partition with the specified offset this message is representing</param>
        public KafkaMessage(byte[] key, byte[] value, IDictionary<string, byte[]> headers = null, KafkaMessageTime messageTime = null, TopicPartitionOffset topicPartitionOffset = null)
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

            MessageTime = messageTime ?? KafkaMessageTime.ProduceTime;

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
                Headers = new Dictionary<string, byte[]>();
                foreach (var kvp in KafkaHeaders)
                {
                    var kvpValue = kvp.GetValueBytes();
                    Headers.Add(kvp.Key, kvpValue);
                    HeaderSize += kvp.Key.Length * 4; // UTF-8 chars are between 1-4 bytes, so worst case assumed
                    HeaderSize += kvpValue.Length;
                }

                MessageSize += HeaderSize;
            }
            
            this.TopicPartitionOffset = consumeResult.TopicPartitionOffset;
            this.MessageTime = consumeResult.Message.Timestamp;
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

    /// <summary>
    /// Time of the kafka message
    /// </summary>
    public class KafkaMessageTime
    {
        /// <summary>
        /// Singleton instance of Kafka Message time with Produce Time type
        /// </summary>
        public static KafkaMessageTime ProduceTime = new KafkaMessageTime(DateTime.MinValue, KafkaMessageTimeType.ProduceTime);
        
        /// <summary>
        /// The UTC time of the message
        /// </summary>
        public DateTime Timestamp { get; }

        /// <summary>
        /// The type of the message time
        /// </summary>
        public KafkaMessageTimeType Type { get; }
        
        /// <summary>
        /// Creates a new instance of <see cref="KafkaMessageTime"/> with current UTC time and
        /// <see cref="KafkaMessageTime.CreateTime"/> type
        /// </summary>
        public KafkaMessageTime()
        {
            this.Timestamp = DateTime.UtcNow;
            this.Type = KafkaMessageTimeType.CreateTime;
        }

        /// <summary>
        /// Creates a new instance of <see cref="KafkaMessageTime"/> with the specified time and type
        /// </summary>
        /// <param name="timestamp"></param>
        /// <param name="type"></param>
        public KafkaMessageTime(DateTime timestamp, KafkaMessageTimeType type)
        {
            this.Timestamp = timestamp;
            this.Type = type;
        }
        
        /// <summary>
        /// Implicitly converts to confluent kafka timestamp
        /// </summary>
        /// <param name="time">the time to convert</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException">unknown type</exception>
        public static explicit operator Timestamp(KafkaMessageTime time)
        {
            return time.Type switch
            {
                KafkaMessageTimeType.Unknown => new Timestamp(time.Timestamp, TimestampType.NotAvailable),
                KafkaMessageTimeType.LogAppendTime => new Timestamp(time.Timestamp, TimestampType.LogAppendTime),
                KafkaMessageTimeType.CreateTime => new Timestamp(time.Timestamp, TimestampType.CreateTime),
                KafkaMessageTimeType.ProduceTime => new Timestamp(DateTime.UtcNow, TimestampType.CreateTime),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
        
        /// <summary>
        /// Implicitly converts to confluent kafka timestamp
        /// </summary>
        /// <param name="time">the time to convert</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException">unknown type</exception>
        public static implicit operator KafkaMessageTime(Timestamp time)
        {
            return time.Type switch
            {
                TimestampType.NotAvailable => new KafkaMessageTime(time.UtcDateTime, KafkaMessageTimeType.Unknown),
                TimestampType.LogAppendTime => new KafkaMessageTime(time.UtcDateTime, KafkaMessageTimeType.LogAppendTime),
                TimestampType.CreateTime => new KafkaMessageTime(time.UtcDateTime, KafkaMessageTimeType.CreateTime),
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }

    /// <summary>
    /// The kafka message time type
    /// </summary>
    public enum KafkaMessageTimeType
    {
        /// <summary>
        /// The time the message got appended to the log
        /// </summary>
        LogAppendTime,
        
        /// <summary>
        /// The time the message got created
        /// </summary>
        CreateTime,
        
        /// <summary>
        /// The time the message is queued to be sent to kafka
        /// </summary>
        ProduceTime,
        
        /// <summary>
        /// Time type is not known
        /// </summary>
        Unknown
    }
}