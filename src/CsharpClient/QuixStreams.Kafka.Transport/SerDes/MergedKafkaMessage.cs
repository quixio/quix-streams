using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace QuixStreams.Kafka.Transport.SerDes
{

    /// <summary>
    /// Kafka message which is made up from multiple messages
    /// </summary>
    public class MergedKafkaMessage : KafkaMessage
    {
        /// <summary>
        /// Initializes a new instance of <see cref="MergedKafkaMessage"/> which is a message merged from multiple messages.
        /// </summary>
        /// <param name="segments">The segments it was merged from</param>
        /// <param name="key">The message key. Specify null for no key.</param>
        /// <param name="value">The value of the message.</param>
        /// <param name="headers">The headers of the message. Specify null for no </param>
        /// <param name="topicPartitionOffset">The partition and offset to use for the merged message</param>
        public MergedKafkaMessage(ICollection<KafkaMessage> segments, byte[] key, byte[] value, KafkaHeader[] headers, TopicPartitionOffset topicPartitionOffset) : base(key, value, headers)
        {
            this.TopicPartitionOffset = topicPartitionOffset;
            this.MessageSegments = segments;
            this.FirstMessage = segments.First();
            this.LastMessage = segments.Last();
        }

        /// <summary>
        /// The last message that got merged to form this message
        /// </summary>
        public KafkaMessage LastMessage { get; }

        /// <summary>
        /// The first message that got merged to form this message
        /// </summary>
        public KafkaMessage FirstMessage { get; }


        /// <summary>
        /// All messages that got merged to form this message
        /// </summary>
        public ICollection<KafkaMessage> MessageSegments { get; }
    }
}