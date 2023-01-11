namespace Quix.Sdk.Transport.Kafka
{
    public static class KnownKafkaTransportContextKeys
    {
        /// <summary>
        ///     The key used to identify the kafka message key
        ///     Type is <see cref="string" />
        /// </summary>
        public const string Key = "KafkaKey";

        /// <summary>
        ///     The key used to identify the topic the message is from when reading from Kafka
        ///     Type is <see cref="string" />
        /// </summary>
        public const string Topic = "KafkaTopic";

        /// <summary>
        ///     The key used to identify the partition the message is from when reading from Kafka
        ///     Type is <see cref="int" />
        /// </summary>
        public const string Partition = "KafkaPartition";

        /// <summary>
        ///     The key used to identify the offset the message has in the source topic and partition
        ///     Type is <see cref="long" />
        /// </summary>
        public const string Offset = "KafkaOffset";

        /// <summary>
        ///     The key used to identify the time the message was received by kafka
        ///     Type is <see cref="DateTime" /> in UTC
        /// </summary>
        public const string DateTime = "KafkaDataTime";

        /// <summary>
        ///     The key used to identify the message size read from kafka. This is uncompressed size
        ///     Type is <see cref="long" />, measured in bytes
        /// </summary>
        public static string MessageSize = "KafkaMessageSize";
    }
}