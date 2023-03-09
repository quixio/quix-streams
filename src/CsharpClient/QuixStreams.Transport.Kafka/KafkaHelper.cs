using System;
using System.Collections.Generic;
using Confluent.Kafka;
using QuixStreams.Transport.IO;

namespace QuixStreams.Transport.Kafka
{
    internal static class KafkaHelper
    {
        /// <summary>
        /// Parses the Kafka <see cref="ConsumeResult{TKey,TValue}"/> into a <see cref="Package{byte[]}"/>
        /// </summary>
        /// <param name="consumeResult">The consume result to parse</param>
        /// <returns>The package</returns>
        public static Package<byte[]> FromResult(ConsumeResult<byte[], byte[]> consumeResult)
        {
            var tContext = new TransportContext(new Dictionary<string, object>
            {
                {KnownTransportContextKeys.MessageGroupKey, consumeResult.Message.Key},
                {KnownKafkaTransportContextKeys.Topic, consumeResult.Topic},
                {KnownKafkaTransportContextKeys.Key, consumeResult.Message.Key},
                {KnownKafkaTransportContextKeys.Partition, consumeResult.Partition.Value},
                {KnownKafkaTransportContextKeys.Offset, consumeResult.Offset.Value},
                {KnownKafkaTransportContextKeys.DateTime, consumeResult.Message.Timestamp.UtcDateTime},
                {KnownKafkaTransportContextKeys.MessageSize, consumeResult.Message.Value.Length}
            });
            return new Package<byte[]>(consumeResult.Message.Value, null, tContext);
        }
    }
}