using System.Collections.Generic;
using System.Text.RegularExpressions;
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
                {KnownKafkaTransportContextKeys.Topic, consumeResult.Topic},
                {KnownKafkaTransportContextKeys.Partition, consumeResult.Partition.Value},
                {KnownKafkaTransportContextKeys.Offset, consumeResult.Offset.Value},
                {KnownKafkaTransportContextKeys.DateTime, consumeResult.Message.Timestamp.UtcDateTime},
                {KnownKafkaTransportContextKeys.MessageSize, consumeResult.Message.Value.Length}
            });

            if (consumeResult.Message.Key != null)
            {
                tContext.Add(KnownTransportContextKeys.MessageGroupKey, consumeResult.Message.Key);
                tContext.Add(KnownKafkaTransportContextKeys.Key, consumeResult.Message.Key);
            }
            
            return new Package<byte[]>(consumeResult.Message.Value, null, tContext);
        }

        private static Regex stateChangeRegex = new Regex(": ([^ ]+): Broker changed state ([a-zA-Z_]*) -> ([a-zA-Z_]*)$", RegexOptions.Compiled);

        public static bool TryParseBrokerState(LogMessage logMessage, out string broker, out string state)
        {
            broker = null;
            state = null;
            try
            {
                // Example:  Debug [thrd:sasl_ssl://IP:PORT/BROKERID]: sasl_ssl://IP:PORT/BROKERID: Broker changed state DOWN -> INIT
                if (logMessage == null) return false;
                if (logMessage.Level != SyslogLevel.Debug) return false;
                if (!logMessage.Message.Contains("Broker changed state")) return false;
                var segments = stateChangeRegex.Match(logMessage.Message);
                broker = segments.Groups[1].Value;
                state = segments.Groups[3].Value;
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}