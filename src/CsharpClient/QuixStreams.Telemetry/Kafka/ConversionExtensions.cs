using QuixStreams.Kafka;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>
    /// Extensions methods for conversion from Transport layer
    /// </summary>
    public static class ConversionExtensions
    {
        /// <summary>
        /// Converts <see cref="TelemetryKafkaConsumerConfiguration"/> to <see cref="ConsumerConfiguration"/>
        /// </summary>
        /// <param name="consumerConfiguration">The <see cref="TelemetryKafkaConsumerConfiguration"/> to convert</param>
        /// <returns>The converted <see cref="ConsumerConfiguration"/></returns>
        public static ConsumerConfiguration ToSubscriberConfiguration(this TelemetryKafkaConsumerConfiguration consumerConfiguration)
        {
            if (consumerConfiguration == null) return null;
            var consConf = new ConsumerConfiguration(consumerConfiguration.BrokerList, consumerConfiguration.ConsumerGroupId, consumerConfiguration.Properties);
            consConf.AutoOffsetReset = consumerConfiguration.AutoOffsetReset;
            return consConf;
        }
    }
}