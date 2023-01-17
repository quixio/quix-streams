using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Process.Kafka
{
    /// <summary>
    /// Extensions methods for conversion from Transport layer
    /// </summary>
    public static class ConversionExtensions
    {
        /// <summary>
        /// Converts <see cref="KafkaReaderConfiguration"/> to <see cref="SubscriberConfiguration"/>
        /// </summary>
        /// <param name="readerConfiguration">The <see cref="KafkaReaderConfiguration"/> to convert</param>
        /// <returns>The converted <see cref="SubscriberConfiguration"/></returns>
        public static SubscriberConfiguration ToSubscriberConfiguration(this KafkaReaderConfiguration readerConfiguration)
        {
            if (readerConfiguration == null) return null;
            var subConfig = new SubscriberConfiguration(readerConfiguration.BrokerList, readerConfiguration.ConsumerGroupId, readerConfiguration.Properties);
            subConfig.AutoOffsetReset = readerConfiguration.AutoOffsetReset;
            return subConfig;
        }
    }
}