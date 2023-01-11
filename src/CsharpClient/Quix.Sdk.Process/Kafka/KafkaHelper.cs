using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Process.Kafka
{
    /// <summary>
    /// Extension methods for Kafka Transport layer instantiation
    /// </summary>
    public static class KafkaHelper
    {
        /// <summary>
        /// Open kafka input based on the configuration provided
        /// </summary>
        /// <param name="config">Kafka Writer configuration</param>
        /// <param name="topic">Topic Id</param>
        /// <returns>New instance of Kafka Input Transport layer</returns>
        public static IKafkaInput OpenKafkaInput(KafkaWriterConfiguration config, string topic)
        {
            // Create kafka input
            var pubConfig = new Transport.Kafka.PublisherConfiguration(config.BrokerList, config.Properties)
            {
                MaxMessageSize = config.MaxMessageSize
            };
            var topicConfig = new Transport.Kafka.InputTopicConfiguration(topic);

            var input = new Transport.Kafka.KafkaInput(pubConfig, topicConfig);
            input.Open();
            return input;
        }

        /// <summary>
        /// Open kafka input with a byte splitter based on the configuration provided
        /// </summary>
        /// <param name="config">Kafka Writer configuration</param>
        /// <param name="topic">Topic Id</param>
        /// <param name="byteSplitter">Byte splitter (output)</param> // TODO: Remove this dependency from Process layer
        /// <returns>New instance of Kafka Input Transport layer</returns>
        public static IKafkaInput OpenKafkaInput(KafkaWriterConfiguration config, string topic, out IByteSplitter byteSplitter)
        {
            // Create kafka input
            var pubConfig = new Transport.Kafka.PublisherConfiguration(config.BrokerList, config.Properties)
            {
                MaxMessageSize = config.MaxMessageSize
            };
            byteSplitter = new Transport.Fw.ByteSplitter(pubConfig.MaxMessageSize - config.MaxKeySize);
            var topicConfig = new Transport.Kafka.InputTopicConfiguration(topic);

            var input = new Transport.Kafka.KafkaInput(pubConfig, topicConfig);
            input.Open();
            return input;
        }
    }
}