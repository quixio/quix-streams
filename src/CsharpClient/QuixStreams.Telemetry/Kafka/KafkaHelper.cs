using QuixStreams.Kafka;
using QuixStreams.Kafka.Transport.SerDes;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>
    /// Extension methods for Kafka Transport layer instantiation
    /// </summary>
    public static class KafkaHelper
    {
        /// <summary>
        /// Open kafka input based on the configuration provided
        /// </summary>
        /// <param name="config">Kafka producer configuration</param>
        /// <param name="topic">Topic Id</param>
        /// <returns>New instance of Kafka Input Transport layer</returns>
        public static IKafkaProducer OpenKafkaInput(KafkaProducerConfiguration config, string topic)
        {
            // Create kafka input
            var prodConfig = new ProducerConfiguration(config.BrokerList, config.Properties);
            var topicConfig = new ProducerTopicConfiguration(topic);

            var kafkaProducer = new KafkaProducer(prodConfig, topicConfig);
            return kafkaProducer;
        }

        /// <summary>
        /// Open kafka input with a byte splitter based on the configuration provided
        /// </summary>
        /// <param name="config">Kafka producer configuration</param>
        /// <param name="topic">Topic Id</param>
        /// <param name="messageSplitter">Message splitter (output)</param>
        /// <returns>New instance of Kafka Input Transport layer</returns>
        public static IKafkaProducer OpenKafkaInput(KafkaProducerConfiguration config, string topic, out IKafkaMessageSplitter messageSplitter)
        {
            // Create kafka input
            var prodConfig = new ProducerConfiguration(config.BrokerList, config.Properties);
            var topicConfig = new ProducerTopicConfiguration(topic);

            var kafkaProducer = new KafkaProducer(prodConfig, topicConfig);
            
            messageSplitter = new KafkaMessageSplitter(kafkaProducer.MaxMessageSizeBytes);
            return kafkaProducer;
        }
    }
}