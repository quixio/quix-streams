using QuixStreams.Transport.Fw;
using QuixStreams.Transport.Kafka;

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
            var pubConfig = new QuixStreams.Transport.Kafka.PublisherConfiguration(config.BrokerList, config.Properties)
            {
                MaxMessageSize = config.MaxMessageSize
            };
            var topicConfig = new QuixStreams.Transport.Kafka.ProducerTopicConfiguration(topic);

            var kafkaProducer = new QuixStreams.Transport.Kafka.KafkaProducer(pubConfig, topicConfig);
            kafkaProducer.Open();
            return kafkaProducer;
        }

        /// <summary>
        /// Open kafka input with a byte splitter based on the configuration provided
        /// </summary>
        /// <param name="config">Kafka producer configuration</param>
        /// <param name="topic">Topic Id</param>
        /// <param name="byteSplitter">Byte splitter (output)</param> // TODO: Remove this dependency from Telemetry layer
        /// <returns>New instance of Kafka Input Transport layer</returns>
        public static IKafkaProducer OpenKafkaInput(KafkaProducerConfiguration config, string topic, out IByteSplitter byteSplitter)
        {
            // Create kafka input
            var pubConfig = new QuixStreams.Transport.Kafka.PublisherConfiguration(config.BrokerList, config.Properties)
            {
                MaxMessageSize = config.MaxMessageSize
            };
            byteSplitter = new QuixStreams.Transport.Fw.ByteSplitter(pubConfig.MaxMessageSize - config.MaxKeySize);
            var topicConfig = new QuixStreams.Transport.Kafka.ProducerTopicConfiguration(topic);

            var kafkaProducer = new QuixStreams.Transport.Kafka.KafkaProducer(pubConfig, topicConfig);
            kafkaProducer.Open();
            return kafkaProducer;
        }
    }
}