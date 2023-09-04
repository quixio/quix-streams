using System;
using System.Collections.Generic;
using QuixStreams.Kafka.Transport;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>
    /// Kafka broker configuration for <see cref="TelemetryKafkaConsumer"/>
    /// </summary>
    public class TelemetryKafkaConsumerConfiguration
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TelemetryKafkaConsumerConfiguration"/>
        /// </summary>
        /// <param name="brokerList">Broker list of the Kafka cluster</param>
        /// <param name="consumerGroupId">Consumer group id of the reading pipeline. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="properties">Extra Kafka configuration properties</param>
        public TelemetryKafkaConsumerConfiguration(string brokerList, string consumerGroupId = null, IDictionary<string, string> properties = null)
        {
            if (string.IsNullOrWhiteSpace(brokerList))
            {
                throw new ArgumentOutOfRangeException(nameof(brokerList), "Cannot be null or empty");
            }
            
            this.BrokerList = brokerList;
            this.ConsumerGroupId = consumerGroupId;
            properties = properties ?? new Dictionary<string, string>();
            if (!properties.ContainsKey("queued.max.messages.kbytes")) properties["queued.max.messages.kbytes"] = "20480";
            if (!properties.ContainsKey("fetch.message.max.bytes")) properties["fetch.message.max.bytes"] = "20480";
            this.Properties = properties;
        }

        /// <summary>
        /// Broker list of the Kafka cluster
        /// </summary>
        public string BrokerList { get; }

        /// <summary>
        /// Consumer group id of the reading pipeline
        /// </summary>
        public string ConsumerGroupId { get; }

        /// <summary>
        /// If consumer group is configured, The auto offset reset determines the start offset in the event
        /// there are not yet any committed offsets for the consumer group for the topic/partitions of interest.
        /// 
        /// If no consumer group is configured, the consumption will start according to value set.
        /// If no auto offset reset is set, in case of no consumer group it defaults to end, otherwise to earliest.
        /// </summary>
        public Confluent.Kafka.AutoOffsetReset? AutoOffsetReset { get; set; } = null;

        /// <summary>
        /// The commit options to use
        /// </summary>
        public CommitOptions CommitOptions { get; set; } = null;

        /// <summary>
        /// Extra Kafka configuration properties
        /// </summary>
        public IDictionary<string, string> Properties { get; }
    }
}
