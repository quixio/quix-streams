using System;
using System.Collections.Generic;

namespace Quix.Streams.Process.Kafka
{
    /// <summary>
    /// Kafka broker configuration for <see cref="TelemetryKafkaProducer"/>
    /// </summary>
    public class KafkaWriterConfiguration
    {
        /// <summary>
        /// Initializes a new instance of <see cref="KafkaWriterConfiguration"/>
        /// </summary>
        /// <param name="brokerList">Broker list of the Kafka cluster</param>
        /// <param name="properties">Extra Kafka configuration properties</param>
        public KafkaWriterConfiguration(string brokerList, IDictionary<string, string> properties = null)
        {
            if (string.IsNullOrWhiteSpace(brokerList))
            {
                throw new ArgumentOutOfRangeException(nameof(brokerList), "Cannot be null or empty");
            }

            this.BrokerList = brokerList;
            this.Properties = properties;
        }

        /// <summary>
        /// Broker list of the Kafka cluster
        /// </summary>
        public string BrokerList { get; }

        /// <summary>
        /// Maximum Kafka protocol request message size in bytes.
        /// default: 1000012
        /// </summary>
        public int MaxMessageSize { get; } = 1000012;

        /// <summary>
        /// Maximum Kafka protocol key size in bytes.
        /// default: 1024
        /// </summary>
        public int MaxKeySize { get; } = 1024;

        /// <summary>
        /// Extra Kafka configuration properties
        /// </summary>
        public IDictionary<string, string> Properties { get; }
    }
}
