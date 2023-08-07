using System;
using System.Collections.Generic;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>
    /// Kafka broker configuration for <see cref="TelemetryKafkaProducer"/>
    /// </summary>
    public class KafkaProducerConfiguration
    {
        /// <summary>
        /// Initializes a new instance of <see cref="KafkaProducerConfiguration"/>
        /// </summary>
        /// <param name="brokerList">Broker list of the Kafka cluster</param>
        /// <param name="properties">Extra Kafka configuration properties</param>
        public KafkaProducerConfiguration(string brokerList, IDictionary<string, string> properties = null)
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
        /// Extra Kafka configuration properties
        /// </summary>
        public IDictionary<string, string> Properties { get; }
    }
}
