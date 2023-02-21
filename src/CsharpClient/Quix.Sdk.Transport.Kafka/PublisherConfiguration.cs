using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Quix.Sdk.Transport.Kafka
{
    public class PublisherConfiguration
    {
        private readonly IDictionary<string, string> producerProperties = new Dictionary<string, string>();

        /// <summary>
        /// Initializes a new instance of <see cref="PublisherConfiguration"/>
        /// </summary>
        /// <param name="brokerList">The list of brokers as a comma separated list of broker host or host:port.</param>
        /// <param name="producerProperties">List of broker and producer kafka properties that overrides the default configuration values.</param>
        public PublisherConfiguration(string brokerList, IDictionary<string, string> producerProperties = null)
        {
            if (string.IsNullOrWhiteSpace(brokerList))
                throw new ArgumentOutOfRangeException(nameof(brokerList), "Cannot be null or empty");

            BrokerList = brokerList;
            this.producerProperties = producerProperties?.ToDictionary(kv => kv.Key, kv => kv.Value) ?? this.producerProperties;
        }

        /// <summary>
        /// The list of brokers as a comma separated list of broker host or host:port.
        /// </summary>
        public string BrokerList { get; }

        /// <summary>
        /// Maximum Kafka protocol request message size in bytes.
        /// default: 1000012
        /// </summary>
        public int MaxMessageSize { get; set; } = 1000012; // https://docs.confluent.io/current/installation/configuration/broker-configs.html

        /// <summary>
        /// Maximum number of messages allowed on the queue.
        /// default: 100000
        /// </summary>
        public int? QueueBufferingMaxMessages { get; set; }

        /// <summary>
        /// Maximum total message size sum allowed on the queue. This property has higher priority than <see cref="QueueBufferingMaxMessages" />
        /// default: 1048576
        /// </summary>
        public int? QueueBufferingMaxKbytes { get; set; }

        /// <summary>
        /// Delay in milliseconds to wait for messages in the queue to accumulate before constructing message batches
        ///     (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved
        /// compression) batches of messages to accumulate at the expense of increased message delivery latency.
        /// default: 0.5
        /// </summary>
        public double? LingerMs { get; set; }

        /// <summary>
        /// How many times to retry sending a failing Message. **Note:** retrying may cause reordering unless ` <see cref="EnableIdempotence" />` is set to true.
        /// default: 2
        /// </summary>
        public int? MessageSendMaxRetries { get; set; }

        /// <summary>
        /// Local message timeout. This value is only enforced locally and limits the time a produced message waits for
        /// successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message
        ///     (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded.
        /// default: 300000
        /// </summary>
        public int? MessageTimeoutMs { get; set; }

        /// <summary>
        /// Partitioner: `random` - random distribution, `consistent` - CRC32 hash of key (Empty and NULL keys are mapped to
        /// single partition), `consistent_random` - CRC32 hash of key (Empty and NULL keys are randomly partitioned),
        ///     `murmur2` - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition),
        ///     `murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is
        /// functionally equivalent to the default partitioner in the Java Producer.).
        /// default: consistent_random
        /// </summary>
        public Partitioner? Partitioner { get; set; }

        /// <summary>
        /// When set to `true`, the producer will ensure that messages are successfully produced exactly once and in the
        /// original produce order. Producer instantiation will fail if user-supplied configuration (<see cref="MessageSendMaxRetries" /> &lt;= 0) is incompatible.
        /// default: false
        /// </summary>
        public bool? EnableIdempotence { get; set; }

        /// <summary>
        /// The backoff time in milliseconds before retrying a protocol request.
        /// default: 100
        /// </summary>
        public int? RetryBackoffMs { get; set; }

        /// <summary>
        /// The threshold of outstanding not yet transmitted broker requests needed to backpressure the producer's message
        /// accumulator. If the number of not yet transmitted requests equals or exceeds this number, produce request creation
        /// that would have otherwise been triggered (for example, in accordance with <see cref="LingerMs" />) will be delayed.
        /// A lower number yields larger and more effective batches. A higher value can improve latency when using compression
        /// on slow machines.
        /// default: 1
        /// </summary>
        public int? QueueBufferingBackpressureThreshold { get; set; }

        /// <summary>
        /// Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by <see cref="MaxMessageSize" />.
        /// default: 10000
        /// </summary>
        public int? BatchNumMessages { get; set; }
        
        /// <summary>
        /// Enable keep alive messages for publisher. Useful to ensure connection isn't idle reaped.
        /// </summary>
        public bool KeepConnectionAlive { get; set; } = true;

        /// <summary>
        /// The keel alive message interval in milliseconds
        /// </summary>
        public int KeepConnectionAliveInterval { get; set; } = 60000;

        internal ProducerConfig ToProducerConfig()
        {
            if (!producerProperties.ContainsKey("log_level"))
            {
                producerProperties["log_level"] = "0";
            }

            if (!producerProperties.ContainsKey("compression.type"))
            {
                producerProperties["compression.type"] = "gzip"; // default to gzip
            }
            
            if (!producerProperties.ContainsKey("socket.keepalive.enable"))
            {
                producerProperties["socket.keepalive.enable"] = "true"; // default to true
            }
            
            /*
             https://github.com/edenhill/librdkafka/issues/3109 not yet implemented
            if (!producerProperties.ContainsKey("connections.max.idle.ms"))
            {
                producerProperties["connections.max.idle.ms"] = "180000"; // Azure closes inbound TCP idle > 240,000 ms, which can result in sending on dead connections (shown as expired batches because of send timeout)
                // see more at https://docs.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations
            }*/
            if (!producerProperties.ContainsKey("metadata.max.age.ms"))
            {
                producerProperties["metadata.max.age.ms"] = "180000"; // Azure closes inbound TCP idle > 240,000 ms, which can result in sending on dead connections (shown as expired batches because of send timeout)
                // The hope here is that by refreshing metadata it is not considered idle
                // see more at https://docs.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations
            }
            
            var config = new ProducerConfig(producerProperties)
            {
                BootstrapServers = BrokerList,
                // Queue buffering
                QueueBufferingMaxKbytes = QueueBufferingMaxKbytes,
                QueueBufferingMaxMessages = QueueBufferingMaxMessages,
                QueueBufferingBackpressureThreshold = QueueBufferingBackpressureThreshold,
                // queue buffering
                LingerMs = LingerMs,

                Partitioner = Partitioner,
                BatchNumMessages = BatchNumMessages,
                EnableIdempotence = EnableIdempotence,
                MessageMaxBytes = MaxMessageSize,
                RetryBackoffMs = RetryBackoffMs,
                MessageTimeoutMs = MessageTimeoutMs,
                MessageSendMaxRetries = MessageSendMaxRetries
            };

            return config;
        }
    }
}