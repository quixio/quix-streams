using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace QuixStreams.Kafka
{
    public class ConsumerConfiguration
    {
        private readonly IDictionary<string, string> consumerProperties = new Dictionary<string, string>();
        public static string ConsumerGroupIdWhenNotSet = "UNSET-" +Guid.NewGuid().ToString("D"); // technically any random static would do, but in case something does commit under that consumer id, it would possibly break things

        /// <summary>
        /// Initializes a new instance of <see cref="ConsumerConfiguration" />
        /// </summary>
        /// <param name="brokerList">The list of brokers as a comma separated list of broker host or host:port.</param>
        /// <param name="groupId">Client group id string. All clients sharing the same GroupId belong to the same group.</param>
        /// <param name="consumerProperties">List of broker and consumer kafka properties that overrides the default configuration values.</param>
        public ConsumerConfiguration(string brokerList, string groupId = null, IDictionary<string, string> consumerProperties = null)
        {
            if (string.IsNullOrWhiteSpace(brokerList))
            {
                throw new ArgumentOutOfRangeException(nameof(brokerList), "Cannot be null or empty");
            }

            if (string.IsNullOrWhiteSpace(groupId))
            {
                // means we're not using consumer group. In this case disallow use of commit
                ConsumerGroupSet = false;
                groupId = ConsumerGroupIdWhenNotSet;
            }
            else
            {
                ConsumerGroupSet = true;
            }

            this.BrokerList = brokerList;
            this.GroupId = groupId;
            this.consumerProperties = consumerProperties?.ToDictionary(kv => kv.Key, kv => kv.Value) ?? this.consumerProperties;
        }

        /// <summary>
        /// The list of brokers as a comma separated list of broker host or host:port.
        /// </summary>
        public string BrokerList { get; }


        /// <summary>
        /// Client group id string. All clients sharing the same GroupId belong to the same group.
        /// </summary>
        public string GroupId { get; }

        /// <summary>
        /// Whether the consumer group is set
        /// </summary>
        public bool ConsumerGroupSet { get; }

        /// <summary>
        /// Deprecated feature used by producers in 0.5.0 and before.
        /// Enables checking for keep alive messages and filters them out.
        /// </summary>
        public bool CheckForKeepAlivePackets { get; set; } = true;

        /// <summary>
        /// If consumer group is configured, The auto offset reset determines the start offset in the event
        /// there are not yet any committed offsets for the consumer group for the topic/partitions of interest.     
        /// 
        /// If no consumer group is configured, the consumption will start according to value set.
        /// If no auto offset reset is set,  defaults to latest.
        /// </summary>
        public AutoOffsetReset? AutoOffsetReset { get; set; } = null;

        internal ConsumerConfig ToConsumerConfig()
        {
            if (!consumerProperties.ContainsKey("log_level"))
            {
                consumerProperties["log_level"] = "0";
            }
            
            /*
             My testing showed this doesn't seem to work, but will leave this here for reference QUIX-882
            if (!consumerProperties.ContainsKey("auto.offset.reset"))
            {
                consumerProperties["auto.offset.reset"] = "smallest";
            }
            */
            
            if (!consumerProperties.ContainsKey("socket.keepalive.enable"))
            {
                consumerProperties["socket.keepalive.enable"] = "true"; // default to true
            }
            /*
             https://github.com/edenhill/librdkafka/issues/3109 not yet implemented
            if (!producerProperties.ContainsKey("connections.max.idle.ms"))
            {
                producerProperties["connections.max.idle.ms"] = "180000"; // Azure closes inbound TCP idle > 240,000 ms, which can result in sending on dead connections (shown as expired batches because of send timeout)
                // see more at https://docs.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations
            }*/
            if (!consumerProperties.ContainsKey("metadata.max.age.ms"))
            {
                consumerProperties["metadata.max.age.ms"] = "180000"; // Azure closes inbound TCP idle > 240,000 ms, which can result in sending on dead connections (shown as expired batches because of send timeout)
                // The hope here is that by refreshing metadata it is not considered idle
                // see more at https://docs.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations
            }

            var config = new ConsumerConfig(consumerProperties)
            {
                GroupId = this.GroupId,
                BootstrapServers = this.BrokerList,
                AutoOffsetReset = this.AutoOffsetReset ?? Confluent.Kafka.AutoOffsetReset.Latest,
                EnableAutoCommit = false
            };
            return config;
        }
    }
}