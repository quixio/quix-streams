using System;
using Quix.Sdk.Process.Kafka;
using System.Collections.Generic;
using Quix.Sdk.Process.Configuration;
using Quix.Sdk.Streaming.Configuration;
using Quix.Sdk.Process.Models;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Transport.Fw;
using AutoOffsetReset = Quix.Sdk.Process.Kafka.AutoOffsetReset;
using Quix.Sdk.Streaming.Raw;
using Quix.Sdk.Streaming.Utils;
using SaslMechanism = Confluent.Kafka.SaslMechanism;
using Quix.Sdk.Streaming.QuixApi.Portal;

namespace Quix.Sdk.Streaming
{
    /// <summary>
    /// Streaming client for kafka
    /// </summary>
    public class KafkaStreamingClient
    {
        private readonly ILogger logger = Logging.CreateLogger<KafkaStreamingClient>();
        private readonly string brokerAddress;
        private readonly Dictionary<string, string> brokerProperties;

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaStreamingClient"/> that is capable of creating topic consumer and producers
        /// </summary>
        /// <param name="brokerAddress">Address of Kafka cluster.</param>
        /// <param name="securityOptions">Optional security options.</param>
        /// <param name="properties">Additional broker properties</param>
        /// <param name="debug">Whether debugging should enabled</param>
        public KafkaStreamingClient(string brokerAddress, SecurityOptions securityOptions = null, IDictionary<string, string> properties = null, bool debug = false)
        {
            this.brokerAddress = brokerAddress;
            if (securityOptions == null)
            {
                this.brokerProperties = new Dictionary<string, string>();
            }
            else
            {
                var securityOptionsBuilder = new SecurityOptionsBuilder();

                if (securityOptions.UseSsl)
                {
                    securityOptionsBuilder.SetSslEncryption(securityOptions.SslCertificates);
                }
                else
                {
                    securityOptionsBuilder.SetNoEncryption();
                }

                if (securityOptions.UseSasl)
                {
                    if (!Enum.TryParse(securityOptions.SaslMechanism.ToString(), true, out Confluent.Kafka.SaslMechanism parsed))
                    {
                        throw new ArgumentOutOfRangeException(nameof(securityOptions.SaslMechanism), "Unsupported sasl mechanism " + securityOptions.SaslMechanism);
                    }

                    securityOptionsBuilder.SetSaslAuthentication(securityOptions.Username, securityOptions.Password, parsed);
                }
                else
                {
                    securityOptionsBuilder.SetNoAuthentication();
                }

                this.brokerProperties = securityOptionsBuilder.Build();
            }

            if (properties != null)
            {
                foreach (var property in properties)
                {
                    this.brokerProperties[property.Key] = property.Value;
                }
            }

            if (debug) this.brokerProperties["debug"] = "all";

            CodecRegistry.Register(CodecType.Protobuf);
        }
        
        /// <summary>
        /// Open an topic consumer capable of subscribing to receive incoming streams
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="options">The settings to use for committing</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public ITopicConsumer CreateTopicConsumer(string topic, string consumerGroup = null, CommitOptions options = null, AutoOffsetReset autoOffset = AutoOffsetReset.Latest)
        {
            var wsIdPrefix = GetWorkspaceIdPrefixFromTopic(topic);
            consumerGroup = UpdateConsumerGroup(consumerGroup, wsIdPrefix);

            var kafkaReaderConfiguration = new TelemetryKafkaConsumerConfiguration(brokerAddress, consumerGroup, brokerProperties)
            {
                CommitOptions = options,
                AutoOffsetReset = autoOffset.ConvertToKafka()
            };

            var kafkaReader = new TelemetryKafkaConsumer(kafkaReaderConfiguration, topic);

            var topicConsumer = new TopicConsumer(kafkaReader);

            Quix.Sdk.Streaming.App.Register(topicConsumer);

            return topicConsumer;
        }

        /// <summary>
        /// Open an topic consumer capable of subscribing to receive non-sdk incoming messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public IRawTopicConsumer CreateRawTopicConsumer(string topic, string consumerGroup = null, AutoOffsetReset? autoOffset = null)
        {
            var rawTopicConsumer = new RawTopicConsumer(brokerAddress, topic, consumerGroup, brokerProperties, autoOffset ?? AutoOffsetReset.Latest);

            Quix.Sdk.Streaming.App.Register(rawTopicConsumer);
            
            return rawTopicConsumer;
        }

        /// <summary>
        /// Open an topic consumer capable of subscribing to receive non-sdk incoming messages  
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public IRawTopicProducer CreateRawTopicProducer(string topic)
        {
            var rawTopicProducer = new RawTopicProducer(brokerAddress, topic, brokerProperties);

            Quix.Sdk.Streaming.App.Register(rawTopicProducer);
            return rawTopicProducer;
        }
        
        /// <summary>
        /// Open an topic producer capable of publishing non-sdk messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public ITopicProducer CreateTopicProducer(string topic)
        {
            var topicProducer = new TopicProducer(new KafkaWriterConfiguration(brokerAddress, brokerProperties), topic);
            
            Quix.Sdk.Streaming.App.Register(topicProducer);

            return topicProducer;
        }

        private string GetWorkspaceIdPrefixFromTopic(string topic)
        {
            if (QuixUtils.TryGetWorkspaceIdPrefix(topic, out var wsId))
            {
                this.logger.LogTrace("Determined your workspace Id to be {0}", wsId.TrimEnd('-'));
                return wsId;
            }

            this.logger.LogWarning("Warning: Your workspace id could not be determined from your topic and it might not work unless using your own kafka.", topic);
            return null;
        }

        private string UpdateConsumerGroup(string consumerGroup, string workspaceIdPrefix)
        {
            if (consumerGroup == null) return null;
            if (consumerGroup.Contains("[MACHINENAME]"))
            {
                consumerGroup = consumerGroup.Replace("[MACHINENAME]", Environment.GetEnvironmentVariable("POD_NAMESPACE") ?? System.Environment.MachineName);
            }

            if (workspaceIdPrefix != null)
            {
                // check if consumerGroup already starts with it
                if (consumerGroup.StartsWith(workspaceIdPrefix)) return consumerGroup;
                return workspaceIdPrefix + consumerGroup;
            }
            
            this.logger.LogWarning("Warning: Your consumer group '{0}' could not be updated with workspace id prefix and might not work.", consumerGroup);
            return consumerGroup;
        }
    }

    /// <summary>
    /// Extensions for Streaming Client class
    /// </summary>
    public static class KafkaStreamingClientExtensions
    {
        /// <summary>
        /// Open an topic consumer capable of subscribing to receive incoming streams
        /// </summary>
        /// <param name="client">Streaming Client instance</param>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="commitMode">The commit strategy to use for this topic</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public static ITopicConsumer CreateTopicConsumer(this KafkaStreamingClient client, string topic, string consumerGroup = null, CommitMode commitMode = CommitMode.Automatic, AutoOffsetReset autoOffset =  AutoOffsetReset.Latest)
        {
            switch (commitMode)
            {
                case CommitMode.Automatic:
                    return client.CreateTopicConsumer(topic, consumerGroup, autoOffset: autoOffset);
                case CommitMode.Manual:
                    var commitOptions = new CommitOptions()
                    {
                        AutoCommitEnabled = false
                    };
                    return client.CreateTopicConsumer(topic, consumerGroup, commitOptions, autoOffset);
                default:
                    throw new ArgumentOutOfRangeException(nameof(commitMode), commitMode, null);
            }
        }
    }
}
