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
    public class StreamingClient
    {
        private readonly ILogger logger = Logging.CreateLogger<StreamingClient>();
        private readonly string brokerAddress;
        private readonly Dictionary<string, string> brokerProperties;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamingClient"/> that is capable of creating input and output topics for reading and writing
        /// </summary>
        /// <param name="brokerAddress">Address of Kafka cluster.</param>
        /// <param name="securityOptions">Optional security options.</param>
        /// <param name="properties">Additional broker properties</param>
        /// <param name="debug">Whether debugging should enabled</param>
        public StreamingClient(string brokerAddress, SecurityOptions securityOptions = null, IDictionary<string, string> properties = null, bool debug = false)
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
        /// Open an input topic capable of reading incoming streams
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="options">The settings to use for committing</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="IInputTopic"/></returns>
        public IInputTopic OpenInputTopic(string topic, string consumerGroup = "Default", CommitOptions options = null, AutoOffsetReset autoOffset = AutoOffsetReset.Earliest)
        {
            var wsIdPrefix = GetWorkspaceIdPrefixFromTopic(topic);
            consumerGroup = UpdateConsumerGroup(consumerGroup, wsIdPrefix);

            var kafkaReaderConfiguration = new KafkaReaderConfiguration(brokerAddress, consumerGroup, brokerProperties)
            {
                CommitOptions = options,
                AutoOffsetReset = autoOffset.ConvertToKafka()
            };

            var kafkaReader = new KafkaReader(kafkaReaderConfiguration, topic);

            var inputTopic = new InputTopic(kafkaReader);

            Quix.Sdk.Streaming.App.Register(inputTopic);

            return inputTopic;
        }

        /// <summary>
        /// Open an input topic capable of reading non-sdk incoming messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="IInputTopic"/></returns>
        public IRawInputTopic OpenRawInputTopic(string topic, string consumerGroup = null, AutoOffsetReset? autoOffset = null)
        {
            var rawInputTopic = new RawInputTopic(brokerAddress, topic, consumerGroup ?? "Default", brokerProperties, autoOffset ?? AutoOffsetReset.Earliest);

            Quix.Sdk.Streaming.App.Register(rawInputTopic);
            
            return rawInputTopic;
        }

        /// <summary>
        /// Open an input topic capable of writing non-sdk incoming messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <returns>Instance of <see cref="IInputTopic"/></returns>
        public IRawOutputTopic OpenRawOutputTopic(string topic)
        {
            var rawOutputTopic = new RawOutputTopic(brokerAddress, topic, brokerProperties);

            Quix.Sdk.Streaming.App.Register(rawOutputTopic);
            return rawOutputTopic;
        }
        
        /// <summary>
        /// Open an output topic capable of writing non-sdk messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <returns>Instance of <see cref="IInputTopic"/></returns>
        public IOutputTopic OpenOutputTopic(string topic)
        {
            var outputTopic = new OutputTopic(new KafkaWriterConfiguration(brokerAddress, brokerProperties), topic);
            
            Quix.Sdk.Streaming.App.Register(outputTopic);

            return outputTopic;
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
    public static class StreamingClientExtensions
    {
        /// <summary>
        /// Open an input topic capable of reading incoming streams
        /// </summary>
        /// <param name="client">Streaming Client instance</param>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="commitMode">The commit strategy to use for this topic</param>
        /// <returns>Instance of <see cref="IInputTopic"/></returns>
        public static IInputTopic OpenInputTopic(this StreamingClient client, string topic, string consumerGroup = "Default", CommitMode commitMode = CommitMode.Automatic, AutoOffsetReset autoOffset =  AutoOffsetReset.Earliest)
        {
            switch (commitMode)
            {
                case CommitMode.Automatic:
                    return client.OpenInputTopic(topic, consumerGroup, autoOffset: autoOffset);
                case CommitMode.Manual:
                    var commitOptions = new CommitOptions()
                    {
                        AutoCommitEnabled = false
                    };
                    return client.OpenInputTopic(topic, consumerGroup, commitOptions, autoOffset);
                default:
                    throw new ArgumentOutOfRangeException(nameof(commitMode), commitMode, null);
            }
        }
    }
}
