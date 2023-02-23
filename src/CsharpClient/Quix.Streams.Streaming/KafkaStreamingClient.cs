using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Quix.Streams.Process.Configuration;
using Quix.Streams.Process.Kafka;
using Quix.Streams.Process.Models;
using Quix.Streams.Streaming.Configuration;
using Quix.Streams.Streaming.Models;
using Quix.Streams.Streaming.Raw;
using Quix.Streams.Transport.Fw;

namespace Quix.Streams.Streaming
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
        public ITopicConsumer GetTopicConsumer(string topic, string consumerGroup = null, CommitOptions options = null, AutoOffsetReset autoOffset = AutoOffsetReset.Latest)
        {
            var kafkaReaderConfiguration = new TelemetryKafkaConsumerConfiguration(brokerAddress, consumerGroup, brokerProperties)
            {
                CommitOptions = options,
                AutoOffsetReset = autoOffset.ConvertToKafka()
            };

            var kafkaReader = new TelemetryKafkaConsumer(kafkaReaderConfiguration, topic);

            var topicConsumer = new TopicConsumer(kafkaReader);

            Quix.Streams.Streaming.App.Register(topicConsumer);

            return topicConsumer;
        }

        /// <summary>
        /// Open an topic consumer capable of subscribing to receive non-quixstreams incoming messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public IRawTopicConsumer CreateRawTopicConsumer(string topic, string consumerGroup = null, AutoOffsetReset? autoOffset = null)
        {
            var rawTopicConsumer = new RawTopicConsumer(brokerAddress, topic, consumerGroup, brokerProperties, autoOffset ?? AutoOffsetReset.Latest);

            Quix.Streams.Streaming.App.Register(rawTopicConsumer);
            
            return rawTopicConsumer;
        }

        /// <summary>
        /// Open an topic consumer capable of subscribing to receive non-quixstreams incoming messages  
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public IRawTopicProducer CreateRawTopicProducer(string topic)
        {
            var rawTopicProducer = new RawTopicProducer(brokerAddress, topic, brokerProperties);

            Quix.Streams.Streaming.App.Register(rawTopicProducer);
            return rawTopicProducer;
        }
        
        /// <summary>
        /// Open an topic producer capable of publishing non-quixstreams messages 
        /// </summary>
        /// <param name="topic">Name of the topic.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public ITopicProducer GetTopicProducer(string topic)
        {
            var topicProducer = new TopicProducer(new KafkaProducerConfiguration(brokerAddress, brokerProperties), topic);
            
            Quix.Streams.Streaming.App.Register(topicProducer);

            return topicProducer;
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
        public static ITopicConsumer GetTopicConsumer(this KafkaStreamingClient client, string topic, string consumerGroup = null, CommitMode commitMode = CommitMode.Automatic, AutoOffsetReset autoOffset =  AutoOffsetReset.Latest)
        {
            switch (commitMode)
            {
                case CommitMode.Automatic:
                    return client.GetTopicConsumer(topic, consumerGroup, autoOffset: autoOffset);
                case CommitMode.Manual:
                    var commitOptions = new CommitOptions()
                    {
                        AutoCommitEnabled = false
                    };
                    return client.GetTopicConsumer(topic, consumerGroup, commitOptions, autoOffset);
                default:
                    throw new ArgumentOutOfRangeException(nameof(commitMode), commitMode, null);
            }
        }
    }
}
