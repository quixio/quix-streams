using System;
using System.Collections.Generic;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Streaming.Raw
{
    /// <summary>
    /// Class to write raw messages into a Topic (capable to write non-sdk messages)
    /// </summary>
    public class RawOutputTopic: IRawOutputTopic, IDisposable
    {
        private PublisherConfiguration publisherConfiguration;
        private ProducerTopicConfiguration topicConfiguration;

        private string topicName;

        private IKafkaProducer kafkaProducer = null;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;

        /// <summary>
        /// Initializes a new instance of <see cref="RawOutputTopic"/>
        /// </summary>
        /// <param name="brokerAddress">Address of Kafka cluster.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="brokerProperties">Additional broker properties</param>
        public RawOutputTopic(string brokerAddress, string topicName, Dictionary<string, string> brokerProperties = null)
        {
            brokerProperties ??= new Dictionary<string, string>();
            if (!brokerProperties.ContainsKey("queued.max.messages.kbytes")) brokerProperties["queued.max.messages.kbytes"] = "20480";

            this.topicName = topicName;

            this.publisherConfiguration = new Transport.Kafka.PublisherConfiguration(brokerAddress, brokerProperties)
            {
            };
            //keepalive packets would interfere with reading raw data since we dont have any protocol defined over the transport layer
            this.publisherConfiguration.KeepConnectionAlive = false;
            this.topicConfiguration = new Transport.Kafka.ProducerTopicConfiguration(this.topicName);

            this.kafkaProducer = new Transport.Kafka.KafkaProducer(this.publisherConfiguration, this.topicConfiguration);
            this.kafkaProducer.Open();
        }

        /// <inheritdoc />
        public void Write(RawMessage message)
        {
            var data = new Package<byte[]>(
                              new Lazy<byte[]>(() => message.Value)
                        );
            data.SetKey(message.Key);
            kafkaProducer.Publish(data);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.kafkaProducer?.Dispose();
            this.OnDisposed?.Invoke(this, EventArgs.Empty);
        }

    }
}