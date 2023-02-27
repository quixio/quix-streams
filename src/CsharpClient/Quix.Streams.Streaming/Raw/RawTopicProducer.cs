using System;
using System.Collections.Generic;
using System.Text;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;

namespace Quix.Streams.Streaming.Raw
{
    /// <summary>
    /// Class to write raw messages into a Topic (capable to write non-quixstreams messages)
    /// </summary>
    public class RawTopicProducer: IRawTopicProducer
    {
        private string topicName;

        private readonly IKafkaProducer kafkaProducer = null;
        
        /// <inheritdoc />
        public event EventHandler OnDisposed;

        /// <summary>
        /// Initializes a new instance of <see cref="RawTopicProducer"/>
        /// </summary>
        /// <param name="brokerAddress">Address of Kafka cluster.</param>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="brokerProperties">Additional broker properties</param>
        public RawTopicProducer(string brokerAddress, string topicName, Dictionary<string, string> brokerProperties = null)
        {
            brokerProperties ??= new Dictionary<string, string>();
            if (!brokerProperties.ContainsKey("queued.max.messages.kbytes")) brokerProperties["queued.max.messages.kbytes"] = "20480";

            this.topicName = topicName;

            var publisherConfiguration = new Transport.Kafka.PublisherConfiguration(brokerAddress, brokerProperties)
                {
                    KeepConnectionAlive = false //keepalive packets would interfere with reading raw data since we dont have any protocol defined over the transport layer
                };
            var topicConfiguration = new Transport.Kafka.ProducerTopicConfiguration(this.topicName);

            this.kafkaProducer = new Transport.Kafka.KafkaProducer(publisherConfiguration, topicConfiguration);
            this.kafkaProducer.Open();
        }

        /// <inheritdoc />
        public void Publish(RawMessage message)
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