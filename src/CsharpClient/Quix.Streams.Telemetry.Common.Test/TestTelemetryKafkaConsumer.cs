using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Quix.Streams.Telemetry.Kafka;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;

namespace Quix.Streams.Telemetry.Common.Test
{
    /// <summary>
    /// Kafka consumer that uses a mocked Message broker for test purposes
    /// </summary>
    public class TestTelemetryKafkaConsumer : TelemetryKafkaConsumer
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TestTelemetryKafkaConsumer"/>
        /// </summary>
        /// <param name="testBroker">Mocked test broker instance of <see cref="TestBroker"/></param>
        public TestTelemetryKafkaConsumer(TestBroker testBroker)
            :base(new TestKafkaBrokerConsumer(testBroker.Consumer))
        {
        }

        public new IStreamContextCache ContextCache => base.ContextCache;
        
        /// <summary>
        /// Transport Output of the Test broker. Stands for the consumer output end point of the Message broker.
        /// </summary>
        private class TestKafkaBrokerConsumer : IKafkaConsumer
        {
            private TestBrokerConsumer consumer;
            
            public Func<Package, Task> OnNewPackage { get; set; }

            public TestKafkaBrokerConsumer(TestBrokerConsumer consumer)
            {
                this.consumer = consumer;
                consumer.OnNewPackage += (package) => this.OnNewPackage?.Invoke(package);
            }

            public async Task Send(Package newPackage)
            {
                await this.consumer.Send(newPackage);
            }

            public void Dispose()
            {
            }

            public event EventHandler<Exception> OnErrorOccurred;

            public void Close()
            {
            }

            public void CommitOffsets()
            {
            }

            public void CommitOffsets(ICollection<TopicPartitionOffset> offsets)
            {
            }

            public void Open()
            {
            }
        }
    }
}
