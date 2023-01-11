using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Process.Common.Test
{
    /// <summary>
    /// Kafka Reader that uses a mocked Message broker for test purposes
    /// </summary>
    public class TestKafkaReader : KafkaReader
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TestKafkaReader"/>
        /// </summary>
        /// <param name="testBroker">Mocked test broker instance of <see cref="TestBroker"/></param>
        public TestKafkaReader(TestBroker testBroker)
            :base(new TestKafkaBrokerOutput(testBroker.Output))
        {
        }

        public new IStreamContextCache ContextCache => base.ContextCache;
        
        /// <summary>
        /// Transport Output of the Test broker. Stands for the consumer output end point of the Message broker.
        /// </summary>
        private class TestKafkaBrokerOutput : IKafkaOutput
        {
            private TestBrokerOutput output;
            
            public Func<Package, Task> OnNewPackage { get; set; }

            public TestKafkaBrokerOutput(TestBrokerOutput output)
            {
                this.output = output;
                output.OnNewPackage += (package) => this.OnNewPackage?.Invoke(package);
            }

            public async Task Send(Package newPackage)
            {
                await this.output.Send(newPackage);
            }

            public void Dispose()
            {
            }

            public event EventHandler<Exception> ErrorOccurred;

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
