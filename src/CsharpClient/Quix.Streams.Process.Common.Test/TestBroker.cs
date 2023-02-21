using System;
using System.Threading;
using System.Threading.Tasks;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Process.Common.Test
{
    /// <summary>
    /// Simulated Message broker for testing purposes
    /// It contains Input and Output properties that can be used as a real IInput and IOutput implementations of Transport layer
    /// All the messages entering to the Input will be redirected to the Output.
    /// </summary>
    public class TestBroker
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TestBroker"/>
        /// </summary>
        /// <param name="generateExceptions">The test broker generates exceptions when it receives data</param>
        public TestBroker(bool generateExceptions = false)
        {
            this.Consumer = new TestBrokerConsumer();
            // Connection between Transport Input and Output simulating a real broker
            this.Producer = new TestBrokerProducer(Consumer, generateExceptions); 
        }

        /// <summary>
        /// Transport Output of the Test broker. Stands for the consumer output end point of the Message broker.
        /// </summary>
        public TestBrokerConsumer Consumer { get; }

        /// <summary>
        /// Transport Input of the Test broker. Stands for the producer input end point of the Message broker.
        /// </summary>
        public TestBrokerProducer Producer { get; }

    }

    /// <summary>
    /// Transport Input of the Test broker. Stands for the producer input end point of the Message broker.
    /// </summary>
    public class TestBrokerProducer : IProducer
    {
        private readonly TestBrokerConsumer testConsumer;
        private readonly bool generateExceptions;

        public TestBrokerProducer(TestBrokerConsumer testConsumer, bool generateExceptions = false)
        {
            this.testConsumer = testConsumer;
            this.generateExceptions = generateExceptions;
        }

        public async Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            if (generateExceptions) throw new Exception("Test broker generated exception.");
            await this.testConsumer.Send(package); // Redirecting to Output all the messages arriving from the Input
        }
    }

    /// <summary>
    /// Transport Output of the Test broker. Stands for the consumer output end point of the Message broker.
    /// </summary>
    public class TestBrokerConsumer : IConsumer
    {
        public Func<Package, Task> OnNewPackage { get; set; }

        public async Task Send(Package newPackage)
        {
            if (this.OnNewPackage == null) return;
            await this.OnNewPackage.Invoke(newPackage);
        }
    }

}
