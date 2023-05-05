using System;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Transport.IO;

namespace QuixStreams.Telemetry.Common.Test
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
        /// <param name="publishDelay">Delay when publishing to simulate a real broker delay</param>
        public TestBroker(bool generateExceptions = false, TimeSpan publishDelay = default)
        {
            this.Consumer = new TestBrokerConsumer();
            // Connection between Transport Input and Output simulating a real broker
            this.Producer = new TestBrokerProducer(Consumer, generateExceptions, publishDelay); 
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
        private readonly TimeSpan publishDelay;

        public TestBrokerProducer(TestBrokerConsumer testConsumer, bool generateExceptions = false, TimeSpan publishDelay = default)
        {
            this.testConsumer = testConsumer;
            this.generateExceptions = generateExceptions;
            this.publishDelay = publishDelay;
        }

        public async Task Publish(Package package, CancellationToken cancellationToken = default)
        {
            if (generateExceptions) throw new Exception("Test broker generated exception.");
            
            await Task.Delay(this.publishDelay, cancellationToken); // Simulating a real broker delay
            
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
