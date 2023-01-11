using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Quix.Sdk.Process.Common.Test
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
            this.Output = new TestBrokerOutput();
            // Connection between Transport Input and Output simulating a real broker
            this.Input = new TestBrokerInput(Output, generateExceptions); 
        }

        /// <summary>
        /// Transport Output of the Test broker. Stands for the consumer output end point of the Message broker.
        /// </summary>
        public TestBrokerOutput Output { get; }

        /// <summary>
        /// Transport Input of the Test broker. Stands for the producer input end point of the Message broker.
        /// </summary>
        public TestBrokerInput Input { get; }

    }

    /// <summary>
    /// Transport Input of the Test broker. Stands for the producer input end point of the Message broker.
    /// </summary>
    public class TestBrokerInput : IInput
    {
        private readonly TestBrokerOutput testOutput;
        private readonly bool generateExceptions;

        public TestBrokerInput(TestBrokerOutput testOutput, bool generateExceptions = false)
        {
            this.testOutput = testOutput;
            this.generateExceptions = generateExceptions;
        }

        public async Task Send(Package package, CancellationToken cancellationToken = default)
        {
            if (generateExceptions) throw new Exception("Test broker generated exception.");
            await this.testOutput.Send(package); // Redirecting to Output all the messages arriving from the Input
        }
    }

    /// <summary>
    /// Transport Output of the Test broker. Stands for the consumer output end point of the Message broker.
    /// </summary>
    public class TestBrokerOutput : IOutput
    {
        public Func<Package, Task> OnNewPackage { get; set; }

        public async Task Send(Package newPackage)
        {
            if (this.OnNewPackage == null) return;
            await this.OnNewPackage.Invoke(newPackage);
        }
    }

}
