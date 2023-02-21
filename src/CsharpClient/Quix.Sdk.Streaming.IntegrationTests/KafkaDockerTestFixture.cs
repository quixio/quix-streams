using System;
using System.Diagnostics;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Extensions;
using Ductus.FluentDocker.Services;
using Quix.Sdk.Streaming.Configuration;
using Xunit;

namespace Quix.Sdk.Streaming.IntegrationTests
{
    
    public class KafkaDockerTestFixture : IDisposable
    {
        private readonly IContainerService kafkaContainer;
        public readonly int ZookeeperPort;
        public readonly int KafkaPort;
        public readonly string BrokerList;
        public SecurityOptions SecurityOptions = null;

        public KafkaDockerTestFixture()
        {
            var random = new Random();
            var host = "127.0.0.1";
            this.ZookeeperPort = random.Next(1324,23457);
            this.KafkaPort = random.Next(23458,58787);
            this.BrokerList = $"{host}:{KafkaPort}";
            Console.WriteLine("Creating Kafka container");
            var sw = Stopwatch.StartNew();
            try
            {
                this.kafkaContainer = new Builder()
                    .UseContainer()
                    .UseImage("lensesio/fast-data-dev:3.3.1")
                    .ExposePort(ZookeeperPort, ZookeeperPort)
                    .ExposePort(KafkaPort, KafkaPort)
                    .WithEnvironment(
                        $"BROKER_PORT={KafkaPort}", 
                        $"ZK_PORT={ZookeeperPort}",
                        $"ADV_HOST={host}",
                        "REST_PORT=0",
                        "WEB_PORT=0",
                        "CONNECT_PORT=0",
                        "REGISTRY_PORT=0",
                        "RUNTESTS=0",
                        "SAMPLEDATA=0",
                        "FORWARDLOGS=0",
                        "SUPERVISORWEB=0")
                    .Build()
                    .Start();
                this.kafkaContainer.WaitForRunning();
            }
            catch
            {
                this.kafkaContainer?.Dispose();
                throw;
            }

            Console.WriteLine("Created Kafka container in {0:g}", sw.Elapsed);
        }

        public void Dispose()
        {
            Console.WriteLine("Disposing Kafka container");
            kafkaContainer.Dispose();
            Console.WriteLine("Disposed Kafka container");
        }

    }

    [CollectionDefinition("Kafka Container Collection")]
    public class KafkaDockerCollection : ICollectionFixture<KafkaDockerTestFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}